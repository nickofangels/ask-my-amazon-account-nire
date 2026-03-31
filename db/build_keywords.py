"""
db/build_keywords.py — Build the keyword_targets table.

Aggregates the asin_keyword_scores matrix (built first by
build_asin_keywords.py) up to one row per search_query.

Strategy buckets:
  Branded      — branded keyword (string-based, categorical)
  Defend       — high purchase share + low headroom (we dominate)
  Grow         — CVR Index >= 1.0 (we beat the market)
  Watch        — CVR Index >= 0.7 (close to market, could tip)
  Deprioritize — everything else

Usage:
    python -m db.build_keywords
"""
from __future__ import annotations

from datetime import datetime, timezone

import psycopg2.extras

from db.utils import keyword_type as _keyword_type, percentile_ranks as _percentile_ranks, trend_windows
from schema import get_conn

# =====================================================================
# Strategy thresholds
# =====================================================================
STRATEGY_THRESHOLDS = {
    "defend_share_floor":    0.70,
    "defend_head_ceil":      0.30,
    "grow_cvr_index_floor":  1.0,
    "watch_cvr_index_floor": 0.7,
}


def _vol_tier(vol: float | None) -> str:
    if vol is None:
        return "unknown"
    if vol >= 100_000:
        return "mega"
    if vol >= 10_000:
        return "head"
    if vol >= 1_000:
        return "mid"
    return "long-tail"


def _classify(kw_type: str, cvr_index: float,
              share_pct: float, head_pct: float) -> str:
    T = STRATEGY_THRESHOLDS
    if kw_type == "branded":
        return "Branded"
    if share_pct >= T["defend_share_floor"] and head_pct <= T["defend_head_ceil"]:
        return "Defend"
    if cvr_index is not None and cvr_index >= T["grow_cvr_index_floor"]:
        return "Grow"
    if cvr_index is not None and cvr_index >= T["watch_cvr_index_floor"]:
        return "Watch"
    return "Deprioritize"


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_keywords(conn) -> int:
    """
    Rebuild keyword_targets from asin_keyword_scores.
    Must be called AFTER build_asin_keywords.
    Returns the number of rows written.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("  [build_keywords] Aggregating from asin_keyword_scores...", flush=True)

    # ------------------------------------------------------------------
    # 1. Aggregate per-keyword from the matrix.
    #    brand_* columns = Nire Beauty metrics (dwc_* in DWC reference)
    # ------------------------------------------------------------------
    agg_sql = """
        WITH brand_agg AS (
            SELECT
                search_query,
                AVG(search_volume)        AS volume,
                SUM(asin_impressions)     AS brand_impressions,
                SUM(asin_clicks)          AS brand_clicks,
                SUM(asin_purchases)       AS brand_purchases,
                COUNT(DISTINCT asin)      AS asin_count
            FROM asin_keyword_scores
            GROUP BY search_query
        ),
        months_cte AS (
            SELECT search_query,
                   COUNT(DISTINCT month) AS months_of_data
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query
        ),
        mkt_per_month AS (
            SELECT search_query, month,
                   MAX(total_impressions) AS mkt_impressions_mo,
                   MAX(total_clicks)      AS mkt_clicks_mo,
                   MAX(total_purchases)   AS mkt_purchases_mo
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query, month
        ),
        mkt_agg AS (
            SELECT search_query,
                   SUM(mkt_impressions_mo) AS mkt_impressions,
                   SUM(mkt_clicks_mo)      AS mkt_clicks,
                   SUM(mkt_purchases_mo)   AS mkt_purchases
            FROM mkt_per_month
            GROUP BY search_query
        )
        SELECT
            d.search_query,
            d.volume,
            mc.months_of_data,
            d.brand_impressions,
            d.brand_clicks,
            d.brand_purchases,
            d.asin_count,
            m.mkt_impressions,
            m.mkt_clicks,
            m.mkt_purchases
        FROM brand_agg d
        LEFT JOIN months_cte mc ON d.search_query = mc.search_query
        JOIN mkt_agg m ON d.search_query = m.search_query
    """
    cur.execute(agg_sql)
    agg_rows = cur.fetchall()
    print(f"  [build_keywords] {len(agg_rows):,} keywords aggregated", flush=True)

    # ------------------------------------------------------------------
    # 2. Share trend
    # ------------------------------------------------------------------
    print("  [build_keywords] Computing keyword-level share trends...", flush=True)
    rec_start, rec_end, pri_start, pri_end = trend_windows(conn)

    cur.execute("""
        WITH monthly_share AS (
            SELECT search_query, month,
                   AVG(asin_purchase_share) AS avg_share
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query, month
        ),
        recent AS (
            SELECT search_query, AVG(avg_share) AS recent_avg,
                   COUNT(*) AS recent_months
            FROM monthly_share
            WHERE month BETWEEN %s AND %s
            GROUP BY search_query
        ),
        prior AS (
            SELECT search_query, AVG(avg_share) AS prior_avg,
                   COUNT(*) AS prior_months
            FROM monthly_share
            WHERE month BETWEEN %s AND %s
            GROUP BY search_query
        )
        SELECT r.search_query,
               (r.recent_avg - p.prior_avg) AS share_trend
        FROM recent r
        JOIN prior p ON r.search_query = p.search_query
        WHERE r.recent_months >= 2 AND p.prior_months >= 2
    """, (rec_start, rec_end, pri_start, pri_end))

    trend_map: dict[str, float] = {
        r["search_query"]: r["share_trend"]
        for r in cur.fetchall()
    }

    # ------------------------------------------------------------------
    # 3. Hero ASIN
    # ------------------------------------------------------------------
    print("  [build_keywords] Selecting hero ASINs from matrix...", flush=True)
    cur.execute("""
        WITH ranked AS (
            SELECT search_query, asin,
                   asin_priority, asin_cvr * 100 AS hero_cvr,
                   aov AS hero_aov,
                   revenue_score AS hero_revenue_score,
                   ROW_NUMBER() OVER (
                       PARTITION BY search_query
                       ORDER BY asin_priority DESC
                   ) AS rn
            FROM asin_keyword_scores
            WHERE asin_purchases > 0
        )
        SELECT search_query, asin AS hero_asin,
               hero_cvr, hero_aov, hero_revenue_score
        FROM ranked WHERE rn = 1
    """)
    hero_map: dict[str, dict] = {
        r["search_query"]: dict(r) for r in cur.fetchall()
    }
    print(f"  [build_keywords] Hero ASINs identified for {len(hero_map):,} keywords", flush=True)

    # ------------------------------------------------------------------
    # 4. Cannibalization flag
    # ------------------------------------------------------------------
    print("  [build_keywords] Computing cannibalization flags...", flush=True)
    cur.execute("""
        WITH per_kw AS (
            SELECT search_query, asin, asin_purchases,
                   asin_purchases::FLOAT
                       / SUM(asin_purchases) OVER (PARTITION BY search_query)
                       AS dominance,
                   ROW_NUMBER() OVER (
                       PARTITION BY search_query
                       ORDER BY asin_purchases DESC
                   ) AS rn
            FROM asin_keyword_scores
            WHERE asin_purchases > 0
        ),
        top2 AS (
            SELECT search_query,
                   MAX(CASE WHEN rn=1 THEN dominance END) AS top1,
                   MAX(CASE WHEN rn=2 THEN dominance END) AS top2
            FROM per_kw WHERE rn <= 2
            GROUP BY search_query
        )
        SELECT search_query,
               CASE WHEN top2 IS NOT NULL
                    AND (top1 - top2) < 0.15
                    THEN 1 ELSE 0 END AS cannibalization_flag
        FROM top2
    """)
    cannibal_map: dict[str, int] = {
        r["search_query"]: r["cannibalization_flag"]
        for r in cur.fetchall()
    }

    # ------------------------------------------------------------------
    # 5. Percentile ranks for classification
    # ------------------------------------------------------------------
    print("  [build_keywords] Computing keyword-level percentile ranks...", flush=True)

    vol_list: list[float | None] = []
    pur_list: list[float | None] = []
    cvr_idx_list: list[float | None] = []
    headroom_list: list[float | None] = []
    share_list: list[float | None] = []

    for r in agg_rows:
        vol    = float(r["volume"] or 0) or None
        brand_c  = int(r["brand_clicks"] or 0)
        brand_p  = int(r["brand_purchases"] or 0)
        mkt_c  = int(r["mkt_clicks"] or 0)
        mkt_p  = int(r["mkt_purchases"] or 0)

        brand_cvr = (brand_p / brand_c) if brand_c > 0 else None
        mkt_cvr = (mkt_p / mkt_c) if mkt_c > 0 else None
        cvr_idx = (brand_cvr / mkt_cvr) if (brand_cvr and mkt_cvr and mkt_cvr > 0) else None

        click_share    = (brand_c / mkt_c) if mkt_c > 0 else None
        purchase_share = (brand_p / mkt_p) if mkt_p > 0 else None
        headroom       = (vol * (1.0 - click_share)) if (vol and click_share is not None) else None

        vol_list.append(vol)
        pur_list.append(float(brand_p) if brand_p > 0 else None)
        cvr_idx_list.append(cvr_idx)
        headroom_list.append(headroom)
        share_list.append(purchase_share)

    vol_pcts   = _percentile_ranks(vol_list)
    pur_pcts   = _percentile_ranks(pur_list)
    cvr_pcts   = _percentile_ranks(cvr_idx_list)
    head_pcts  = _percentile_ranks(headroom_list)
    share_pcts = _percentile_ranks(share_list)

    # ------------------------------------------------------------------
    # 6. Build rows and classify
    # ------------------------------------------------------------------
    print("  [build_keywords] Classifying strategy buckets...", flush=True)

    rows: list[tuple] = []
    strategy_counts: dict[str, int] = {}

    for i, r in enumerate(agg_rows):
        query            = r["search_query"]
        vol              = float(r["volume"] or 0)
        brand_impressions = int(r["brand_impressions"] or 0)
        brand_clicks     = int(r["brand_clicks"] or 0)
        brand_purchases  = int(r["brand_purchases"] or 0)
        mkt_impressions  = int(r["mkt_impressions"] or 0)
        mkt_clicks       = int(r["mkt_clicks"] or 0)
        mkt_purchases    = int(r["mkt_purchases"] or 0)
        months           = int(r["months_of_data"] or 0)
        asin_count       = int(r["asin_count"] or 0)

        brand_click_share    = (brand_clicks / mkt_clicks * 100) if mkt_clicks > 0 else 0.0
        brand_purchase_share = (brand_purchases / mkt_purchases * 100) if mkt_purchases > 0 else 0.0
        brand_cvr   = (brand_purchases / brand_clicks * 100) if brand_clicks > 0 else None
        mkt_cvr     = (mkt_purchases / mkt_clicks * 100) if mkt_clicks > 0 else None
        cvr_index   = (brand_cvr / mkt_cvr) if (brand_cvr is not None and mkt_cvr and mkt_cvr > 0) else None
        brand_ctr   = (brand_clicks / brand_impressions * 100) if brand_impressions > 0 else None
        mkt_ctr     = (mkt_clicks / mkt_impressions * 100) if mkt_impressions > 0 else None
        ctr_index   = (brand_ctr / mkt_ctr) if (brand_ctr is not None and mkt_ctr and mkt_ctr > 0) else None

        kw_type = _keyword_type(query)

        strategy = _classify(
            kw_type,
            cvr_index,
            share_pcts[i] or 0.0,
            head_pcts[i]  or 0.0,
        )
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        hero       = hero_map.get(query, {})
        share_trend = trend_map.get(query)
        cannibal   = cannibal_map.get(query, 0)

        rows.append((
            query,
            int(vol) if vol else None,
            _vol_tier(vol),
            brand_clicks,
            brand_purchases,
            round(brand_click_share, 4),
            round(brand_purchase_share, 4),
            mkt_clicks,
            mkt_purchases,
            round(mkt_cvr, 4) if mkt_cvr is not None else None,
            round(brand_cvr, 4) if brand_cvr is not None else None,
            round(cvr_index, 4) if cvr_index is not None else None,
            brand_impressions,
            mkt_impressions,
            round(brand_ctr, 4) if brand_ctr is not None else None,
            round(mkt_ctr, 4) if mkt_ctr is not None else None,
            round(ctr_index, 4) if ctr_index is not None else None,
            hero.get("hero_asin"),
            round(hero["hero_cvr"], 4) if hero.get("hero_cvr") is not None else None,
            round(hero["hero_aov"], 2) if hero.get("hero_aov") is not None else None,
            round(hero["hero_revenue_score"], 4) if hero.get("hero_revenue_score") is not None else None,
            strategy,
            kw_type,
            asin_count,
            cannibal,
            round(share_trend, 6) if share_trend is not None else None,
            months,
            now,
        ))

    # ------------------------------------------------------------------
    # 7. Write keyword_targets
    # ------------------------------------------------------------------
    print("  [build_keywords] Writing keyword_targets...", flush=True)
    wcur = conn.cursor()
    wcur.execute("DELETE FROM keyword_targets")

    from psycopg2.extras import execute_values
    insert_sql = """
        INSERT INTO keyword_targets
           (search_query, volume, vol_tier,
            brand_clicks, brand_purchases, brand_click_share, brand_purchase_share,
            mkt_clicks, mkt_purchases, mkt_cvr, brand_cvr, cvr_index,
            brand_impressions, mkt_impressions, brand_ctr, mkt_ctr, ctr_index,
            hero_asin, hero_cvr, hero_aov, hero_revenue_score,
            strategy, keyword_type, asin_count, cannibalization_flag,
            share_trend, months_of_data, built_at)
        VALUES %s
    """
    if rows:
        execute_values(wcur, insert_sql, rows, page_size=1000)
    conn.commit()
    wcur.close()
    cur.close()

    print(f"  [build_keywords] Done — {len(rows):,} keyword targets written")
    print("  [build_keywords] Strategy breakdown:")
    for s, cnt in sorted(strategy_counts.items(), key=lambda x: -x[1]):
        print(f"    {s:<12} {cnt:>6,}")

    return len(rows)


def main():
    conn = get_conn()
    n = build_keywords(conn)
    conn.close()
    print(f"\nDone. {n:,} keyword targets written.")


if __name__ == "__main__":
    main()
