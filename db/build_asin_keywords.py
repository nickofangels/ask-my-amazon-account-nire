"""
db/build_asin_keywords.py — Build the asin_keyword_scores matrix.

Produces one row per (asin, search_query) from the L52 search_query_performance
data. All numeric scores are normalized to percentile ranks (0-1) relative to
the current catalog distribution — no hardcoded thresholds.

Two composite scores are computed:
  keyword_relevance — ASIN-side: how much does this keyword matter to this ASIN?
  asin_priority     — KW-side:   how much should this ASIN own this keyword?

Usage:
    python -m db.build_asin_keywords
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

import psycopg2.extras

from db.utils import (
    keyword_type as _keyword_type,
    percentile_ranks as _percentile_ranks,
    safe_pct as _safe_pct,
    trend_windows,
)
from schema import get_conn

# =====================================================================
# SCORING WEIGHTS
# =====================================================================

KW_RELEVANCE_WEIGHTS = {
    "revenue":       0.40,
    "cvr_advantage": 0.35,
    "traffic_dep":   0.15,
    "aov":           0.05,
    "momentum":      0.05,
}

ASIN_PRIORITY_WEIGHTS = {
    "revenue":       0.40,
    "cvr_advantage": 0.35,
    "dominance":     0.15,
    "aov":           0.05,
    "momentum":      0.05,
}

SHRINKAGE_K = 50
POWER_STRETCH = 0.6

ROLE_THRESHOLDS = {
    "core_traffic_floor":         0.70,
    "growth_cvr_floor":           0.50,
    "growth_headroom_floor":      0.50,
    "aspirational_volume_floor":  0.75,
    "aspirational_traffic_ceil":  0.25,
    "harvest_cvr_floor":          0.70,
    "harvest_headroom_ceil":      0.30,
}


# =====================================================================
# Main build function
# =====================================================================

def build_asin_keywords(conn) -> int:
    """
    Rebuild asin_keyword_scores from search_query_performance (L52 period).
    Returns the number of rows written.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("  [build_asin_keywords] Aggregating raw funnel per (asin, keyword)...", flush=True)

    # ------------------------------------------------------------------
    # 1. Aggregate raw per-(asin, keyword) funnel from SQP.
    # ------------------------------------------------------------------
    raw_sql = """
        WITH per_asin_kw AS (
            SELECT
                asin,
                search_query,
                AVG(search_query_volume)   AS search_volume,
                SUM(asin_impressions)      AS asin_impressions,
                SUM(asin_clicks)           AS asin_clicks,
                SUM(asin_cart_adds)        AS asin_cart_adds,
                SUM(asin_purchases)        AS asin_purchases
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY asin, search_query
        ),
        mkt_per_month AS (
            SELECT search_query, month,
                   MAX(total_clicks)    AS mkt_clicks_mo,
                   MAX(total_purchases) AS mkt_purchases_mo
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query, month
        ),
        mkt_agg AS (
            SELECT search_query,
                   SUM(mkt_clicks_mo)    AS mkt_clicks,
                   SUM(mkt_purchases_mo) AS mkt_purchases
            FROM mkt_per_month
            GROUP BY search_query
        ),
        aov_lookup AS (
            SELECT asin,
                   CASE WHEN SUM(units) > 0
                        THEN SUM(revenue)::FLOAT / SUM(units)
                        ELSE NULL END AS aov
            FROM sales_traffic_asin
            WHERE period = 'L52'
            GROUP BY asin
        )
        SELECT
            p.asin,
            p.search_query,
            p.search_volume::INTEGER    AS search_volume,
            p.asin_impressions,
            p.asin_clicks,
            p.asin_cart_adds,
            p.asin_purchases,
            m.mkt_clicks,
            m.mkt_purchases,
            a.aov
        FROM per_asin_kw p
        JOIN mkt_agg m ON p.search_query = m.search_query
        LEFT JOIN aov_lookup a ON p.asin = a.asin
    """
    cur.execute(raw_sql)
    raw_rows = cur.fetchall()
    print(f"  [build_asin_keywords] {len(raw_rows):,} (asin, keyword) pairs found", flush=True)

    # ------------------------------------------------------------------
    # 2. Share trend per (asin, keyword)
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Computing per-ASIN share trends...", flush=True)
    rec_start, rec_end, pri_start, pri_end = trend_windows(conn)
    print(f"  [build_asin_keywords] Trend windows — recent: {rec_start}-{rec_end}, "
          f"prior: {pri_start}-{pri_end}", flush=True)

    cur.execute("""
        WITH recent AS (
            SELECT asin, search_query,
                   AVG(asin_purchase_share) AS recent_avg,
                   COUNT(*) AS recent_months
            FROM search_query_performance
            WHERE period = 'L52' AND month BETWEEN %s AND %s
            GROUP BY asin, search_query
        ),
        prior AS (
            SELECT asin, search_query,
                   AVG(asin_purchase_share) AS prior_avg,
                   COUNT(*) AS prior_months
            FROM search_query_performance
            WHERE period = 'L52' AND month BETWEEN %s AND %s
            GROUP BY asin, search_query
        )
        SELECT r.asin, r.search_query,
               (r.recent_avg - p.prior_avg) AS share_trend
        FROM recent r
        JOIN prior p ON r.asin = p.asin AND r.search_query = p.search_query
        WHERE r.recent_months >= 2 AND p.prior_months >= 2
    """, (rec_start, rec_end, pri_start, pri_end))

    trend_map: dict[tuple[str, str], float] = {
        (r["asin"], r["search_query"]): r["share_trend"]
        for r in cur.fetchall()
    }
    print(f"  [build_asin_keywords] Share trend available for {len(trend_map):,} pairs", flush=True)

    # ------------------------------------------------------------------
    # 3. Compute derived metrics for every row.
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Computing derived metrics...", flush=True)

    records: list[dict] = []

    asin_total_clicks: dict[str, int] = defaultdict(int)
    asin_total_purchases: dict[str, int] = defaultdict(int)
    kw_total_brand_purchases: dict[str, int] = defaultdict(int)

    for r in raw_rows:
        asin_total_clicks[r["asin"]] += int(r["asin_clicks"] or 0)
        asin_total_purchases[r["asin"]] += int(r["asin_purchases"] or 0)
        kw_total_brand_purchases[r["search_query"]] += int(r["asin_purchases"] or 0)

    asin_overall_cvr: dict[str, float] = {}
    for a in asin_total_clicks:
        tc = asin_total_clicks[a]
        asin_overall_cvr[a] = (asin_total_purchases[a] / tc) if tc > 0 else 0.0

    for r in raw_rows:
        asin   = r["asin"]
        query  = r["search_query"]
        vol    = int(r["search_volume"] or 0)
        imp    = int(r["asin_impressions"] or 0)
        clk    = int(r["asin_clicks"] or 0)
        cart   = int(r["asin_cart_adds"] or 0)
        pur    = int(r["asin_purchases"] or 0)
        mkt_c  = int(r["mkt_clicks"] or 0)
        mkt_p  = int(r["mkt_purchases"] or 0)
        aov    = r["aov"]

        asin_cvr = (pur / clk) if clk > 0 else None
        mkt_cvr  = (mkt_p / mkt_c) if mkt_c > 0 else None

        prior_cvr = asin_overall_cvr.get(asin, 0.0)
        if clk > 0:
            adjusted_cvr = (clk * asin_cvr + SHRINKAGE_K * prior_cvr) / (clk + SHRINKAGE_K)
        else:
            adjusted_cvr = None

        cvr_index = (adjusted_cvr / mkt_cvr) if (adjusted_cvr is not None and mkt_cvr and mkt_cvr > 0) else None

        click_share    = (clk / mkt_c) if mkt_c > 0 else None
        purchase_share = (pur / mkt_p) if mkt_p > 0 else None

        revenue_score = (pur * aov) if (aov is not None) else None

        tot_clk = asin_total_clicks[asin]
        traffic_contrib = (clk / tot_clk) if tot_clk > 0 else None

        tot_brand_pur = kw_total_brand_purchases[query]
        brand_dominance = (pur / tot_brand_pur) if tot_brand_pur > 0 else None

        headroom = (vol * (1.0 - click_share)) if (vol and click_share is not None) else None

        share_trend = trend_map.get((asin, query))

        records.append({
            "asin":            asin,
            "search_query":    query,
            "search_volume":   vol or None,
            "asin_impressions": imp,
            "asin_clicks":     clk,
            "asin_cart_adds":  cart,
            "asin_purchases":  pur,
            "mkt_clicks":      mkt_c,
            "mkt_purchases":   mkt_p,
            "asin_cvr":        asin_cvr,
            "adjusted_cvr":    adjusted_cvr,
            "mkt_cvr":         mkt_cvr,
            "cvr_index":       cvr_index,
            "click_share":     click_share,
            "purchase_share":  purchase_share,
            "aov":             aov,
            "revenue_score":   revenue_score,
            "traffic_contrib": traffic_contrib,
            "brand_dominance": brand_dominance,
            "headroom":        headroom,
            "share_trend":     share_trend,
            "keyword_type":    _keyword_type(query),
        })

    n = len(records)
    print(f"  [build_asin_keywords] Computing global percentile ranks across {n:,} pairs...", flush=True)

    # ------------------------------------------------------------------
    # 4. Global percentile ranks
    # ------------------------------------------------------------------
    global_vol_pcts    = _percentile_ranks([r["search_volume"] for r in records])
    global_cvr_pcts    = _percentile_ranks([r["cvr_index"] for r in records])
    global_head_pcts   = _percentile_ranks([r["headroom"] for r in records])
    global_mom_pcts    = _percentile_ranks([r["share_trend"] for r in records])
    global_rev_pcts    = _percentile_ranks([r["revenue_score"] for r in records])

    for i, rec in enumerate(records):
        rec["volume_pct"]        = global_vol_pcts[i]
        rec["cvr_advantage_pct"] = global_cvr_pcts[i]
        rec["headroom_pct"]      = global_head_pcts[i]
        rec["momentum_pct"]      = global_mom_pcts[i]
        rec["global_revenue_pct"]= global_rev_pcts[i]

    # ------------------------------------------------------------------
    # 5. Within-ASIN percentile ranks
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Computing within-ASIN percentile ranks...", flush=True)

    asin_groups: dict[str, list[int]] = defaultdict(list)
    for i, rec in enumerate(records):
        asin_groups[rec["asin"]].append(i)

    for asin, indices in asin_groups.items():
        tc_vals  = [records[i]["traffic_contrib"] for i in indices]
        rev_vals = [records[i]["revenue_score"]   for i in indices]

        tc_pcts  = _percentile_ranks(tc_vals)
        rev_pcts = _percentile_ranks(rev_vals)

        for j, idx in enumerate(indices):
            records[idx]["within_asin_traffic_pct"] = tc_pcts[j]
            records[idx]["within_asin_revenue_pct"] = rev_pcts[j]

    # ------------------------------------------------------------------
    # 6. Within-keyword percentile ranks
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Computing within-keyword percentile ranks...", flush=True)

    kw_groups: dict[str, list[int]] = defaultdict(list)
    for i, rec in enumerate(records):
        kw_groups[rec["search_query"]].append(i)

    for query, indices in kw_groups.items():
        dom_vals = [records[i]["brand_dominance"] for i in indices]
        cvr_vals = [records[i]["adjusted_cvr"]    for i in indices]
        aov_vals = [records[i]["aov"]             for i in indices]

        dom_pcts = _percentile_ranks(dom_vals)
        cvr_pcts = _percentile_ranks(cvr_vals)
        aov_pcts = _percentile_ranks(aov_vals)

        for j, idx in enumerate(indices):
            records[idx]["within_kw_dominance_pct"] = dom_pcts[j]
            records[idx]["within_kw_cvr_pct"]       = cvr_pcts[j]
            records[idx]["within_kw_aov_pct"]       = aov_pcts[j]

    # ------------------------------------------------------------------
    # 7. Composite scores
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Computing composite scores...", flush=True)
    W_REL  = KW_RELEVANCE_WEIGHTS
    W_PRI  = ASIN_PRIORITY_WEIGHTS
    _P     = POWER_STRETCH

    def _stretched(rec, key):
        v = _safe_pct([rec.get(key)], 0)
        return v ** _P

    for rec in records:
        rec["keyword_relevance"] = (
            W_REL["revenue"]       * _stretched(rec, "within_asin_revenue_pct")
          + W_REL["cvr_advantage"] * _stretched(rec, "cvr_advantage_pct")
          + W_REL["traffic_dep"]   * _stretched(rec, "within_asin_traffic_pct")
          + W_REL["aov"]           * _stretched(rec, "within_kw_aov_pct")
          + W_REL["momentum"]      * _stretched(rec, "momentum_pct")
        )

        rec["asin_priority"] = (
            W_PRI["revenue"]       * _stretched(rec, "global_revenue_pct")
          + W_PRI["cvr_advantage"] * _stretched(rec, "within_kw_cvr_pct")
          + W_PRI["dominance"]     * _stretched(rec, "within_kw_dominance_pct")
          + W_PRI["aov"]           * _stretched(rec, "within_kw_aov_pct")
          + W_PRI["momentum"]      * _stretched(rec, "momentum_pct")
        )

    # ------------------------------------------------------------------
    # 8. Classify keyword_role
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Classifying keyword roles...", flush=True)
    T = ROLE_THRESHOLDS

    role_counts: dict[str, int] = defaultdict(int)
    for rec in records:
        ktype  = rec["keyword_type"]
        w_tc   = rec.get("within_asin_traffic_pct") or 0.0
        w_dom  = rec.get("within_kw_dominance_pct") or 0.0
        g_cvr  = rec.get("cvr_advantage_pct") or 0.0
        g_head = rec.get("headroom_pct") or 0.0
        g_vol  = rec.get("volume_pct") or 0.0

        if ktype == "branded" and w_dom >= 0.50:
            role = "defend"
        elif ktype == "branded" and w_dom < 0.50:
            role = "halo"
        elif w_tc >= T["core_traffic_floor"]:
            role = "core"
        elif g_cvr >= T["harvest_cvr_floor"] and g_head <= T["harvest_headroom_ceil"]:
            role = "harvest"
        elif g_cvr >= T["growth_cvr_floor"] and g_head >= T["growth_headroom_floor"]:
            role = "growth"
        elif g_vol >= T["aspirational_volume_floor"] and w_tc <= T["aspirational_traffic_ceil"]:
            role = "aspirational"
        else:
            role = "other"

        rec["keyword_role"] = role
        role_counts[role] += 1

    # ------------------------------------------------------------------
    # 9. Write to asin_keyword_scores
    # ------------------------------------------------------------------
    print("  [build_asin_keywords] Writing asin_keyword_scores...", flush=True)

    rows: list[tuple] = []
    for rec in records:
        def _r(v, digits=6):
            return round(v, digits) if v is not None else None

        rows.append((
            rec["asin"],
            rec["search_query"],
            rec["asin_impressions"],
            rec["asin_clicks"],
            rec["asin_cart_adds"],
            rec["asin_purchases"],
            rec["mkt_clicks"],
            rec["mkt_purchases"],
            rec["search_volume"],
            _r(rec["asin_cvr"]),
            _r(rec["adjusted_cvr"]),
            _r(rec["mkt_cvr"]),
            _r(rec["cvr_index"]),
            _r(rec["click_share"]),
            _r(rec["purchase_share"]),
            _r(rec["aov"], 2),
            _r(rec["revenue_score"], 2),
            _r(rec.get("within_asin_traffic_pct")),
            _r(rec.get("within_asin_revenue_pct")),
            _r(rec.get("within_kw_dominance_pct")),
            _r(rec.get("within_kw_cvr_pct")),
            _r(rec.get("within_kw_aov_pct")),
            _r(rec.get("volume_pct")),
            _r(rec.get("cvr_advantage_pct")),
            _r(rec.get("headroom_pct")),
            _r(rec.get("momentum_pct")),
            _r(rec["keyword_relevance"]),
            _r(rec["asin_priority"]),
            rec["keyword_type"],
            rec["keyword_role"],
            _r(rec["share_trend"]),
            now,
        ))

    # Use a write cursor (not the RealDictCursor)
    wcur = conn.cursor()
    wcur.execute("DELETE FROM asin_keyword_scores")

    # Batch insert
    from psycopg2.extras import execute_values
    insert_sql = """
        INSERT INTO asin_keyword_scores
           (asin, search_query,
            asin_impressions, asin_clicks, asin_cart_adds, asin_purchases,
            mkt_clicks, mkt_purchases, search_volume,
            asin_cvr, adjusted_cvr, mkt_cvr, cvr_index, click_share, purchase_share,
            aov, revenue_score,
            within_asin_traffic_pct, within_asin_revenue_pct,
            within_kw_dominance_pct, within_kw_cvr_pct, within_kw_aov_pct,
            volume_pct, cvr_advantage_pct, headroom_pct, momentum_pct,
            keyword_relevance, asin_priority,
            keyword_type, keyword_role, share_trend,
            built_at)
        VALUES %s
    """
    if rows:
        execute_values(wcur, insert_sql, rows, page_size=1000)
    conn.commit()
    wcur.close()
    cur.close()

    print(f"  [build_asin_keywords] Done — {len(rows):,} rows written")
    print("  [build_asin_keywords] Role breakdown:")
    for role, cnt in sorted(role_counts.items(), key=lambda x: -x[1]):
        print(f"    {role:<15} {cnt:>6,}")

    return len(rows)


def main():
    conn = get_conn()
    n = build_asin_keywords(conn)
    conn.close()
    print(f"\nDone. {n:,} asin_keyword_scores rows written.")


if __name__ == "__main__":
    main()
