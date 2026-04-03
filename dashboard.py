"""
Nire Beauty Analytics Dashboard — http://localhost:5052
Tabs: Overview | Products | Search Funnel | Search Terms | ASIN Explorer
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2.extras
from flask import Flask, jsonify, render_template_string, request

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from schema import get_conn

app = Flask(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn():
    return get_conn()

def _rows(conn, sql, params=()):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    return rows

def _period_meta(conn):
    """Load period boundaries and labels from DB."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT period, start_date, end_date, label FROM period_meta")
    result = {r["period"]: dict(r) for r in cur.fetchall()}
    cur.close()
    return result

# ---------------------------------------------------------------------------
# API — /api/period-meta
# ---------------------------------------------------------------------------

@app.route("/api/period-meta")
def api_period_meta():
    conn = _conn()
    meta = _period_meta(conn)
    conn.close()
    return jsonify(meta)


# ---------------------------------------------------------------------------
# API — /api/summary
# ---------------------------------------------------------------------------

@app.route("/api/summary")
def api_summary():
    conn = _conn()
    sql = """
        SELECT period,
            SUM(units)    AS units,
            SUM(revenue)  AS revenue,
            SUM(sessions) AS sessions,
            SUM(page_views) AS page_views,
            CASE WHEN SUM(sessions)>0
                 THEN SUM(units)::FLOAT/SUM(sessions)*100 ELSE 0 END AS conversion_rate_pct,
            AVG(buy_box_pct) AS avg_buy_box_pct
        FROM sales_traffic_asin WHERE period IN ('L52','P52') GROUP BY period
    """
    rows = {r["period"]: r for r in _rows(conn, sql)}
    conn.close()

    def _fmt_val(v, metric):
        if metric == "revenue":
            return f"${v:,.0f}"
        if metric in ("conversion_rate_pct","avg_buy_box_pct"):
            return f"{v:.1f}%"
        return f"{v:,.0f}"

    metrics = [
        ("units",               "Units",           "units",               "%"),
        ("revenue",             "Revenue",         "revenue",             "%"),
        ("sessions",            "Sessions",        "sessions",            "%"),
        ("page_views",          "Page Views",      "page_views",          "%"),
        ("conversion_rate_pct", "Conversion Rate", "conversion_rate_pct", "pp"),
        ("avg_buy_box_pct",     "Buy Box %",       "avg_buy_box_pct",     "pp"),
    ]
    results = []
    l52 = rows.get("L52", {})
    p52 = rows.get("P52", {})
    for key, label, field, kind in metrics:
        v_l52 = float(l52.get(field) or 0)
        v_p52 = float(p52.get(field) or 0)
        if kind == "%" and v_p52:
            change = (v_l52 - v_p52) / v_p52 * 100
        elif kind == "pp":
            change = v_l52 - v_p52
        else:
            change = 0
        results.append({
            "key": key, "label": label, "kind": kind,
            "l52": v_l52, "p52": v_p52,
            "l52_fmt": _fmt_val(v_l52, field),
            "p52_fmt": _fmt_val(v_p52, field),
            "change": round(change, 2),
            "positive": change >= 0,
        })
    return jsonify(results)


# ---------------------------------------------------------------------------
# API — /api/trends
# ---------------------------------------------------------------------------

@app.route("/api/trends")
def api_trends():
    conn = _conn()
    rows = _rows(conn, """
        SELECT period, month,
               SUM(units) AS units, SUM(revenue) AS revenue,
               SUM(sessions) AS sessions, SUM(page_views) AS page_views,
               CASE WHEN SUM(sessions)>0
                    THEN SUM(units)::FLOAT/SUM(sessions)*100 ELSE 0
               END AS conversion_rate_pct
        FROM sales_traffic_asin
        WHERE period IN ('L52','P52')
        GROUP BY period, month
        ORDER BY period, month
    """)
    conn.close()
    # Add calendar_month and deduplicate (boundary month appears once per period)
    for r in rows:
        r["calendar_month"] = int(r["month"][-2:])
    seen = set()
    deduped = []
    for r in rows:
        key = (r["period"], r["calendar_month"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return jsonify(deduped)


# ---------------------------------------------------------------------------
# API — /api/products
# ---------------------------------------------------------------------------

@app.route("/api/products")
def api_products():
    conn = _conn()

    meta = _period_meta(conn)
    month_start = request.args.get("month_start") or meta["L52"]["start_date"][:7]
    month_end = request.args.get("month_end") or meta["L52"]["end_date"][:7]
    date_params = (month_start, month_end)

    # Sales + traffic pivot from DB (single source of truth)
    st_rows = _rows(conn, """
        SELECT a.asin,
               COALESCE(l.product_name, '') AS product_name,
               SUM(CASE WHEN a.period='L52' THEN a.units ELSE 0 END)    AS l52_units,
               SUM(CASE WHEN a.period='P52' THEN a.units ELSE 0 END)    AS p52_units,
               SUM(CASE WHEN a.period='L52' THEN a.revenue ELSE 0 END)  AS l52_revenue,
               SUM(CASE WHEN a.period='P52' THEN a.revenue ELSE 0 END)  AS p52_revenue,
               SUM(CASE WHEN a.period='L52' THEN a.sessions ELSE 0 END) AS l52_sessions,
               SUM(CASE WHEN a.period='P52' THEN a.sessions ELSE 0 END) AS p52_sessions,
               CASE WHEN SUM(CASE WHEN a.period='L52' THEN a.sessions ELSE 0 END) > 0
                    THEN SUM(CASE WHEN a.period='L52' THEN a.units ELSE 0 END)::FLOAT
                         / SUM(CASE WHEN a.period='L52' THEN a.sessions ELSE 0 END) * 100
                    ELSE 0 END AS l52_cvr,
               CASE WHEN SUM(CASE WHEN a.period='P52' THEN a.sessions ELSE 0 END) > 0
                    THEN SUM(CASE WHEN a.period='P52' THEN a.units ELSE 0 END)::FLOAT
                         / SUM(CASE WHEN a.period='P52' THEN a.sessions ELSE 0 END) * 100
                    ELSE 0 END AS p52_cvr
        FROM sales_traffic_asin a
        LEFT JOIN listings l ON a.asin = l.asin
        WHERE a.period IN ('L52','P52')
          AND (a.period = 'P52' OR (a.month >= %s AND a.month <= %s))
        GROUP BY a.asin, l.product_name
    """, date_params)

    catalog = {}
    for r in _rows(conn, """
        SELECT asin, period,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(cart_adds) AS cart_adds, SUM(purchases) AS purchases,
               SUM(search_traffic_sales) AS search_sales
        FROM catalog_performance WHERE period IN ('L52','P52')
          AND (period = 'P52' OR (month >= %s AND month <= %s))
        GROUP BY asin, period
    """, date_params):
        catalog.setdefault(r["asin"], {})[r["period"]] = r

    repeat = {}
    for r in _rows(conn, """
        SELECT asin, period,
               SUM(orders) AS orders,
               AVG(repeat_customers_pct) AS avg_repeat_pct
        FROM repeat_purchase WHERE period IN ('L52','P52')
          AND (period = 'P52' OR (month >= %s AND month <= %s))
        GROUP BY asin, period
    """, date_params):
        repeat.setdefault(r["asin"], {})[r["period"]] = r
    conn.close()

    results = []
    for r in st_rows:
        asin = r["asin"]
        l52_u, p52_u = float(r["l52_units"]), float(r["p52_units"])
        l52_r, p52_r = float(r["l52_revenue"]), float(r["p52_revenue"])
        l52_s, p52_s = float(r["l52_sessions"]), float(r["p52_sessions"])
        cl52 = catalog.get(asin, {}).get("L52", {})
        cp52 = catalog.get(asin, {}).get("P52", {})
        rl52 = repeat.get(asin, {}).get("L52", {})
        results.append({
            "asin":                 asin,
            "product_name":         r["product_name"],
            "l52_units":            l52_u,
            "l52_revenue":          l52_r,
            "p52_units":            p52_u,
            "p52_revenue":          p52_r,
            "units_change_pct":     round((l52_u - p52_u) / p52_u * 100, 2) if p52_u else 0,
            "revenue_change_pct":   round((l52_r - p52_r) / p52_r * 100, 2) if p52_r else 0,
            "l52_sessions":         l52_s,
            "p52_sessions":         p52_s,
            "sessions_change_pct":  round((l52_s - p52_s) / p52_s * 100, 2) if p52_s else 0,
            "l52_cvr":              round(float(r["l52_cvr"]), 2),
            "p52_cvr":              round(float(r["p52_cvr"]), 2),
            "cvr_change_pp":        round(float(r["l52_cvr"]) - float(r["p52_cvr"]), 2),
            "l52_impressions":      float(cl52.get("impressions") or 0),
            "l52_clicks":           float(cl52.get("clicks") or 0),
            "l52_cart_adds":        float(cl52.get("cart_adds") or 0),
            "l52_search_sales":     float(cl52.get("search_sales") or 0),
            "p52_impressions":      float(cp52.get("impressions") or 0),
            "avg_repeat_pct":       round(float(rl52.get("avg_repeat_pct") or 0), 1),
        })
    results.sort(key=lambda r: r["l52_revenue"], reverse=True)
    return jsonify(results)


# ---------------------------------------------------------------------------
# API — /api/movers
# ---------------------------------------------------------------------------

@app.route("/api/movers")
def api_movers():
    conn = _conn()
    pivot_sql = """
        SELECT a.asin,
               COALESCE(l.product_name, '') AS product_name,
               SUM(CASE WHEN a.period='L52' THEN a.units ELSE 0 END)    AS l52_units,
               SUM(CASE WHEN a.period='P52' THEN a.units ELSE 0 END)    AS p52_units,
               SUM(CASE WHEN a.period='L52' THEN a.revenue ELSE 0 END)  AS l52_revenue,
               SUM(CASE WHEN a.period='P52' THEN a.revenue ELSE 0 END)  AS p52_revenue
        FROM sales_traffic_asin a
        LEFT JOIN listings l ON a.asin = l.asin
        WHERE a.period IN ('L52','P52')
        GROUP BY a.asin, l.product_name
    """
    rows = _rows(conn, pivot_sql)
    conn.close()

    for r in rows:
        r["units_change"] = float(r["l52_units"]) - float(r["p52_units"])
        r["revenue_change"] = float(r["l52_revenue"]) - float(r["p52_revenue"])
        p52_u = float(r["p52_units"])
        r["units_change_pct"] = round(r["units_change"] / p52_u * 100, 2) if p52_u else 0

    gainers = sorted(rows, key=lambda r: r["units_change"], reverse=True)[:10]
    decliners = sorted(rows, key=lambda r: r["units_change"])[:10]

    def _fmt(r):
        return {k: float(r[k]) if k != "asin" and k != "product_name" else r[k] for k in
                ("asin","product_name","l52_units","p52_units","l52_revenue","p52_revenue",
                 "units_change","units_change_pct","revenue_change")}

    return jsonify({"gainers": [_fmt(r) for r in gainers],
                     "decliners": [_fmt(r) for r in decliners]})


# ---------------------------------------------------------------------------
# API — /api/search-terms
# ---------------------------------------------------------------------------

@app.route("/api/search-terms")
def api_search_terms():
    conn = _conn()
    # Aggregate search terms by period directly from DB
    term_rows = _rows(conn, """
        SELECT search_term, period,
               AVG(click_share) AS avg_click_share,
               AVG(conversion_share) AS avg_conversion_share,
               AVG(search_freq_rank) AS avg_search_freq_rank,
               COUNT(DISTINCT month) AS months_present
        FROM search_terms
        WHERE period IN ('L52','P52')
        GROUP BY search_term, period
    """)

    # Sparkline data
    spark_rows = _rows(conn, """
        SELECT search_term, month, AVG(click_share)*100 AS avg_cs
        FROM   search_terms
        GROUP  BY search_term, month
        ORDER  BY search_term, month
    """)
    conn.close()

    spark_map: dict[str, dict[str, float]] = {}
    for r in spark_rows:
        spark_map.setdefault(r["search_term"], {})[r["month"]] = round(r["avg_cs"], 2)
    all_months = sorted(set(m for d in spark_map.values() for m in d))

    # Pivot L52/P52 per term
    terms: dict[str, dict] = {}
    for r in term_rows:
        t = terms.setdefault(r["search_term"], {})
        prefix = "l52" if r["period"] == "L52" else "p52"
        t[f"{prefix}_avg_click_share"] = round(float(r["avg_click_share"] or 0) * 100, 2)
        t[f"{prefix}_avg_conversion_share"] = round(float(r["avg_conversion_share"] or 0) * 100, 2)
        t[f"{prefix}_avg_search_freq_rank"] = float(r["avg_search_freq_rank"] or 0)
        t[f"{prefix}_months_present"] = int(r["months_present"])

    results = []
    for term, d in terms.items():
        l52_cs = d.get("l52_avg_click_share", 0)
        p52_cs = d.get("p52_avg_click_share", 0)
        spark = [spark_map.get(term, {}).get(m, 0) for m in all_months]
        results.append({
            "search_term":                  term,
            "l52_avg_click_share":          l52_cs,
            "p52_avg_click_share":          p52_cs,
            "click_share_change_pp":        round(l52_cs - p52_cs, 2),
            "l52_avg_conversion_share":     d.get("l52_avg_conversion_share", 0),
            "p52_avg_conversion_share":     d.get("p52_avg_conversion_share", 0),
            "l52_avg_search_freq_rank":     d.get("l52_avg_search_freq_rank", 0),
            "p52_avg_search_freq_rank":     d.get("p52_avg_search_freq_rank", 0),
            "l52_months_present":           d.get("l52_months_present", 0),
            "sparkline":                    spark,
            "sparkline_months":             all_months,
        })
    results.sort(key=lambda r: r["l52_avg_click_share"], reverse=True)
    return jsonify(results)


# ---------------------------------------------------------------------------
# API — /api/search-funnel
# ---------------------------------------------------------------------------

@app.route("/api/search-funnel")
def api_search_funnel():
    conn = _conn()

    monthly = _rows(conn, """
        SELECT month, period,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(cart_adds) AS cart_adds, SUM(purchases) AS purchases,
               SUM(search_traffic_sales) AS search_sales,
               CASE WHEN SUM(impressions)>0 THEN SUM(clicks)::FLOAT/SUM(impressions)*100 ELSE 0 END AS ctr,
               CASE WHEN SUM(clicks)>0 THEN SUM(cart_adds)::FLOAT/SUM(clicks)*100 ELSE 0 END AS cart_rate,
               CASE WHEN SUM(cart_adds)>0 THEN SUM(purchases)::FLOAT/SUM(cart_adds)*100 ELSE 0 END AS purchase_rate
        FROM catalog_performance
        WHERE period IN ('L52','P52')
        GROUP BY month
        ORDER BY month
    """)

    by_asin = _rows(conn, """
        SELECT asin, period,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(cart_adds) AS cart_adds, SUM(purchases) AS purchases,
               SUM(search_traffic_sales) AS search_sales,
               CASE WHEN SUM(impressions)>0 THEN SUM(clicks)::FLOAT/SUM(impressions)*100 ELSE 0 END AS ctr,
               CASE WHEN SUM(clicks)>0 THEN SUM(cart_adds)::FLOAT/SUM(clicks)*100 ELSE 0 END AS cart_rate,
               CASE WHEN SUM(cart_adds)>0 THEN SUM(purchases)::FLOAT/SUM(cart_adds)*100 ELSE 0 END AS purchase_rate
        FROM catalog_performance
        WHERE period IN ('L52','P52')
        GROUP BY asin, period
        ORDER BY SUM(search_traffic_sales) DESC
    """)

    totals = _rows(conn, """
        SELECT period,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(cart_adds) AS cart_adds, SUM(purchases) AS purchases,
               SUM(search_traffic_sales) AS search_sales
        FROM catalog_performance WHERE period IN ('L52','P52')
        GROUP BY period
    """)

    top_queries = _rows(conn, """
        SELECT search_query,
               SUM(asin_impressions) AS impressions,
               SUM(asin_clicks) AS clicks,
               SUM(asin_cart_adds) AS cart_adds,
               SUM(asin_purchases) AS purchases,
               COUNT(DISTINCT asin) AS asin_count
        FROM search_query_performance
        WHERE period = 'L52'
        GROUP BY search_query
        ORDER BY SUM(asin_clicks) DESC
        LIMIT 100
    """)

    conn.close()

    names = {}
    try:
        c2 = _conn()
        for r in _rows(c2, "SELECT asin, product_name FROM listings"):
            names[r["asin"]] = r["product_name"]
        c2.close()
    except Exception:
        pass

    for r in by_asin:
        r["product_name"] = names.get(r["asin"], "")

    return jsonify({
        "monthly": monthly,
        "by_asin": by_asin,
        "totals": {r["period"]: r for r in totals},
        "top_queries": top_queries,
    })


# ---------------------------------------------------------------------------
# API — /api/repeat-purchase
# ---------------------------------------------------------------------------

@app.route("/api/repeat-purchase")
def api_repeat_purchase():
    conn = _conn()
    rows = _rows(conn, """
        SELECT rp.asin, rp.period,
               SUM(rp.orders) AS orders,
               SUM(rp.unique_customers) AS unique_customers,
               AVG(rp.repeat_customers_pct) AS avg_repeat_pct,
               SUM(rp.repeat_purchase_revenue) AS repeat_purchase_revenue,
               l.product_name
        FROM repeat_purchase rp
        LEFT JOIN listings l ON rp.asin = l.asin
        WHERE rp.period IN ('L52','P52')
        GROUP BY rp.asin, rp.period
        ORDER BY SUM(rp.orders) DESC
    """)
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/asins (for autocomplete)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# API — /api/scoring-weights
# ---------------------------------------------------------------------------

@app.route("/api/scoring-weights")
def api_scoring_weights():
    """Return the current scoring weight configuration from build_asin_keywords."""
    try:
        from db.build_asin_keywords import (
            KW_RELEVANCE_WEIGHTS, ASIN_PRIORITY_WEIGHTS, ROLE_THRESHOLDS
        )
        return jsonify({
            "kw_relevance":    KW_RELEVANCE_WEIGHTS,
            "asin_priority":   ASIN_PRIORITY_WEIGHTS,
            "role_thresholds": ROLE_THRESHOLDS,
        })
    except ImportError:
        return jsonify({"error": "build_asin_keywords not found"}), 500


# ---------------------------------------------------------------------------
# API — /api/keywords
# ---------------------------------------------------------------------------

@app.route("/api/keywords")
def api_keywords():
    conn = _conn()
    strategy = request.args.get("strategy", "")
    sort     = request.args.get("sort", "volume")
    limit    = int(request.args.get("limit", 500))

    allowed_sorts = {
        "volume":            "kt.volume DESC NULLS LAST",
        "cvr_index":         "kt.cvr_index DESC NULLS LAST",
        "ctr_index":         "kt.ctr_index DESC NULLS LAST",
        "share_trend":       "kt.share_trend DESC NULLS LAST",
        "brand_purchase_share":"kt.brand_purchase_share DESC",
        "brand_purchases":     "kt.brand_purchases DESC",
    }
    order_clause = allowed_sorts.get(sort, "kt.volume DESC NULLS LAST")

    where = ""
    params: list = []
    if strategy and strategy != "All":
        where = "WHERE kt.strategy = %s"
        params.append(strategy)

    sql = f"""
        SELECT kt.search_query, kt.volume, kt.vol_tier, kt.strategy,
               kt.keyword_type, kt.asin_count, kt.cannibalization_flag,
               kt.brand_clicks, kt.brand_purchases,
               kt.brand_click_share, kt.brand_purchase_share,
               kt.mkt_cvr, kt.brand_cvr, kt.cvr_index,
               kt.brand_ctr, kt.mkt_ctr, kt.ctr_index,
               kt.hero_asin, kt.hero_cvr, kt.hero_aov,
               kt.share_trend, kt.months_of_data,
               kg.target_purchase_share, kg.priority, kg.notes,
               ads.ad_spend, ads.ad_clicks, ads.ad_sales, ads.ad_acos
        FROM keyword_targets kt
        LEFT JOIN keyword_goals kg ON kt.search_query = kg.search_query
        LEFT JOIN (
            SELECT LOWER(customer_search_term) AS term,
                   SUM(spend)  AS ad_spend,
                   SUM(clicks) AS ad_clicks,
                   SUM(sales)  AS ad_sales,
                   CASE WHEN SUM(sales) > 0
                        THEN SUM(spend) / SUM(sales) END AS ad_acos
            FROM ads_search_terms
            GROUP BY LOWER(customer_search_term)
        ) ads ON LOWER(kt.search_query) = ads.term
        {where}
        ORDER BY {order_clause}
        LIMIT %s
    """
    params.append(limit)
    rows = _rows(conn, sql, params)
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/keywords/monthly
# ---------------------------------------------------------------------------

@app.route("/api/keywords/monthly")
def api_keywords_monthly():
    """Per-(keyword, month) aggregated data for client-side time-horizon slicing."""
    conn = _conn()
    rows = _rows(conn, """
        WITH brand_mo AS (
            SELECT search_query, month,
                   SUM(asin_impressions)    AS brand_impressions,
                   SUM(asin_clicks)         AS brand_clicks,
                   SUM(asin_purchases)      AS brand_purchases,
                   AVG(search_query_volume) AS volume
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query, month
        ),
        mkt_mo AS (
            SELECT search_query, month,
                   MAX(total_impressions) AS mkt_impressions,
                   MAX(total_clicks)      AS mkt_clicks,
                   MAX(total_purchases)   AS mkt_purchases
            FROM search_query_performance
            WHERE period = 'L52'
            GROUP BY search_query, month
        )
        SELECT d.search_query, d.month,
               d.brand_impressions, d.brand_clicks, d.brand_purchases, CAST(d.volume AS INTEGER) AS volume,
               m.mkt_impressions, m.mkt_clicks, m.mkt_purchases
        FROM brand_mo d
        JOIN mkt_mo m ON d.search_query = m.search_query AND d.month = m.month
        ORDER BY d.search_query, d.month
    """)
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/keyword/<query>/asins
# ---------------------------------------------------------------------------

@app.route("/api/keyword/<path:query>/asins")
def api_keyword_asins(query: str):
    """All brand ASINs for a keyword, ranked by asin_priority."""
    conn = _conn()
    rows = _rows(conn, """
        SELECT
            s.asin,
            (SELECT product_name FROM listings WHERE asin = s.asin LIMIT 1) AS product_name,
            (SELECT price       FROM listings WHERE asin = s.asin LIMIT 1) AS price,
            s.asin_priority,
            s.keyword_role,
            s.keyword_type,
            s.within_kw_dominance_pct,
            s.within_kw_cvr_pct,
            s.within_kw_aov_pct,
            s.asin_cvr,
            s.adjusted_cvr,
            s.mkt_cvr,
            s.cvr_index,
            s.aov,
            s.revenue_score,
            s.asin_purchases,
            s.asin_clicks,
            s.click_share,
            s.purchase_share,
            s.share_trend
        FROM asin_keyword_scores s
        WHERE s.search_query = %s
        ORDER BY s.asin_priority DESC
    """, (query,))
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/asin/<asin>/keywords
# ---------------------------------------------------------------------------

@app.route("/api/asin/<asin>/keywords")
def api_asin_keywords(asin: str):
    """All keywords for an ASIN, ranked by keyword_relevance."""
    asin = asin.strip().upper()
    conn = _conn()

    rows = _rows(conn, """
        SELECT
            s.search_query,
            s.keyword_relevance,
            s.keyword_role,
            s.keyword_type,
            s.within_asin_traffic_pct,
            s.within_asin_revenue_pct,
            s.volume_pct,
            s.cvr_advantage_pct,
            s.headroom_pct,
            s.momentum_pct,
            s.search_volume,
            s.asin_clicks,
            s.asin_purchases,
            s.asin_cvr,
            s.adjusted_cvr,
            s.mkt_cvr,
            s.cvr_index,
            s.click_share,
            s.purchase_share,
            s.aov,
            s.revenue_score,
            s.share_trend,
            kt.strategy,
            kt.volume,
            kt.asin_count,
            kt.cannibalization_flag
        FROM asin_keyword_scores s
        LEFT JOIN keyword_targets kt ON s.search_query = kt.search_query
        WHERE s.asin = %s
        ORDER BY s.keyword_relevance DESC
    """, (asin,))

    # KPI summary for this ASIN's keyword portfolio
    total_kws = len(rows)
    total_traffic = sum(r["asin_clicks"] or 0 for r in rows)
    top10_traffic  = sum(r["asin_clicks"] or 0 for r in rows[:10])
    traffic_concentration = (top10_traffic / total_traffic * 100) if total_traffic > 0 else 0

    # Weighted avg CVR index (weighted by clicks)
    total_cvr_wt = sum(
        (r["cvr_index"] or 0) * (r["asin_clicks"] or 0)
        for r in rows if r["cvr_index"] is not None
    )
    total_cvr_clicks = sum(r["asin_clicks"] or 0 for r in rows if r["cvr_index"] is not None)
    avg_cvr_index = (total_cvr_wt / total_cvr_clicks) if total_cvr_clicks > 0 else None

    total_search_revenue = sum(r["revenue_score"] or 0 for r in rows)

    conn.close()
    return jsonify({
        "asin": asin,
        "kpis": {
            "total_keywords":         total_kws,
            "traffic_concentration":  round(traffic_concentration, 1),
            "avg_cvr_index":          round(avg_cvr_index, 3) if avg_cvr_index else None,
            "total_search_revenue":   round(total_search_revenue, 2),
        },
        "keywords": rows,
    })


# ---------------------------------------------------------------------------
# API — /api/keyword-share
# ---------------------------------------------------------------------------

@app.route("/api/keyword-share")
def api_keyword_share():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    conn = _conn()

    # Per-month aggregate: sum brand, max market (de-dup)
    sql = """
        WITH brand_mo AS (
            SELECT month,
                   SUM(asin_clicks)    AS brand_clicks,
                   SUM(asin_purchases) AS brand_purchases
            FROM search_query_performance
            WHERE search_query = %s AND period = 'L52'
            GROUP BY month
        ),
        mkt_mo AS (
            SELECT month,
                   MAX(total_clicks)    AS mkt_clicks,
                   MAX(total_purchases) AS mkt_purchases
            FROM search_query_performance
            WHERE search_query = %s AND period = 'L52'
            GROUP BY month
        )
        SELECT d.month,
               d.brand_clicks, d.brand_purchases, m.mkt_clicks, m.mkt_purchases,
               CASE WHEN m.mkt_clicks > 0
                    THEN d.brand_clicks::FLOAT / m.mkt_clicks * 100
                    ELSE 0 END AS brand_click_share,
               CASE WHEN m.mkt_purchases > 0
                    THEN d.brand_purchases::FLOAT / m.mkt_purchases * 100
                    ELSE 0 END AS brand_purchase_share,
               CASE WHEN d.brand_clicks > 0
                    THEN d.brand_purchases::FLOAT / d.brand_clicks * 100
                    ELSE NULL END AS brand_cvr,
               CASE WHEN m.mkt_clicks > 0
                    THEN m.mkt_purchases::FLOAT / m.mkt_clicks * 100
                    ELSE NULL END AS mkt_cvr
        FROM brand_mo d JOIN mkt_mo m ON d.month = m.month
        ORDER BY d.month
    """
    rows = _rows(conn, sql, (q, q))
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/keyword-goals (GET list + POST upsert)
# ---------------------------------------------------------------------------

@app.route("/api/keyword-goals", methods=["GET", "POST"])
def api_keyword_goals():
    from flask import request as req
    from datetime import datetime, timezone
    conn = _conn()

    if req.method == "POST":
        body = req.get_json(force=True) or {}
        search_query = str(body.get("search_query", "")).strip()
        if not search_query:
            conn.close()
            return jsonify({"error": "search_query required"}), 400
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO keyword_goals
               (search_query, target_purchase_share, priority, notes, updated_at)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (search_query) DO UPDATE SET
                 target_purchase_share = EXCLUDED.target_purchase_share,
                 priority = EXCLUDED.priority,
                 notes = EXCLUDED.notes,
                 updated_at = EXCLUDED.updated_at""",
            (
                search_query,
                body.get("target_purchase_share"),
                body.get("priority", "med"),
                body.get("notes", ""),
                now,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})

    rows = _rows(conn, """
        SELECT kg.*, kt.strategy, kt.brand_purchase_share, kt.volume
        FROM keyword_goals kg
        LEFT JOIN keyword_targets kt ON kg.search_query = kt.search_query
        ORDER BY kg.updated_at DESC
    """)
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/keyword-stats  (summary counts for tab KPI cards)
# ---------------------------------------------------------------------------

@app.route("/api/keyword-stats")
def api_keyword_stats():
    conn = _conn()
    counts = _rows(conn, """
        SELECT strategy, COUNT(*) AS cnt,
               SUM(brand_purchases) AS purchases
        FROM keyword_targets
        GROUP BY strategy
    """)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM keyword_targets
        WHERE strategy = 'Grow' AND cvr_index >= 1.5
    """)
    total_grow_opp = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM keyword_targets
        WHERE share_trend > 0.005 AND strategy NOT IN ('Branded','Defend')
    """)
    trending_up = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify({
        "by_strategy": counts,
        "high_cvr_grow": total_grow_opp,
        "trending_up": trending_up,
    })


# ---------------------------------------------------------------------------
# API — /api/highlights  (Executive YoY highlights)
# ---------------------------------------------------------------------------

@app.route("/api/highlights")
def api_highlights():
    conn = _conn()

    # Traffic/sales aggregates from per-ASIN monthly data
    daily = {r["period"]: r for r in _rows(conn, """
        SELECT period,
               COUNT(DISTINCT month) AS day_count,
               SUM(revenue)           AS total_revenue,
               SUM(units)             AS total_units,
               SUM(sessions)          AS total_sessions,
               AVG(conversion_rate)   AS avg_cvr,
               AVG(buy_box_pct)       AS avg_buy_box
        FROM sales_traffic_asin
        WHERE period IN ('L52','P52')
        GROUP BY period
    """)}

    # Repeat-purchase loyalty aggregates
    loyalty = {r["period"]: r for r in _rows(conn, """
        SELECT period,
               SUM(repeat_purchase_revenue)         AS repeat_purchase_revenue,
               SUM(unique_customers)       AS unique_customers,
               AVG(repeat_customers_pct)   AS avg_repeat_cust_pct,
               AVG(repeat_purchase_revenue_pct)     AS avg_repeat_rev_pct
        FROM repeat_purchase
        WHERE period IN ('L52','P52')
        GROUP BY period
    """)}

    # Search visibility (SQP) — use monthly averages to normalise unequal month coverage
    # (L52 has 12 months of data, P52 only has 5 months)
    search_vis = {r["period"]: r for r in _rows(conn, """
        SELECT period,
               COUNT(DISTINCT month)                                  AS month_count,
               SUM(asin_impressions)   / COUNT(DISTINCT month)        AS avg_impressions,
               SUM(asin_clicks)        / COUNT(DISTINCT month)        AS avg_clicks,
               SUM(asin_purchases)     / COUNT(DISTINCT month)        AS avg_purchases,
               SUM(asin_cart_adds)     / COUNT(DISTINCT month)        AS avg_cart_adds
        FROM search_query_performance
        WHERE period IN ('L52','P52')
        GROUP BY period
    """)}

    conn.close()

    def _safe(d, k):
        return float(d.get(k) or 0) if d else 0.0

    def _pct_chg(new, old):
        if old and old != 0:
            return round((new - old) / abs(old) * 100, 1)
        return None

    def _pp_chg(new, old):
        return round((new - old) * 100, 2) if old is not None else None

    l_d = daily.get("L52", {}); p_d = daily.get("P52", {})
    l_lo = loyalty.get("L52", {}); p_lo = loyalty.get("P52", {})
    l_s = search_vis.get("L52", {}); p_s = search_vis.get("P52", {})

    l_days = _safe(l_d, "day_count") or 1
    p_days = _safe(p_d, "day_count") or 1

    # Daily averages
    l_daily_rev  = _safe(l_d, "total_revenue") / l_days
    p_daily_rev  = _safe(p_d, "total_revenue") / p_days
    l_daily_units = _safe(l_d, "total_units") / l_days
    p_daily_units = _safe(p_d, "total_units") / p_days

    # Total revenue
    l_total_rev = _safe(l_d, "total_revenue")
    p_total_rev = _safe(p_d, "total_revenue")

    # CVR and buy box (already averages)
    l_cvr = _safe(l_d, "avg_cvr")
    p_cvr = _safe(p_d, "avg_cvr")
    l_bb  = _safe(l_d, "avg_buy_box")
    p_bb  = _safe(p_d, "avg_buy_box")

    # Loyalty
    l_rep_rev  = _safe(l_lo, "repeat_purchase_revenue")
    p_rep_rev  = _safe(p_lo, "repeat_purchase_revenue")
    l_rep_cust = _safe(l_lo, "avg_repeat_cust_pct") * 100
    p_rep_cust = _safe(p_lo, "avg_repeat_cust_pct") * 100
    l_rep_rev_pct = _safe(l_lo, "avg_repeat_rev_pct") * 100
    p_rep_rev_pct = _safe(p_lo, "avg_repeat_rev_pct") * 100
    l_uniq_cust = _safe(l_lo, "unique_customers")
    p_uniq_cust = _safe(p_lo, "unique_customers")

    # Search visibility — monthly averages (P52 only has 5 months, L52 has 12)
    l_imp   = _safe(l_s, "avg_impressions"); p_imp   = _safe(p_s, "avg_impressions")
    l_clk   = _safe(l_s, "avg_clicks");      p_clk   = _safe(p_s, "avg_clicks")
    l_purch = _safe(l_s, "avg_purchases");   p_purch = _safe(p_s, "avg_purchases")
    l_cart  = _safe(l_s, "avg_cart_adds");   p_cart  = _safe(p_s, "avg_cart_adds")
    l_sqp_months = int(_safe(l_s, "month_count")); p_sqp_months = int(_safe(p_s, "month_count"))

    # Need a fresh connection for _period_meta since we closed above
    conn2 = _conn()
    pmeta = _period_meta(conn2)
    conn2.close()

    return jsonify({
        "kpis": [
            {
                "id": "daily_revenue", "label": "Daily Avg Revenue",
                "l52": round(l_daily_rev, 2), "p52": round(p_daily_rev, 2),
                "l52_fmt": f"${l_daily_rev:,.0f}", "p52_fmt": f"${p_daily_rev:,.0f}",
                "change_pct": _pct_chg(l_daily_rev, p_daily_rev),
                "kind": "currency", "positive": l_daily_rev >= p_daily_rev,
            },
            {
                "id": "total_revenue", "label": "Total Revenue (L52)",
                "l52": round(l_total_rev, 2), "p52": round(p_total_rev, 2),
                "l52_fmt": f"${l_total_rev:,.0f}", "p52_fmt": f"${p_total_rev:,.0f}",
                "change_pct": _pct_chg(l_total_rev, p_total_rev),
                "kind": "currency", "positive": l_total_rev >= p_total_rev,
            },
            {
                "id": "daily_units", "label": "Daily Avg Units",
                "l52": round(l_daily_units, 1), "p52": round(p_daily_units, 1),
                "l52_fmt": f"{l_daily_units:,.0f}", "p52_fmt": f"{p_daily_units:,.0f}",
                "change_pct": _pct_chg(l_daily_units, p_daily_units),
                "kind": "number", "positive": l_daily_units >= p_daily_units,
            },
            {
                "id": "conversion_rate", "label": "Conversion Rate",
                "l52": round(l_cvr, 2), "p52": round(p_cvr, 2),
                "l52_fmt": f"{l_cvr:.1f}%", "p52_fmt": f"{p_cvr:.1f}%",
                "change_pct": _pct_chg(l_cvr, p_cvr),
                "kind": "percent", "positive": l_cvr >= p_cvr,
            },
            {
                "id": "buy_box", "label": "Buy Box %",
                "l52": round(l_bb, 2), "p52": round(p_bb, 2),
                "l52_fmt": f"{l_bb:.1f}%", "p52_fmt": f"{p_bb:.1f}%",
                "change_pct": _pct_chg(l_bb, p_bb),
                "kind": "percent", "positive": l_bb >= p_bb,
            },
        ],
        "loyalty": {
            "l52_repeat_revenue": round(l_rep_rev, 2),
            "p52_repeat_revenue": round(p_rep_rev, 2),
            "repeat_revenue_change_pct": _pct_chg(l_rep_rev, p_rep_rev),
            "l52_repeat_cust_pct": round(l_rep_cust, 2),
            "p52_repeat_cust_pct": round(p_rep_cust, 2),
            "l52_repeat_rev_pct": round(l_rep_rev_pct, 2),
            "p52_repeat_rev_pct": round(p_rep_rev_pct, 2),
            "l52_unique_customers": round(l_uniq_cust),
            "p52_unique_customers": round(p_uniq_cust),
        },
        "search_visibility": {
            "l52_impressions": round(l_imp), "p52_impressions": round(p_imp),
            "impressions_change_pct": _pct_chg(l_imp, p_imp),
            "l52_clicks": round(l_clk),      "p52_clicks": round(p_clk),
            "clicks_change_pct": _pct_chg(l_clk, p_clk),
            "l52_purchases": round(l_purch),  "p52_purchases": round(p_purch),
            "purchases_change_pct": _pct_chg(l_purch, p_purch),
            "l52_cart_adds": round(l_cart),   "p52_cart_adds": round(p_cart),
            "cart_adds_change_pct": _pct_chg(l_cart, p_cart),
            "l52_months": l_sqp_months, "p52_months": p_sqp_months,
            "note": f"Monthly averages (L52: {l_sqp_months} months, P52: {p_sqp_months} months)",
        },
        "periods": {
            "l52_days": int(l_days), "p52_days": int(p_days),
            **{f"{p.lower()}_range": m["label"] for p, m in pmeta.items()},
        },
    })


# ---------------------------------------------------------------------------
# API — /api/highlights-movers  (Top 5 ASINs by absolute revenue growth)
# ---------------------------------------------------------------------------

@app.route("/api/highlights-movers")
def api_highlights_movers():
    conn = _conn()
    rows = _rows(conn, """
        SELECT sta.asin,
               l.product_name,
               SUM(CASE WHEN sta.period='L52' THEN sta.revenue ELSE 0 END) AS l52_rev,
               SUM(CASE WHEN sta.period='P52' THEN sta.revenue ELSE 0 END) AS p52_rev,
               SUM(CASE WHEN sta.period='L52' THEN sta.units   ELSE 0 END) AS l52_units,
               SUM(CASE WHEN sta.period='P52' THEN sta.units   ELSE 0 END) AS p52_units
        FROM sales_traffic_asin sta
        LEFT JOIN listings l ON sta.asin = l.asin
        WHERE sta.period IN ('L52','P52')
        GROUP BY sta.asin, l.product_name
        HAVING SUM(CASE WHEN sta.period='P52' THEN sta.revenue ELSE 0 END) > 0
        ORDER BY (SUM(CASE WHEN sta.period='L52' THEN sta.revenue ELSE 0 END) - SUM(CASE WHEN sta.period='P52' THEN sta.revenue ELSE 0 END)) DESC
        LIMIT 5
    """)
    conn.close()

    result = []
    for r in rows:
        l_rev = float(r["l52_rev"] or 0)
        p_rev = float(r["p52_rev"] or 0)
        l_u   = float(r["l52_units"] or 0)
        p_u   = float(r["p52_units"] or 0)
        rev_growth = l_rev - p_rev
        pct_growth = (rev_growth / p_rev * 100) if p_rev else 0
        result.append({
            "asin":         r["asin"],
            "product_name": r["product_name"] or r["asin"],
            "l52_rev":      round(l_rev, 2),
            "p52_rev":      round(p_rev, 2),
            "l52_units":    round(l_u),
            "p52_units":    round(p_u),
            "rev_growth":   round(rev_growth, 2),
            "pct_growth":   round(pct_growth, 1),
        })
    return jsonify(result)


# ---------------------------------------------------------------------------
# API — Advertising endpoints
# ---------------------------------------------------------------------------

@app.route("/api/revenue-by-month")
def api_revenue_by_month():
    conn = _conn()
    rows = _rows(conn, """
        SELECT month, SUM(revenue) AS revenue
        FROM sales_traffic_asin
        GROUP BY month ORDER BY month
    """)
    conn.close()
    return jsonify(rows)

@app.route("/api/ads/summary")
def api_ads_summary():
    conn = _conn()
    rows = _rows(conn, """
        SELECT ad_type, attribution_window,
               COUNT(*)            AS campaigns,
               SUM(spend)          AS spend,
               SUM(impressions)    AS impressions,
               SUM(clicks)         AS clicks,
               SUM(orders)         AS orders,
               SUM(sales)          AS sales,
               CASE WHEN SUM(sales) > 0
                    THEN SUM(spend) / SUM(sales) END AS acos,
               CASE WHEN SUM(spend) > 0
                    THEN SUM(sales) / SUM(spend) END AS roas,
               CASE WHEN SUM(impressions) > 0
                    THEN SUM(clicks)::float / SUM(impressions) END AS ctr,
               CASE WHEN SUM(clicks) > 0
                    THEN SUM(spend) / SUM(clicks) END AS cpc
        FROM ads_campaigns
        GROUP BY ad_type, attribution_window
        ORDER BY ad_type
    """)
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/summary/monthly")
def api_ads_summary_monthly():
    """Per-month campaign-level totals for client-side horizon slicing."""
    conn = _conn()
    rows = _rows(conn, """
        SELECT month,
               COUNT(*)         AS campaigns,
               SUM(spend)       AS spend,
               SUM(impressions) AS impressions,
               SUM(clicks)      AS clicks,
               SUM(orders)      AS orders,
               SUM(sales)       AS sales
        FROM ads_campaigns
        GROUP BY month
        ORDER BY month
    """)
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/keywords/monthly")
def api_ads_keywords_monthly():
    """Per-(keyword, month) ad data for client-side horizon slicing."""
    conn = _conn()
    rows = _rows(conn, """
        SELECT LOWER(customer_search_term) AS search_term, month,
               SUM(spend)       AS ad_spend,
               SUM(impressions) AS ad_impressions,
               SUM(clicks)      AS ad_clicks,
               SUM(orders)      AS ad_orders,
               SUM(units)       AS ad_units,
               SUM(sales)       AS ad_sales,
               COUNT(DISTINCT campaign_name) AS num_campaigns,
               STRING_AGG(DISTINCT ad_type, ',') AS ad_type_list,
               MIN(impression_rank)  AS best_impression_rank,
               MAX(impression_share) AS best_impression_share
        FROM ads_search_terms
        GROUP BY LOWER(customer_search_term), month
        ORDER BY search_term, month
    """)
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/keywords")
def api_ads_keywords():
    """Level 1: keyword-level aggregated view with organic data joined."""
    conn = _conn()
    sort = request.args.get("sort", "ad_spend")
    strategy = request.args.get("strategy", "")
    ad_type = request.args.get("ad_type", "")
    limit = int(request.args.get("limit", 500))

    allowed_sorts = {
        "ad_spend":      "a.ad_spend DESC NULLS LAST",
        "ad_sales":      "a.ad_sales DESC NULLS LAST",
        "ad_acos":       "a.ad_acos ASC NULLS LAST",
        "ad_roas":       "a.ad_roas DESC NULLS LAST",
        "ad_clicks":     "a.ad_clicks DESC NULLS LAST",
        "impressions":   "a.ad_impressions DESC NULLS LAST",
        "organic_volume":"kt.volume DESC NULLS LAST",
        "cvr_index":     "kt.cvr_index DESC NULLS LAST",
    }
    order_clause = allowed_sorts.get(sort, "a.ad_spend DESC NULLS LAST")

    where_parts = []
    params: list = []
    if strategy and strategy != "All":
        where_parts.append("kt.strategy = %s")
        params.append(strategy)
    if ad_type and ad_type != "All":
        where_parts.append("a.ad_type_list LIKE %s")
        params.append(f"%{ad_type}%")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = f"""
        WITH ad_agg AS (
            SELECT
                LOWER(customer_search_term) AS search_term,
                STRING_AGG(DISTINCT ad_type, ',') AS ad_type_list,
                SUM(spend)       AS ad_spend,
                SUM(impressions) AS ad_impressions,
                SUM(clicks)      AS ad_clicks,
                SUM(orders)      AS ad_orders,
                SUM(sales)       AS ad_sales,
                CASE WHEN SUM(sales) > 0
                     THEN SUM(spend) / SUM(sales) END AS ad_acos,
                CASE WHEN SUM(spend) > 0
                     THEN SUM(sales) / SUM(spend) END AS ad_roas,
                COUNT(DISTINCT campaign_name) AS num_campaigns,
                MIN(impression_rank)  AS best_impression_rank,
                MAX(impression_share) AS best_impression_share
            FROM ads_search_terms
            GROUP BY LOWER(customer_search_term)
        )
        SELECT a.search_term, a.ad_type_list, a.ad_spend, a.ad_impressions,
               a.ad_clicks, a.ad_orders, a.ad_sales, a.ad_acos, a.ad_roas,
               a.num_campaigns, a.best_impression_rank, a.best_impression_share,
               kt.volume AS organic_volume, kt.strategy,
               kt.keyword_type, kt.brand_purchase_share AS organic_purchase_share,
               kt.cvr_index, kt.brand_cvr AS organic_cvr,
               kt.share_trend, kt.vol_tier,
               kt.brand_clicks AS organic_clicks,
               kt.brand_purchases AS organic_purchases
        FROM ad_agg a
        LEFT JOIN keyword_targets kt ON a.search_term = LOWER(kt.search_query)
        {where}
        ORDER BY {order_clause}
        LIMIT %s
    """
    params.append(limit)
    rows = _rows(conn, sql, params)
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/keyword/<path:term>/products")
def api_ads_keyword_products(term: str):
    """Level 2: per-ASIN breakdown for a keyword."""
    conn = _conn()
    rows = _rows(conn, """
        WITH ad_by_asin AS (
            SELECT
                ap.asin,
                SUM(ast.spend)   AS ad_spend,
                SUM(ast.clicks)  AS ad_clicks,
                SUM(ast.orders)  AS ad_orders,
                SUM(ast.sales)   AS ad_sales,
                CASE WHEN SUM(ast.sales) > 0
                     THEN SUM(ast.spend) / SUM(ast.sales) END AS ad_acos,
                SUM(ast.own_sku_sales)   AS own_sku_sales,
                SUM(ast.other_sku_sales) AS other_sku_sales,
                COUNT(DISTINCT ast.campaign_name) AS num_campaigns,
                STRING_AGG(DISTINCT ast.ad_type, ',') AS ad_types
            FROM ads_search_terms ast
            JOIN ads_products ap
              ON ast.campaign_name = ap.campaign_name
             AND ast.ad_group_name = ap.ad_group_name
             AND ast.ad_type = ap.ad_type
             AND ast.month = ap.month
            WHERE LOWER(ast.customer_search_term) = LOWER(%s)
            GROUP BY ap.asin
        )
        SELECT a.asin, a.ad_spend, a.ad_clicks, a.ad_orders, a.ad_sales,
               a.ad_acos, a.own_sku_sales, a.other_sku_sales,
               a.num_campaigns, a.ad_types,
               (SELECT product_name FROM listings WHERE asin = a.asin LIMIT 1) AS product_name,
               s.keyword_relevance, s.asin_priority, s.keyword_role,
               s.purchase_share AS organic_purchase_share,
               s.cvr_index, s.asin_cvr, s.adjusted_cvr, s.mkt_cvr,
               s.revenue_score, s.share_trend
        FROM ad_by_asin a
        LEFT JOIN asin_keyword_scores s
          ON a.asin = s.asin AND LOWER(s.search_query) = LOWER(%s)
        ORDER BY a.ad_spend DESC
    """, (term, term))
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/keyword/<path:term>/product/<asin>/campaigns")
def api_ads_keyword_product_campaigns(term: str, asin: str):
    """Level 3: campaign-level detail for a keyword + ASIN."""
    asin = asin.strip().upper()
    conn = _conn()
    rows = _rows(conn, """
        SELECT ast.campaign_name, ast.ad_type, ast.match_type,
               ast.ad_group_name, ast.targeting_text,
               ast.spend, ast.cpc, ast.clicks, ast.impressions,
               ast.orders, ast.sales, ast.acos, ast.roas,
               ast.impression_rank, ast.impression_share,
               ast.cvr
        FROM ads_search_terms ast
        JOIN ads_products ap
          ON ast.campaign_name = ap.campaign_name
         AND ast.ad_group_name = ap.ad_group_name
         AND ast.ad_type = ap.ad_type
         AND ast.month = ap.month
        WHERE LOWER(ast.customer_search_term) = LOWER(%s)
          AND ap.asin = %s
        ORDER BY ast.spend DESC
    """, (term, asin))
    conn.close()
    return jsonify(rows)


@app.route("/api/ads/campaigns")
def api_ads_campaigns():
    conn = _conn()
    rows = _rows(conn, """
        SELECT campaign_name, ad_type, attribution_window, status,
               portfolio_name, budget, spend, impressions, clicks,
               ctr, cpc, orders, units, sales, acos, roas,
               bidding_strategy, targeting_type,
               avg_time_in_budget, recommended_budget,
               est_missed_imp_lower, est_missed_imp_upper,
               est_missed_sales_lower, est_missed_sales_upper,
               ntb_orders, ntb_sales, branded_searches
        FROM ads_campaigns
        ORDER BY spend DESC NULLS LAST
    """)
    conn.close()
    return jsonify(rows)


@app.route("/api/asins")
def api_asins():
    conn = _conn()
    rows = _rows(conn, """
        SELECT DISTINCT l.asin, l.product_name, l.sku, l.price
        FROM listings l
        ORDER BY l.product_name
    """)
    conn.close()
    return jsonify(rows)


# ---------------------------------------------------------------------------
# API — /api/asin/<asin>
# ---------------------------------------------------------------------------

@app.route("/api/asin/<asin>")
def api_asin(asin: str):
    asin = asin.strip().upper()
    conn = _conn()

    listing = _rows(conn, "SELECT * FROM listings WHERE asin=%s LIMIT 1", (asin,))

    monthly = _rows(conn, """
        SELECT month, period, units, revenue, sessions,
               page_views, conversion_rate AS conversion_rate_pct, buy_box_pct
        FROM   sales_traffic_asin
        WHERE  asin=%s ORDER BY month
    """, (asin,))

    terms = _rows(conn, """
        SELECT month, period, search_term, search_freq_rank,
               click_share_rank,
               click_share*100      AS click_share_pct,
               conversion_share*100 AS conversion_share_pct
        FROM   search_terms
        WHERE  clicked_asin=%s
        ORDER  BY month, click_share DESC
    """, (asin,))

    catalog = _rows(conn, """
        SELECT month, period, impressions, clicks, click_rate,
               cart_adds, purchases, conversion_rate, search_traffic_sales
        FROM   catalog_performance
        WHERE  asin=%s ORDER BY month
    """, (asin,))

    repeat_purchase = _rows(conn, """
        SELECT month, period, orders, unique_customers,
               repeat_customers_pct, repeat_purchase_revenue, repeat_purchase_revenue_pct
        FROM   repeat_purchase
        WHERE  asin=%s ORDER BY month
    """, (asin,))

    market_basket = _rows(conn, """
        SELECT mb.month, mb.period, mb.purchased_with_asin,
               mb.purchased_with_rank, mb.combination_pct,
               l.product_name AS pw_product_name
        FROM   market_basket mb
        LEFT JOIN listings l ON mb.purchased_with_asin = l.asin
        WHERE  mb.asin=%s
        ORDER  BY mb.month, mb.purchased_with_rank
    """, (asin,))

    top_queries = _rows(conn, """
        SELECT search_query,
               SUM(asin_impressions) AS impressions,
               SUM(asin_clicks) AS clicks,
               SUM(asin_cart_adds) AS cart_adds,
               SUM(asin_purchases) AS purchases,
               AVG(asin_impression_share) AS avg_imp_share,
               AVG(asin_click_share) AS avg_click_share
        FROM   search_query_performance
        WHERE  asin=%s AND period='L52'
        GROUP  BY search_query
        ORDER  BY SUM(asin_clicks) DESC
        LIMIT  50
    """, (asin,))

    sqp_monthly = _rows(conn, """
        SELECT month, period,
               SUM(asin_impressions)  AS asin_impressions,
               SUM(asin_clicks)       AS asin_clicks,
               SUM(asin_purchases)    AS asin_purchases,
               SUM(total_impressions) AS total_impressions,
               SUM(total_clicks)      AS total_clicks
        FROM   search_query_performance
        WHERE  asin=%s
        GROUP  BY month, period
        ORDER  BY month
    """, (asin,))

    conn.close()

    mb_agg = {}
    for r in market_basket:
        pw = r["purchased_with_asin"]
        if pw not in mb_agg:
            mb_agg[pw] = {"purchased_with_asin": pw, "product_name": r.get("pw_product_name",""), "count": 0, "total_pct": 0}
        mb_agg[pw]["count"] += 1
        mb_agg[pw]["total_pct"] += r["combination_pct"]
    mb_summary = sorted(mb_agg.values(), key=lambda x: x["total_pct"], reverse=True)[:20]
    for item in mb_summary:
        item["avg_pct"] = round(item["total_pct"] / max(item["count"], 1), 2)

    return jsonify({
        "asin":             asin,
        "listing":          listing[0] if listing else {},
        "monthly_sales":    monthly,
        "search_terms":     terms,
        "catalog":          catalog,
        "repeat_purchase":  repeat_purchase,
        "market_basket":    mb_summary,
        "top_queries":      top_queries,
        "sqp_monthly":      sqp_monthly,
    })


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nire Beauty Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--surface:#161b22;--surface2:#1e2533;--border:#2d3148;
  --text:#e6edf3;--muted:#8b949e;--blue:#58a6ff;--green:#3fb950;
  --red:#f85149;--yellow:#e3b341;--purple:#bc8cff;--orange:#d29922;
  --cyan:#56d4dd;
}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
a{color:var(--blue);text-decoration:none}

.header{background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;display:flex;align-items:center;gap:24px;height:56px}
.header h1{font-size:1.1rem;font-weight:700;color:var(--text);white-space:nowrap}
.header .tagline{font-size:0.78rem;color:var(--muted)}
nav{display:flex;gap:2px;margin-left:auto}
nav button{background:none;border:none;color:var(--muted);padding:8px 16px;border-radius:6px;cursor:pointer;font-size:0.85rem;font-weight:500;transition:all .15s}
nav button:hover{background:var(--surface2);color:var(--text)}
nav button.active{background:var(--surface2);color:var(--blue)}
.main{padding:24px;max-width:1500px;margin:0 auto}
.tab-panel{display:none}.tab-panel.active{display:block}

.kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:24px}
.kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}
.kpi-label{font-size:0.7rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:8px}
.kpi-l52{font-size:1.5rem;font-weight:700;color:var(--text);line-height:1}
.kpi-p52{font-size:0.78rem;color:var(--muted);margin-top:4px}
.kpi-change{font-size:0.82rem;font-weight:600;margin-top:6px}
.kpi-change.up{color:var(--green)}.kpi-change.down{color:var(--red)}

.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px}
.card-title{font-size:0.78rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:16px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px}
.three-col{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px}
.chart-wrap{position:relative;height:320px}

.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem}
th{text-align:left;padding:8px 12px;color:var(--muted);font-weight:500;font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}
th:hover{color:var(--text)}
th.sort-asc::after{content:" ▲";color:var(--blue);font-weight:700}th.sort-desc::after{content:" ▼";color:var(--blue);font-weight:700}
td{padding:8px 12px;border-bottom:1px solid #1a1f2e;vertical-align:middle}
tr:hover td{background:var(--surface2)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.cell-up{color:var(--green)}.cell-down{color:var(--red)}.cell-neutral{color:var(--muted)}
.product-name{max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.asin-link{color:var(--blue);cursor:pointer;font-size:0.75rem}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.7rem;font-weight:600}
.badge-l52{background:#1a3a5c;color:var(--blue)}.badge-p52{background:#2a2a3a;color:var(--purple)}

.spark{display:inline-flex;align-items:flex-end;gap:1px;height:20px;vertical-align:middle}
.spark span{display:inline-block;width:3px;background:var(--blue);border-radius:1px;min-height:2px}


.search-wrap{position:relative;margin-bottom:24px}
.search-wrap input{width:100%;padding:12px 16px;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:0.95rem;outline:none}
.search-wrap input:focus{border-color:var(--blue)}
.autocomplete-list{position:absolute;top:100%;left:0;right:0;background:var(--surface2);border:1px solid var(--border);border-radius:8px;margin-top:4px;max-height:280px;overflow-y:auto;z-index:100;display:none}
.autocomplete-list.open{display:block}
.ac-item{padding:10px 16px;cursor:pointer;border-bottom:1px solid var(--border)}
.ac-item:hover{background:var(--surface)}
.ac-item .ac-asin{font-size:0.72rem;color:var(--muted)}
.product-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px;display:grid;grid-template-columns:1fr auto;gap:16px}
.product-card h2{font-size:1.1rem;font-weight:600;margin-bottom:8px}
.product-meta{display:flex;gap:16px;flex-wrap:wrap}
.meta-item{font-size:0.78rem;color:var(--muted)}.meta-item strong{color:var(--text);font-weight:600}
.explorer-placeholder{text-align:center;padding:60px 24px;color:var(--muted)}
.explorer-placeholder h3{font-size:1.1rem;margin-bottom:8px;color:var(--text)}

.loading{text-align:center;padding:40px;color:var(--muted);font-size:0.85rem}
.section-head{font-size:0.9rem;font-weight:600;color:var(--text);margin:20px 0 12px;padding-bottom:8px;border-bottom:1px solid var(--border)}

/* Keywords tab */
.chip-row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}
.chip{padding:5px 14px;border-radius:20px;border:1px solid var(--border);background:var(--surface2);color:var(--muted);font-size:0.78rem;font-weight:500;cursor:pointer;transition:all .15s}
.chip:hover{border-color:var(--blue);color:var(--text)}
.chip.active{background:var(--blue);border-color:var(--blue);color:#fff}
.chip-branded{border-color:#1a3a5c}.chip-branded.active{background:#1a3a5c;color:var(--blue)}
.chip-defend{border-color:#1a3a3a}.chip-defend.active{background:#1a3a3a;color:var(--cyan)}
.chip-grow{border-color:#1a3a2c}.chip-grow.active{background:#1a3a2c;color:var(--green)}
.chip-watch{border-color:#3a2a1a}.chip-watch.active{background:#3a2a1a;color:var(--yellow)}
.chip-deprioritize{border-color:#1e2533}.chip-deprioritize.active{background:#1e2533;color:var(--muted)}

.badge-branded{background:#1a3a5c;color:var(--blue)}
.badge-defend{background:#1a3a3a;color:var(--cyan)}
.badge-grow{background:#1a3a2c;color:var(--green)}
.badge-watch{background:#3a2a1a;color:var(--yellow)}
.badge-deprioritize{background:#1e2533;color:var(--muted)}

.trend-up{color:var(--green)}.trend-down{color:var(--red)}.trend-flat{color:var(--muted)}
.kw-row-selected td{background:rgba(88,166,255,.08)!important}
.row-active td{background:rgba(88,166,255,.08)!important}
.goal-set{color:var(--green);font-size:0.72rem}

/* Keywords filter bar */
.kw-filter-bar{background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-direction:column;gap:12px}
.kw-filter-row{display:flex;align-items:center;flex-wrap:wrap;gap:10px}
.kw-filter-label{font-size:0.7rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;min-width:80px;flex-shrink:0}
.kw-filter-bar input[type=text]{background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.82rem;padding:5px 10px;outline:none;transition:border-color .15s;width:260px}
.kw-filter-bar input[type=text]:focus{border-color:var(--blue)}
.kw-filter-bar select{background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.78rem;padding:4px 8px;outline:none;cursor:pointer}
.kw-filter-bar select:focus{border-color:var(--blue)}
.kw-filter-bar input[type=month]{background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.78rem;padding:4px 8px;outline:none;cursor:pointer;color-scheme:dark}
.kw-filter-bar input[type=month]:focus{border-color:var(--blue)}
.kw-horizon-btn{padding:4px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--muted);font-size:0.78rem;font-weight:600;cursor:pointer;transition:all .15s}
.kw-horizon-btn:hover{border-color:var(--blue);color:var(--text)}
.kw-horizon-btn.active{background:var(--blue);border-color:var(--blue);color:#fff}
.kw-cannibal-toggle{display:flex;align-items:center;gap:6px;font-size:0.78rem;color:var(--muted);cursor:pointer}
.kw-cannibal-toggle input{accent-color:var(--yellow);cursor:pointer}
.kw-cannibal-toggle.active-label{color:var(--yellow)}
.kw-filter-count{font-size:0.72rem;color:var(--muted);margin-left:auto;white-space:nowrap}
.kw-horizon-period{font-size:0.72rem;color:var(--muted);font-style:italic;margin-left:6px}

.tracker-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px;display:none}
.tracker-wrap.visible{display:block}
.tracker-title{font-size:0.85rem;font-weight:600;color:var(--text);margin-bottom:4px}
.tracker-sub{font-size:0.75rem;color:var(--muted);margin-bottom:16px}
.tracker-chart-wrap{position:relative;height:240px}

.kw-kpi-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}

/* Scoring weights panel */
.weights-panel{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:14px 18px;margin-bottom:20px;font-size:0.78rem}
.weights-panel summary{cursor:pointer;font-weight:600;color:var(--muted);font-size:0.72rem;text-transform:uppercase;letter-spacing:.07em;list-style:none;display:flex;align-items:center;gap:8px}
.weights-panel summary::before{content:"\25b6";font-size:0.6rem;transition:transform .2s}
.weights-panel[open] summary::before{transform:rotate(90deg)}
.weights-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:12px}
.weights-group-title{font-weight:600;color:var(--text);margin-bottom:8px;font-size:0.75rem}
.weight-row{display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid rgba(45,49,72,.6)}
.weight-label{color:var(--muted)}
.weight-val{font-weight:600;color:var(--blue);font-variant-numeric:tabular-nums}
.weight-bar-wrap{width:60px;height:6px;background:var(--border);border-radius:3px;margin-left:8px}
.weight-bar{height:100%;background:var(--blue);border-radius:3px}

/* ASIN allocation panel */
.allocation-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px;display:none}
.allocation-wrap.visible{display:block}
.allocation-title{font-size:0.85rem;font-weight:600;color:var(--text);margin-bottom:4px}
.allocation-sub{font-size:0.75rem;color:var(--muted);margin-bottom:16px}
.cannibal-warn{display:inline-block;padding:2px 8px;border-radius:4px;background:rgba(227,179,65,.15);color:var(--yellow);font-size:0.7rem;font-weight:600;margin-left:8px}

/* Role badges */
.role-core{background:#1a3a5c;color:var(--blue)}
.role-defend{background:#1a2a4a;color:#79c0ff}
.role-growth{background:#1a3a2c;color:var(--green)}
.role-aspirational{background:#2a1a3a;color:var(--purple)}
.role-halo{background:#1e2533;color:var(--muted)}
.role-harvest{background:#2a3a1a;color:var(--cyan)}
.role-other{background:#1e2533;color:var(--muted)}

/* Highlights tab */
.hl-hero-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:28px}
.hl-hero-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px 18px;position:relative;overflow:hidden}
.hl-hero-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:12px 12px 0 0}
.hl-hero-card.up::before{background:var(--green)}
.hl-hero-card.down::before{background:var(--red)}
.hl-hero-label{font-size:0.68rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:10px}
.hl-hero-val{font-size:1.8rem;font-weight:700;color:var(--text);line-height:1.1;margin-bottom:4px}
.hl-hero-prior{font-size:0.76rem;color:var(--muted);margin-bottom:8px}
.hl-hero-badge{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:20px;font-size:0.8rem;font-weight:700}
.hl-hero-badge.up{background:rgba(63,185,80,.15);color:var(--green)}
.hl-hero-badge.down{background:rgba(248,81,73,.15);color:var(--red)}

.hl-section-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
.hl-section-grid.three{grid-template-columns:1fr 1fr 1fr}

.hl-stat-row{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border)}
.hl-stat-row:last-child{border-bottom:none}
.hl-stat-label{font-size:0.82rem;color:var(--muted)}
.hl-stat-vals{display:flex;align-items:center;gap:16px}
.hl-stat-l52{font-size:0.9rem;font-weight:600;color:var(--text)}
.hl-stat-p52{font-size:0.78rem;color:var(--muted)}
.hl-stat-pill{display:inline-block;padding:2px 8px;border-radius:12px;font-size:0.72rem;font-weight:700}
.hl-stat-pill.up{background:rgba(63,185,80,.15);color:var(--green)}
.hl-stat-pill.down{background:rgba(248,81,73,.15);color:var(--red)}

.hl-mover-bar-wrap{margin-bottom:16px}
.hl-mover-label{font-size:0.75rem;color:var(--muted);margin-bottom:4px;display:flex;justify-content:space-between}
.hl-mover-label span{color:var(--green);font-weight:700}
.hl-bar-track{height:28px;background:var(--surface2);border-radius:6px;overflow:hidden;position:relative}
.hl-bar-l52{height:100%;background:linear-gradient(90deg,#1a3a5c,#58a6ff);border-radius:6px;display:flex;align-items:center;padding-left:10px;font-size:0.75rem;font-weight:600;color:#fff;white-space:nowrap;overflow:hidden;transition:width .7s cubic-bezier(.4,0,.2,1)}
.hl-bar-p52-marker{position:absolute;top:4px;bottom:4px;width:2px;background:var(--purple);border-radius:1px}

.hl-chart-wrap{position:relative;height:280px}

.hl-period-note{font-size:0.72rem;color:var(--muted);margin-bottom:20px;padding:8px 12px;background:var(--surface2);border-radius:6px;display:inline-block}
</style>
</head>
<body>

<div class="header">
  <h1>Nire Beauty Analytics</h1>
  <span class="tagline" id="tagline">Full Brand Analytics Suite</span>
  <nav>
    <button class="active" onclick="showTab('overview')">Overview</button>
    <button onclick="showTab('highlights')" style="color:var(--green);font-weight:700">&#9733; Highlights</button>
    <button onclick="showTab('products')">Products</button>

    <button onclick="showTab('search')">Search Terms</button>
    <button onclick="showTab('explorer')">ASIN Explorer</button>
    <button onclick="showTab('keywords')">Keywords</button>
    <button onclick="showTab('advertising')">Advertising</button>
  </nav>
</div>

<div class="main">

  <!-- ===== OVERVIEW ===== -->
  <div id="tab-overview" class="tab-panel active">
    <div class="kpi-grid" id="kpi-grid"><div class="loading">Loading...</div></div>
    <div class="card">
      <div class="card-title" id="title-revenue">Monthly Revenue</div>
      <div class="chart-wrap"><canvas id="chart-revenue"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" id="title-units">Monthly Units</div>
      <div class="chart-wrap"><canvas id="chart-units"></canvas></div>
    </div>
    <div class="two-col">
      <div class="card">
        <div class="card-title">Top Gainers (by units)</div>
        <div id="gainers-table"></div>
      </div>
      <div class="card">
        <div class="card-title">Top Decliners (by units)</div>
        <div id="decliners-table"></div>
      </div>
    </div>
  </div>

  <!-- ===== HIGHLIGHTS ===== -->
  <div id="tab-highlights" class="tab-panel">
    <p class="hl-period-note" id="hl-period-note">Loading period info...</p>

    <!-- Hero KPI Cards -->
    <div class="hl-hero-grid" id="hl-hero-grid">
      <div class="loading">Loading...</div>
    </div>

    <div class="hl-section-grid">
      <!-- Search Visibility -->
      <div class="card">
        <div class="card-title">Search Visibility -- Brand Reach YoY</div>
        <div id="hl-search-stats"></div>
      </div>

      <!-- Loyalty -->
      <div class="card">
        <div class="card-title">Customer Loyalty -- Repeat Business YoY</div>
        <div id="hl-loyalty-stats"></div>
      </div>
    </div>

    <div class="hl-section-grid">
      <!-- Top 5 Revenue Movers chart -->
      <div class="card">
        <div class="card-title" id="title-movers">Top 5 Revenue Breakouts</div>
        <div class="hl-chart-wrap"><canvas id="chart-hl-movers"></canvas></div>
      </div>

      <!-- Loyalty bar chart -->
      <div class="card">
        <div class="card-title">Repeat Revenue vs Total Revenue Trend</div>
        <div class="hl-chart-wrap"><canvas id="chart-hl-loyalty"></canvas></div>
      </div>
    </div>

    <!-- Movers detail bars -->
    <div class="card">
      <div class="card-title">Breakout ASIN Detail -- Revenue Growth Bars</div>
      <div id="hl-mover-bars"><div class="loading">Loading...</div></div>
    </div>
  </div>

  <!-- ===== PRODUCTS ===== -->
  <div id="tab-products" class="tab-panel">
    <div class="kw-filter-bar">
      <div class="kw-filter-row">
        <span class="kw-filter-label">Search</span>
        <input type="text" id="prod-search" placeholder="Filter products..." oninput="applyProductFilters()">
        <span class="kw-filter-count" id="prod-filter-count"></span>
      </div>
      <div class="kw-filter-row">
        <span class="kw-filter-label">Growth</span>
        <span id="prod-growth-chips">
          <span class="chip active" data-growth="All" onclick="setChipFilter('prod-growth-chips',this);applyProductFilters()">All</span>
          <span class="chip" data-growth="Growing" onclick="setChipFilter('prod-growth-chips',this);applyProductFilters()">Growing</span>
          <span class="chip" data-growth="Declining" onclick="setChipFilter('prod-growth-chips',this);applyProductFilters()">Declining</span>
          <span class="chip" data-growth="Flat" onclick="setChipFilter('prod-growth-chips',this);applyProductFilters()">Flat</span>
        </span>
        <span class="kw-filter-label" style="margin-left:12px">Revenue Tier</span>
        <span id="prod-tier-chips">
          <span class="chip active" data-tier="All" onclick="setChipFilter('prod-tier-chips',this);applyProductFilters()">All</span>
          <span class="chip" data-tier="top25" onclick="setChipFilter('prod-tier-chips',this);applyProductFilters()">Top 25%</span>
          <span class="chip" data-tier="mid50" onclick="setChipFilter('prod-tier-chips',this);applyProductFilters()">Mid 50%</span>
          <span class="chip" data-tier="bot25" onclick="setChipFilter('prod-tier-chips',this);applyProductFilters()">Bottom 25%</span>
        </span>
        <span class="kw-filter-label" style="margin-left:12px">Min Units</span>
        <select id="prod-units-filter" onchange="applyProductFilters()">
          <option value="0">Any</option>
        </select>
      </div>
      <div class="kw-filter-row">
        <span class="kw-filter-label">Date Range</span>
        <input type="month" id="prod-month-start" onchange="onProductDateChange()">
        <span style="color:var(--muted);font-size:0.78rem">to</span>
        <input type="month" id="prod-month-end" onchange="onProductDateChange()">
        <span class="chip" onclick="resetProductDateRange()" style="margin-left:4px">Reset</span>
      </div>
    </div>
    <div class="card" style="padding:0">
      <div class="tbl-wrap">
        <table id="products-table">
          <thead><tr>
            <th data-col="product_name">Product</th>
            <th data-col="l52_revenue" class="num sort-desc">L52 Rev</th>
            <th data-col="p52_revenue" class="num">P52 Rev</th>
            <th data-col="revenue_change_pct" class="num">Rev Chg%</th>
            <th data-col="l52_units" class="num">L52 Units</th>
            <th data-col="units_change_pct" class="num">Units Chg%</th>
            <th data-col="l52_sessions" class="num">Sessions</th>
            <th data-col="l52_impressions" class="num">Search Imp</th>
            <th data-col="l52_clicks" class="num">Search Clk</th>
            <th data-col="l52_cart_adds" class="num">Cart Adds</th>
            <th data-col="l52_search_sales" class="num">Search $</th>
            <th data-col="avg_repeat_pct" class="num">Repeat%</th>
          </tr></thead>
          <tbody id="products-tbody"><tr><td colspan="12" class="loading">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>

    <!-- Keyword detail panel (shown on product row click) -->
    <div class="allocation-wrap" id="product-kw-panel">
      <div class="allocation-title" id="product-kw-title">Keywords for --</div>
      <div class="allocation-sub">Keywords ranked by relevance score for this product</div>
      <div class="tbl-wrap">
        <table id="product-kw-table">
          <thead><tr>
            <th>Keyword</th>
            <th class="num" title="Weighted composite relevance score (0-1)">Relevance</th>
            <th>Role</th>
            <th>Type</th>
            <th class="num">CVR Index</th>
            <th class="num">Purchases</th>
            <th class="num">Revenue</th>
            <th class="num">Trend</th>
            <th>Strategy</th>
          </tr></thead>
          <tbody id="product-kw-tbody"><tr><td colspan="9" class="loading">Select a product above</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>


  <!-- ===== SEARCH TERMS ===== -->
  <div id="tab-search" class="tab-panel">
    <div class="card" style="padding:0">
      <div class="tbl-wrap">
        <table id="search-table">
          <thead><tr>
            <th data-col="search_term">Search Term</th>
            <th data-col="l52_avg_search_freq_rank" class="num">L52 SFR</th>
            <th data-col="l52_avg_click_share" class="num sort-desc">L52 CS%</th>
            <th data-col="p52_avg_click_share" class="num">P52 CS%</th>
            <th data-col="click_share_change_pp" class="num">CS Chg pp</th>
            <th data-col="l52_avg_conversion_share" class="num">L52 ConvS%</th>
            <th data-col="p52_avg_conversion_share" class="num">P52 ConvS%</th>
            <th data-col="l52_months_present" class="num">Months</th>
            <th>Trend</th>
          </tr></thead>
          <tbody id="search-tbody"><tr><td colspan="9" class="loading">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ===== ASIN EXPLORER ===== -->
  <div id="tab-explorer" class="tab-panel">
    <div class="search-wrap">
      <input id="asin-input" type="text" placeholder="Search by ASIN or product name..." autocomplete="off"/>
      <div class="autocomplete-list" id="ac-list"></div>
    </div>
    <div id="explorer-content">
      <div class="explorer-placeholder">
        <h3>ASIN Explorer</h3>
        <p>Search for a product above to see full monthly history, search funnel, repeat purchase, market basket, and top queries.</p>
      </div>
    </div>
  </div>

  <!-- ===== KEYWORDS ===== -->
  <div id="tab-keywords" class="tab-panel">

    <!-- KPI strip -->
    <div class="kw-kpi-grid" id="kw-kpi-grid"><div class="loading">Loading...</div></div>

    <!-- Scoring Weights (collapsible) -->
    <details class="weights-panel" id="weights-panel">
      <summary>Scoring Weights &amp; Thresholds</summary>
      <div id="weights-content"><div class="loading" style="padding:16px 0">Loading weights...</div></div>
    </details>

    <!-- Filter Bar -->
    <div class="kw-filter-bar">
      <!-- Row 1: text search + time horizon -->
      <div class="kw-filter-row">
        <span class="kw-filter-label">Search</span>
        <input type="text" id="kw-search" placeholder="Filter keywords..." oninput="applyFilters()">
        <span style="margin-left:auto;display:flex;align-items:center;gap:6px">
          <span class="kw-filter-label" style="min-width:auto">Time Window</span>
          <button class="kw-horizon-btn active" data-period="L1M"  onclick="setHorizon(this)">L1M</button>
          <button class="kw-horizon-btn" data-period="L3M"  onclick="setHorizon(this)">L3M</button>
          <button class="kw-horizon-btn" data-period="L6M"  onclick="setHorizon(this)">L6M</button>
          <button class="kw-horizon-btn" data-period="L12M" onclick="setHorizon(this)">L12M</button>
          <span class="kw-horizon-period" id="kw-period-label"></span>
          <span style="width:1px;height:16px;background:var(--border);margin:0 4px"></span>
          <span class="kw-filter-label" style="min-width:auto">Trend vs</span>
          <button class="kw-horizon-btn active" data-trend="yoy"    onclick="setTrendMode(this)">Y/Y</button>
          <button class="kw-horizon-btn"        data-trend="period" onclick="setTrendMode(this)">Prior</button>
        </span>
      </div>
      <!-- Row 2: keyword type + volume tier -->
      <div class="kw-filter-row">
        <span class="kw-filter-label">Type</span>
        <span id="kw-type-chips" style="display:contents">
        <span class="chip active" data-kwtype="All"        onclick="setChipFilter('kw-type-chips',this);applyFilters()">All</span>
        <span class="chip"        data-kwtype="branded"    onclick="setChipFilter('kw-type-chips',this);applyFilters()">Branded</span>
        <span class="chip"        data-kwtype="competitor" onclick="setChipFilter('kw-type-chips',this);applyFilters()">Competitor</span>
        <span class="chip"        data-kwtype="category"   onclick="setChipFilter('kw-type-chips',this);applyFilters()">Category</span>
        </span>
        <span style="width:1px;height:16px;background:var(--border);margin:0 6px"></span>
        <span class="kw-filter-label" style="min-width:auto">Tier</span>
        <span id="kw-tier-chips" style="display:contents">
        <span class="chip active" data-kwtier="All"       onclick="setChipFilter('kw-tier-chips',this);applyFilters()">All</span>
        <span class="chip"        data-kwtier="mega"      onclick="setChipFilter('kw-tier-chips',this);applyFilters()">Mega</span>
        <span class="chip"        data-kwtier="head"      onclick="setChipFilter('kw-tier-chips',this);applyFilters()">Head</span>
        <span class="chip"        data-kwtier="mid"       onclick="setChipFilter('kw-tier-chips',this);applyFilters()">Mid</span>
        <span class="chip"        data-kwtier="long-tail" onclick="setChipFilter('kw-tier-chips',this);applyFilters()">Long-Tail</span>
        </span>
      </div>
      <!-- Row 3: CVR filter + min months + cannibalization + result count -->
      <div class="kw-filter-row">
        <span class="kw-filter-label">CVR Index</span>
        <select id="kw-cvr-filter" onchange="applyFilters()">
          <option value="all">All</option>
          <option value="over">Outperforming (&gt;1x)</option>
          <option value="under">Underperforming (&lt;1x)</option>
        </select>
        <span style="width:1px;height:16px;background:var(--border);margin:0 6px"></span>
        <span class="kw-filter-label" style="min-width:auto">Min Clicks</span>
        <select id="kw-clicks-filter" onchange="applyFilters()">
          <option value="0">Any</option>
          <option value="10">10+ clicks</option>
          <option value="25">25+ clicks</option>
          <option value="50">50+ clicks</option>
          <option value="100">100+ clicks</option>
          <option value="250">250+ clicks</option>
        </select>
        <label class="kw-cannibal-toggle" id="kw-cannibal-label">
          <input type="checkbox" id="kw-cannibal-filter" onchange="applyFilters()">
          Cannibalization risk only
        </label>
        <span class="kw-filter-count" id="kw-filter-count"></span>
      </div>
    </div>

    <!-- Strategy Board -->
    <div class="card">
      <div class="card-title">Strategy Board</div>
      <div class="chip-row" id="strategy-chips">
        <span class="chip active" data-strategy="All" onclick="filterKeywords(this)">All</span>
        <span class="chip chip-branded" data-strategy="Branded" onclick="filterKeywords(this)">Branded</span>
        <span class="chip chip-defend"  data-strategy="Defend"  onclick="filterKeywords(this)">Defend</span>
        <span class="chip chip-grow"    data-strategy="Grow"    onclick="filterKeywords(this)">Grow</span>
        <span class="chip chip-watch"   data-strategy="Watch"   onclick="filterKeywords(this)">Watch</span>
        <span class="chip chip-deprioritize" data-strategy="Deprioritize" onclick="filterKeywords(this)">Deprioritize</span>
      </div>
      <div class="tbl-wrap">
        <table id="kw-table">
          <thead><tr>
            <th data-kwcol="search_query">Keyword</th>
            <th data-kwcol="volume" class="num">Volume</th>
            <th data-kwcol="vol_tier">Tier</th>
            <th data-kwcol="strategy">Strategy</th>
            <th data-kwcol="asin_count" class="num" title="Brand ASINs appearing on this keyword">ASINs</th>
            <th data-kwcol="brand_click_share" class="num">Click Share</th>
            <th data-kwcol="brand_purchase_share" class="num">Purch. Share</th>
            <th data-kwcol="cvr_index" class="num">CVR Index</th>
            <th data-kwcol="ctr_index" class="num">CTR Index</th>
            <th data-kwcol="brand_cvr" class="num">Brand CVR</th>
            <th data-kwcol="mkt_cvr" class="num">Mkt CVR</th>
            <th data-kwcol="share_trend" class="num">Share</th>
            <th data-kwcol="volume_trend" class="num">Vol. Trend</th>
            <th data-kwcol="hero_asin">Hero ASIN</th>
            <th data-kwcol="brand_clicks" class="num sort-desc">Clicks</th>
          </tr></thead>
          <tbody id="kw-tbody"><tr><td colspan="15" class="loading">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>

    <!-- Share Tracker (shown on row click) -->
    <div class="tracker-wrap" id="share-tracker">
      <div class="tracker-title" id="tracker-title">--</div>
      <div class="tracker-sub" id="tracker-sub"></div>
      <div class="tracker-chart-wrap"><canvas id="chart-share-tracker"></canvas></div>
    </div>

    <!-- ASIN Allocation panel (shown on row click) -->
    <div class="allocation-wrap" id="asin-allocation">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
        <div class="allocation-title" id="allocation-title">ASIN Allocation</div>
        <span id="cannibal-badge" class="cannibal-warn" style="display:none">Cannibalization Risk</span>
      </div>
      <div class="allocation-sub" id="allocation-sub">Brand ASINs ranked by priority score for this keyword</div>
      <div class="tbl-wrap">
        <table id="allocation-table">
          <thead><tr>
            <th>ASIN</th>
            <th>Product</th>
            <th class="num" title="Weighted composite score (higher = this ASIN should own this keyword)">Priority Score</th>
            <th class="num" title="Within-keyword dominance percentile">Dominance %ile</th>
            <th class="num">CVR Index</th>
            <th class="num">ASIN CVR</th>
            <th class="num">AOV</th>
            <th class="num">Revenue</th>
            <th class="num">Purch.</th>
            <th>Role</th>
          </tr></thead>
          <tbody id="allocation-tbody"><tr><td colspan="10" class="loading">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>

    <!-- Bottom two panels -->
    <div class="two-col">
      <!-- CVR Index Leaders -->
      <div class="card">
        <div class="card-title">CVR Index Leaders -- Top Grow Opportunities</div>
        <div class="tbl-wrap">
          <table id="cvr-leaders-table">
            <thead><tr>
              <th>Keyword</th>
              <th class="num">Volume</th>
              <th class="num">CVR Index</th>
              <th class="num">Brand CVR</th>
              <th class="num">Mkt CVR</th>
              <th class="num">Click Share</th>
              <th class="num">Brand Purch.</th>
            </tr></thead>
            <tbody id="cvr-leaders-tbody"></tbody>
          </table>
        </div>
      </div>

      <!-- Long-Tail Harvest -->
      <div class="card">
        <div class="card-title">Long-Tail Harvest -- High CVR, Low Volume</div>
        <div class="tbl-wrap">
          <table id="longtail-table">
            <thead><tr>
              <th>Keyword</th>
              <th class="num">Volume</th>
              <th class="num">Brand CVR</th>
              <th class="num">Brand Purch.</th>
              <th class="num">Purch. Share</th>
              <th class="num">CVR Index</th>
            </tr></thead>
            <tbody id="longtail-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>

  </div>

  <!-- ===== ADVERTISING ===== -->
  <div id="tab-advertising" class="tab-panel">

    <!-- KPI cards -->
    <div class="kw-kpi-grid" id="ads-kpi-grid"><div class="loading">Loading...</div></div>

    <!-- Shared filters -->
    <div class="kw-filter-bar">
      <div class="kw-filter-row">
        <span class="kw-filter-label">Search</span>
        <input type="text" id="ads-kw-search" placeholder="Filter keywords..." oninput="filterAdsKeywords()">
      </div>
      <div class="kw-filter-row">
        <span class="kw-filter-label">Strategy</span>
        <span id="ads-strategy-chips" style="display:contents">
        <span class="chip active" data-adstrat="All"           onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">All</span>
        <span class="chip chip-branded" data-adstrat="Branded" onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">Branded</span>
        <span class="chip chip-defend"  data-adstrat="Defend"  onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">Defend</span>
        <span class="chip chip-grow"    data-adstrat="Grow"    onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">Grow</span>
        <span class="chip chip-watch"   data-adstrat="Watch"   onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">Watch</span>
        <span class="chip chip-deprioritize" data-adstrat="Deprioritize" onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">Deprioritize</span>
        <span class="chip" data-adstrat="" onclick="setChipFilter('ads-strategy-chips',this);filterAdsKeywords()">No Organic</span>
        </span>
        <span style="width:1px;height:16px;background:var(--border);margin:0 6px"></span>
        <span class="kw-filter-label" style="min-width:auto">Ad Type</span>
        <span id="ads-adtype-chips" style="display:contents">
        <span class="chip active" data-adtype="All" onclick="setChipFilter('ads-adtype-chips',this);filterAdsKeywords()">All</span>
        <span class="chip" data-adtype="SP" onclick="setChipFilter('ads-adtype-chips',this);filterAdsKeywords()">SP</span>
        <span class="chip" data-adtype="SB" onclick="setChipFilter('ads-adtype-chips',this);filterAdsKeywords()">SB</span>
        <span class="chip" data-adtype="SD" onclick="setChipFilter('ads-adtype-chips',this);filterAdsKeywords()">SD</span>
        </span>
      </div>
    </div>

    <!-- Two-panel filters: Ad Data (left) | SQP / Organic (right) -->
    <div class="two-col" style="margin-bottom:16px">
      <div class="kw-filter-bar" style="margin-bottom:0;position:relative">
        <div style="font-size:0.72rem;font-weight:700;color:var(--blue);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px">Ad Data</div>
        <div class="kw-filter-row" style="opacity:0.4;pointer-events:none">
          <span class="kw-filter-label">Window</span>
          <button class="kw-horizon-btn active" data-adwindow="L1M">L1M</button>
          <button class="kw-horizon-btn" data-adwindow="L3M">L3M</button>
          <button class="kw-horizon-btn" data-adwindow="L6M">L6M</button>
          <button class="kw-horizon-btn" data-adwindow="ALL">All</button>
          <span class="kw-horizon-period" id="ads-period-label"></span>
        </div>
        <div style="font-size:0.7rem;color:var(--yellow);font-style:italic;margin:-2px 0 4px">
          &#9888; Only Feb 2026 available &mdash; more months coming soon
        </div>
        <div class="kw-filter-row">
          <span class="kw-filter-label">Min Clicks</span>
          <select id="ads-clicks-filter" onchange="filterAdsKeywords()">
            <option value="0">Any</option>
            <option value="10">10+ clicks</option>
            <option value="25">25+ clicks</option>
            <option value="50">50+ clicks</option>
            <option value="100">100+ clicks</option>
          </select>
          <span class="kw-filter-count" id="ads-filter-count"></span>
        </div>
      </div>
      <div class="kw-filter-bar" style="margin-bottom:0">
        <div style="font-size:0.72rem;font-weight:700;color:var(--cyan);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px">SQP / Organic</div>
        <div class="kw-filter-row">
          <span class="kw-filter-label">Window</span>
          <button class="kw-horizon-btn active" data-sqpwindow="L1M"  onclick="setSqpHorizon(this)">L1M</button>
          <button class="kw-horizon-btn" data-sqpwindow="L3M"  onclick="setSqpHorizon(this)">L3M</button>
          <button class="kw-horizon-btn" data-sqpwindow="L6M"  onclick="setSqpHorizon(this)">L6M</button>
          <button class="kw-horizon-btn" data-sqpwindow="ALL"  onclick="setSqpHorizon(this)">All</button>
          <span class="kw-horizon-period" id="sqp-period-label"></span>
        </div>
        <div class="kw-filter-row">
          <span class="kw-filter-label">Min Clicks</span>
          <select id="sqp-clicks-filter" onchange="filterAdsKeywords()">
            <option value="0">Any</option>
            <option value="10">10+ clicks</option>
            <option value="25">25+ clicks</option>
            <option value="50">50+ clicks</option>
            <option value="100">100+ clicks</option>
            <option value="250">250+ clicks</option>
          </select>
        </div>
      </div>
    </div>
    <p style="color:var(--dim);font-size:0.7rem;margin:0 0 8px">
      SP uses 7-day attribution; SB/SD use 14-day. Click a keyword to drill down.
    </p>

    <!-- Level 1: Keyword table -->
    <div class="card">
      <div class="card-title">Keyword Performance</div>
      <div class="tbl-wrap">
        <table id="ads-kw-table">
          <thead><tr>
            <th data-adcol="search_term">Search Term</th>
            <th data-adcol="ad_type_list">Ad Type</th>
            <th data-adcol="ad_spend" class="num">Ad Spend</th>
            <th data-adcol="ad_clicks" class="num">Ad Clicks</th>
            <th data-adcol="ad_sales" class="num sort-desc">Ad Sales</th>
            <th data-adcol="ad_acos" class="num">ACOS</th>
            <th data-adcol="ad_roas" class="num">ROAS</th>
            <th data-adcol="num_campaigns" class="num">Campaigns</th>
            <th data-adcol="best_impression_share" class="num">Imp Share</th>
            <th data-adcol="strategy">Strategy</th>
            <th data-adcol="organic_volume" class="num">Org Volume</th>
            <th data-adcol="organic_units" class="num">Org Units</th>
            <th data-adcol="cvr_index" class="num">CVR Index</th>
          </tr></thead>
          <tbody id="ads-kw-tbody"></tbody>
        </table>
      </div>
    </div>

    <!-- Level 2: Product drill-down (shown on keyword click) -->
    <div class="tracker-wrap" id="ads-product-detail">
      <div class="tracker-title" id="ads-product-header"></div>
      <div class="tbl-wrap">
        <table id="ads-product-table">
          <thead><tr>
            <th>ASIN</th>
            <th>Product</th>
            <th class="num">Ad Spend</th>
            <th class="num">Ad Sales</th>
            <th class="num">ACOS</th>
            <th class="num">Campaigns</th>
            <th>Role</th>
            <th class="num">Org Share</th>
            <th class="num">CVR Index</th>
          </tr></thead>
          <tbody id="ads-product-tbody"></tbody>
        </table>
      </div>
    </div>

    <!-- Level 3: Campaign drill-down (shown on product click) -->
    <div class="allocation-wrap" id="ads-campaign-detail">
      <div class="allocation-title" id="ads-campaign-header"></div>
      <div class="tbl-wrap">
        <table id="ads-campaign-table">
          <thead><tr>
            <th>Campaign</th>
            <th>Type</th>
            <th>Match</th>
            <th class="num">Spend</th>
            <th class="num">CPC</th>
            <th class="num">Clicks</th>
            <th class="num">Orders</th>
            <th class="num">ACOS</th>
            <th class="num">Imp Share</th>
          </tr></thead>
          <tbody id="ads-campaign-tbody"></tbody>
        </table>
      </div>
    </div>

  </div>

</div>

<script>
const TABS = ['overview','highlights','products','search','explorer','keywords','advertising'];
const loaded = {};
function showTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  document.querySelectorAll('nav button')[TABS.indexOf(name)].classList.add('active');
  if (!loaded[name]) { loaded[name] = true; initTab(name); }
}

function initTab(name) {
  if (name === 'overview')    loadOverview();
  if (name === 'highlights')  loadHighlights();
  if (name === 'products')    loadProducts();

  if (name === 'search')      loadSearch();
  if (name === 'explorer')    initExplorer();
  if (name === 'keywords')    loadKeywords();
  if (name === 'advertising') loadAdvertising();
}

const fmt$ = v => '$' + (+v).toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:0});
const fmtN = v => (+v).toLocaleString('en-US', {maximumFractionDigits:0});
const fmtP = v => (+v).toFixed(1) + '%';
const fmtPP= v => (v >= 0 ? '+' : '') + (+v).toFixed(1) + 'pp';
const fmtD = v => (v >= 0 ? '+' : '') + (+v).toFixed(1) + '%';

function changeClass(v) {
  if (v > 0.05) return 'cell-up'; if (v < -0.05) return 'cell-down'; return 'cell-neutral';
}

Chart.defaults.color = '#8b949e';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
Chart.defaults.font.size = 11;

const MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

// ----------- HIGHLIGHTS -----------
let hlMoversChart = null, hlLoyaltyChart = null;

async function loadHighlights() {
  const [hl, movers] = await Promise.all([
    fetch('/api/highlights').then(r => r.json()),
    fetch('/api/highlights-movers').then(r => r.json()),
  ]);

  // Period note
  const { l52_days, p52_days, l52_range, p52_range } = hl.periods;
  document.getElementById('hl-period-note').textContent =
    `L52: ${l52_range} (${l52_days} days)  |  P52: ${p52_range} (${p52_days} days)  |  Daily averages used to normalise period lengths`;

  // Hero KPI cards
  const heroGrid = document.getElementById('hl-hero-grid');
  heroGrid.innerHTML = hl.kpis.map(k => {
    const up = k.positive;
    const cls = up ? 'up' : 'down';
    const arrow = up ? '\u25b2' : '\u25bc';
    const chg = k.change_pct != null
      ? `${arrow} ${Math.abs(k.change_pct).toFixed(1)}%`
      : '--';
    return `<div class="hl-hero-card ${cls}">
      <div class="hl-hero-label">${k.label}</div>
      <div class="hl-hero-val">${k.l52_fmt}</div>
      <div class="hl-hero-prior">vs Prior: ${k.p52_fmt}</div>
      <span class="hl-hero-badge ${cls}">${chg}</span>
    </div>`;
  }).join('');

  // Search visibility stats (monthly averages)
  const sv = hl.search_visibility;
  const svRows = [
    { label: 'Avg Impressions / mo', l52: sv.l52_impressions, p52: sv.p52_impressions, pct: sv.impressions_change_pct, fmt: fmtN },
    { label: 'Avg Clicks / mo',      l52: sv.l52_clicks,      p52: sv.p52_clicks,      pct: sv.clicks_change_pct,      fmt: fmtN },
    { label: 'Avg Purchases / mo',   l52: sv.l52_purchases,   p52: sv.p52_purchases,   pct: sv.purchases_change_pct,   fmt: fmtN },
    { label: 'Avg Cart Adds / mo',   l52: sv.l52_cart_adds,   p52: sv.p52_cart_adds,   pct: sv.cart_adds_change_pct,   fmt: fmtN },
  ];
  document.getElementById('hl-search-stats').innerHTML =
    `<div style="font-size:0.68rem;color:var(--muted);margin-bottom:10px;padding:5px 8px;background:var(--surface2);border-radius:4px">
       Monthly averages -- L52: ${sv.l52_months} months | P52: ${sv.p52_months} months
     </div>` +
    svRows.map(r => {
      const up = r.pct >= 0;
      const pillCls = up ? 'up' : 'down';
      const chgStr = r.pct != null ? `${up ? '+' : ''}${r.pct.toFixed(1)}%` : '--';
      return `<div class="hl-stat-row">
        <span class="hl-stat-label">${r.label}</span>
        <div class="hl-stat-vals">
          <span class="hl-stat-p52">P52: ${r.fmt(r.p52)}</span>
          <span class="hl-stat-l52">${r.fmt(r.l52)}</span>
          <span class="hl-stat-pill ${pillCls}">${chgStr}</span>
        </div>
      </div>`;
    }).join('');

  // Loyalty stats
  const lo = hl.loyalty;
  const loRows = [
    { label: 'Repeat Revenue',       l52: lo.l52_repeat_revenue,   p52: lo.p52_repeat_revenue,   pct: lo.repeat_revenue_change_pct,    fmt: fmt$ },
    { label: 'Repeat Revenue Share', l52: lo.l52_repeat_rev_pct,   p52: lo.p52_repeat_rev_pct,   pct: null, fmt: v => fmtP(v), suffix: '%' },
    { label: 'Repeat Customer Rate', l52: lo.l52_repeat_cust_pct,  p52: lo.p52_repeat_cust_pct,  pct: null, fmt: v => fmtP(v), suffix: '%' },
    { label: 'Unique Customers',     l52: lo.l52_unique_customers,  p52: lo.p52_unique_customers,  pct: lo.l52_unique_customers && lo.p52_unique_customers ? ((lo.l52_unique_customers - lo.p52_unique_customers) / lo.p52_unique_customers * 100) : null, fmt: fmtN },
  ];
  document.getElementById('hl-loyalty-stats').innerHTML = loRows.map(r => {
    const up = r.pct != null ? r.pct >= 0 : (r.l52 >= r.p52);
    const pillCls = up ? 'up' : 'down';
    const chgStr = r.pct != null ? `${r.pct >= 0 ? '+' : ''}${r.pct.toFixed(1)}%` : (r.l52 >= r.p52 ? '\u25b2' : '\u25bc');
    return `<div class="hl-stat-row">
      <span class="hl-stat-label">${r.label}</span>
      <div class="hl-stat-vals">
        <span class="hl-stat-p52">P52: ${r.fmt(r.p52)}</span>
        <span class="hl-stat-l52">${r.fmt(r.l52)}</span>
        <span class="hl-stat-pill ${pillCls}">${chgStr}</span>
      </div>
    </div>`;
  }).join('');

  // Movers horizontal bar chart
  if (hlMoversChart) { hlMoversChart.destroy(); hlMoversChart = null; }
  const moverLabels = movers.map(m => {
    const name = m.product_name.length > 36 ? m.product_name.slice(0, 34) + '...' : m.product_name;
    return name;
  });
  const ctx1 = document.getElementById('chart-hl-movers').getContext('2d');
  hlMoversChart = new Chart(ctx1, {
    type: 'bar',
    data: {
      labels: moverLabels,
      datasets: [
        {
          label: 'L52 Revenue',
          data: movers.map(m => m.l52_rev),
          backgroundColor: 'rgba(88,166,255,0.75)',
          borderColor: '#58a6ff',
          borderWidth: 1,
          borderRadius: 4,
        },
        {
          label: 'P52 Revenue',
          data: movers.map(m => m.p52_rev),
          backgroundColor: 'rgba(188,140,255,0.45)',
          borderColor: '#bc8cff',
          borderWidth: 1,
          borderRadius: 4,
        },
      ]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmt$(ctx.raw)}`,
          }
        }
      },
      scales: {
        x: {
          grid: { color: '#2d3148' },
          ticks: {
            callback: v => '$' + (v >= 1000000 ? (v/1000000).toFixed(1)+'M' : (v/1000).toFixed(0)+'K'),
          }
        },
        y: { grid: { display: false } }
      }
    }
  });

  // Detail bars for movers
  const maxRev = Math.max(...movers.map(m => m.l52_rev));
  document.getElementById('hl-mover-bars').innerHTML = movers.map(m => {
    const l52Pct = (m.l52_rev / maxRev * 100).toFixed(1);
    const p52Pct = (m.p52_rev / maxRev * 100).toFixed(1);
    const name = m.product_name.length > 60 ? m.product_name.slice(0, 58) + '...' : m.product_name;
    return `<div class="hl-mover-bar-wrap">
      <div class="hl-mover-label">
        <span>${name} <span style="color:var(--muted);font-weight:400;font-size:0.68rem">${m.asin}</span></span>
        <span>+${fmt$(m.rev_growth)} (+${m.pct_growth.toFixed(1)}%)</span>
      </div>
      <div class="hl-bar-track" style="height:34px">
        <div class="hl-bar-l52" style="width:${l52Pct}%">${fmt$(m.l52_rev)}</div>
        <div class="hl-bar-p52-marker" style="left:${p52Pct}%;top:4px;bottom:4px" title="P52: ${fmt$(m.p52_rev)}"></div>
      </div>
      <div style="font-size:0.7rem;color:var(--muted);margin-top:3px">Purple line = P52 level (${fmt$(m.p52_rev)})</div>
    </div>`;
  }).join('');

  // Loyalty comparison bar chart (repeat rev vs total rev, L52 vs P52)
  if (hlLoyaltyChart) { hlLoyaltyChart.destroy(); hlLoyaltyChart = null; }
  const ctx2 = document.getElementById('chart-hl-loyalty').getContext('2d');
  const totalRevL52 = hl.kpis.find(k => k.id === 'total_revenue').l52;
  const totalRevP52 = hl.kpis.find(k => k.id === 'total_revenue').p52;
  hlLoyaltyChart = new Chart(ctx2, {
    type: 'bar',
    data: {
      labels: ['P52 (Prior Year)', 'L52 (This Year)'],
      datasets: [
        {
          label: 'Repeat Revenue',
          data: [lo.p52_repeat_revenue, lo.l52_repeat_revenue],
          backgroundColor: ['rgba(63,185,80,0.45)', 'rgba(63,185,80,0.8)'],
          borderColor: ['#3fb950', '#3fb950'],
          borderWidth: 1,
          borderRadius: 4,
          stack: 'rev',
        },
        {
          label: 'Non-Repeat Revenue',
          data: [totalRevP52 - lo.p52_repeat_revenue, totalRevL52 - lo.l52_repeat_revenue],
          backgroundColor: ['rgba(88,166,255,0.35)', 'rgba(88,166,255,0.65)'],
          borderColor: ['#58a6ff', '#58a6ff'],
          borderWidth: 1,
          borderRadius: 4,
          stack: 'rev',
        },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmt$(ctx.raw)}`,
          }
        }
      },
      scales: {
        x: { stacked: true, grid: { display: false } },
        y: {
          stacked: true,
          grid: { color: '#2d3148' },
          ticks: {
            callback: v => '$' + (v >= 1000000 ? (v/1000000).toFixed(1)+'M' : (v/1000).toFixed(0)+'K'),
          }
        }
      }
    }
  });
}

// ----------- OVERVIEW -----------
let chartRevenue = null, chartUnits = null;

async function loadOverview() {
  const [summary, trends, movers, periodMeta] = await Promise.all([
    fetch('/api/summary').then(r=>r.json()),
    fetch('/api/trends').then(r=>r.json()),
    fetch('/api/movers').then(r=>r.json()),
    fetch('/api/period-meta').then(r=>r.json()),
  ]);
  const l52Year = 'This Year';
  const p52Year = 'Prior Year';
  const yoyLabel = 'Year over Year';
  document.getElementById('tagline').textContent = `${yoyLabel} | Full Brand Analytics Suite`;
  document.getElementById('title-revenue').textContent = `Monthly Revenue -- ${yoyLabel}`;
  document.getElementById('title-units').textContent = `Monthly Units -- ${yoyLabel}`;
  document.getElementById('title-movers').textContent = `Top 5 Revenue Breakouts -- ${yoyLabel}`;
  renderKPIs(summary);
  renderTrendChart(trends, periodMeta);
  renderMovers(movers);
}

function renderKPIs(data) {
  const grid = document.getElementById('kpi-grid');
  grid.innerHTML = data.map(m => {
    const up = m.change >= 0;
    const arrow = up ? '\u25b2' : '\u25bc';
    const cls = up ? 'up' : 'down';
    const changeStr = m.kind === 'pp'
      ? `${arrow} ${Math.abs(m.change).toFixed(1)}pp`
      : `${arrow} ${Math.abs(m.change).toFixed(1)}%`;
    return `<div class="kpi-card">
      <div class="kpi-label">${m.label}</div>
      <div class="kpi-l52">${m.l52_fmt}</div>
      <div class="kpi-p52">Prior: ${m.p52_fmt}</div>
      <div class="kpi-change ${cls}">${changeStr}</div>
    </div>`;
  }).join('');
}

function renderTrendChart(rows, periodMeta) {
  const l52 = rows.filter(r=>r.period==='L52').sort((a,b)=>a.month.localeCompare(b.month));
  const p52 = rows.filter(r=>r.period==='P52').sort((a,b)=>a.month.localeCompare(b.month));
  const labels = l52.map(r => MONTH_LABELS[parseInt(r.month.split('-')[1])-1]);
  const p52Map = Object.fromEntries(p52.map(r=>[r.calendar_month, r]));
  const p52Aligned = l52.map(r => p52Map[r.calendar_month] || null);

  const cfg = (id, field) => ({
    type:'line',
    data:{
      labels,
      datasets:[
        {label:periodMeta.L52?.label||'This Year',data:l52.map(r=>r[field]),borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,.1)',borderWidth:2,tension:.3,fill:true,pointRadius:4,pointHoverRadius:6},
        {label:periodMeta.P52?.label||'Prior Year',data:p52Aligned.map(r=>r?r[field]:null),borderColor:'#bc8cff',backgroundColor:'rgba(188,140,255,.06)',borderWidth:2,tension:.3,fill:true,pointRadius:4,pointHoverRadius:6,borderDash:[5,4],spanGaps:true},
      ]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'top',labels:{boxWidth:12,usePointStyle:true}},tooltip:{mode:'index',intersect:false,callbacks:{
        label:c=>`${c.dataset.label}: ${field==='revenue'?fmt$(c.raw):fmtN(c.raw)}`,
        afterBody(items){
          if(items.length<2) return '';
          const cur=items[0].raw, prior=items[1].raw;
          if(!prior) return '';
          const pct=((cur-prior)/prior*100).toFixed(1);
          const sign=pct>=0?'+':'';
          return `\nYoY: ${sign}${pct}%`;
        }
      }}},
      scales:{y:{grid:{color:'#1e2533'},ticks:{callback:v=>field==='revenue'?'$'+Math.round(v/1000)+'k':fmtN(v)}}}
    }
  });

  if (chartRevenue) chartRevenue.destroy();
  if (chartUnits) chartUnits.destroy();
  chartRevenue = new Chart(document.getElementById('chart-revenue').getContext('2d'), cfg('r','revenue'));
  chartUnits = new Chart(document.getElementById('chart-units').getContext('2d'), cfg('u','units'));
}

function renderMovers(data) {
  const makeTable = (items, isGainer) => `
    <div class="tbl-wrap"><table><thead><tr>
      <th>Product</th><th class="num">L52 Units</th><th class="num">Units Chg</th><th class="num">Rev Chg</th>
    </tr></thead><tbody>
    ${items.map(r=>`<tr>
      <td><div class="product-name" title="${r.product_name}">${r.product_name||r.asin}</div>
          <span class="asin-link" onclick="openAsin('${r.asin}')">${r.asin}</span></td>
      <td class="num">${fmtN(r.l52_units)}</td>
      <td class="num ${isGainer?'cell-up':'cell-down'}">${isGainer?'+':''}${fmtN(r.units_change)}</td>
      <td class="num ${isGainer?'cell-up':'cell-down'}">${isGainer?'+':''}${fmt$(r.revenue_change)}</td>
    </tr>`).join('')}
    </tbody></table></div>`;
  document.getElementById('gainers-table').innerHTML = makeTable(data.gainers, true);
  document.getElementById('decliners-table').innerHTML = makeTable(data.decliners, false);
}

// ----------- PRODUCTS -----------
let productsData = [];
let productsSortCol = 'l52_revenue', productsSortDir = -1;
let prodRevP75 = 0, prodRevP25 = 0;
let prodUnitsP25 = 0, prodUnitsP50 = 0, prodUnitsP75 = 0;
let prodPeriodMeta = null;

function percentile(vals, p) {
  const s = [...vals].sort((a,b) => a - b);
  const i = Math.ceil(s.length * p) - 1;
  return s[Math.max(0, i)] || 0;
}

async function initProductDatePickers() {
  if (prodPeriodMeta) return;
  prodPeriodMeta = await fetch('/api/period-meta').then(r=>r.json());
  const startEl = document.getElementById('prod-month-start');
  const endEl = document.getElementById('prod-month-end');
  const l52 = prodPeriodMeta.L52;
  const minMonth = l52.start_date.slice(0,7);
  const maxMonth = l52.end_date.slice(0,7);
  startEl.min = minMonth; startEl.max = maxMonth; startEl.value = minMonth;
  endEl.min = minMonth; endEl.max = maxMonth; endEl.value = maxMonth;
}

function onProductDateChange() {
  const startEl = document.getElementById('prod-month-start');
  const endEl = document.getElementById('prod-month-end');
  if (startEl.value > endEl.value) endEl.value = startEl.value;
  loadProducts();
}

function resetProductDateRange() {
  if (!prodPeriodMeta) return;
  const l52 = prodPeriodMeta.L52;
  document.getElementById('prod-month-start').value = l52.start_date.slice(0,7);
  document.getElementById('prod-month-end').value = l52.end_date.slice(0,7);
  loadProducts();
}

async function loadProducts() {
  await initProductDatePickers();
  const ms = document.getElementById('prod-month-start').value;
  const me = document.getElementById('prod-month-end').value;
  const params = new URLSearchParams();
  if (ms) params.set('month_start', ms);
  if (me) params.set('month_end', me);
  productsData = await fetch('/api/products?' + params.toString()).then(r=>r.json());

  const revs = productsData.map(r => +r.l52_revenue || 0);
  const units = productsData.map(r => +r.l52_units || 0);
  prodRevP75 = percentile(revs, 0.75);
  prodRevP25 = percentile(revs, 0.25);
  prodUnitsP25 = percentile(units, 0.25);
  prodUnitsP50 = percentile(units, 0.50);
  prodUnitsP75 = percentile(units, 0.75);

  const sel = document.getElementById('prod-units-filter');
  let unitOpts = '<option value="0">Any</option>';
  if (prodUnitsP25 > 0) unitOpts += `<option value="${prodUnitsP25}">\u2265 ${fmtN(prodUnitsP25)} (p25)</option>`;
  if (prodUnitsP50 > 0) unitOpts += `<option value="${prodUnitsP50}">\u2265 ${fmtN(prodUnitsP50)} (p50)</option>`;
  if (prodUnitsP75 > 0) unitOpts += `<option value="${prodUnitsP75}">\u2265 ${fmtN(prodUnitsP75)} (p75)</option>`;
  sel.innerHTML = unitOpts;

  applyProductFilters();

  document.querySelectorAll('#products-table th').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (productsSortCol === col) productsSortDir *= -1;
      else { productsSortCol = col; productsSortDir = -1; }
      document.querySelectorAll('#products-table th').forEach(t => t.classList.remove('sort-asc','sort-desc'));
      th.classList.add(productsSortDir === 1 ? 'sort-asc' : 'sort-desc');
      applyProductFilters();
    });
  });
}

function applyProductFilters() {
  const searchVal = (document.getElementById('prod-search')?.value || '').toLowerCase().trim();
  const growthActive = document.querySelector('#prod-growth-chips .chip.active')?.dataset.growth || 'All';
  const tierActive = document.querySelector('#prod-tier-chips .chip.active')?.dataset.tier || 'All';
  const minUnits = parseInt(document.getElementById('prod-units-filter')?.value || '0', 10);

  let filtered = productsData.filter(r => {
    if (searchVal && !(r.product_name || '').toLowerCase().includes(searchVal)) return false;
    if (growthActive !== 'All') {
      const pct = +r.revenue_change_pct || 0;
      if (growthActive === 'Growing' && pct <= 5) return false;
      if (growthActive === 'Declining' && pct >= -5) return false;
      if (growthActive === 'Flat' && (pct > 5 || pct < -5)) return false;
    }
    if (tierActive !== 'All') {
      const rev = +r.l52_revenue || 0;
      if (tierActive === 'top25' && rev < prodRevP75) return false;
      if (tierActive === 'mid50' && (rev >= prodRevP75 || rev < prodRevP25)) return false;
      if (tierActive === 'bot25' && rev >= prodRevP25) return false;
    }
    if (minUnits > 0 && (+r.l52_units || 0) < minUnits) return false;
    return true;
  });

  const countEl = document.getElementById('prod-filter-count');
  if (countEl) countEl.textContent = `${filtered.length.toLocaleString()} product${filtered.length !== 1 ? 's' : ''}`;

  renderProducts(filtered);
}

function renderProducts(data) {
  const src = data || productsData;
  const sorted = [...src].sort((a,b) => {
    const va = a[productsSortCol], vb = b[productsSortCol];
    if (typeof va === 'string') return productsSortDir * va.localeCompare(vb);
    return productsSortDir * ((+va||0) - (+vb||0));
  });
  document.getElementById('products-tbody').innerHTML = sorted.map(r => `
    <tr data-asin="${r.asin}" style="cursor:pointer" onclick="selectProductRow('${r.asin}','${(r.product_name||'').replace(/'/g,"\\'")}')">
      <td><div class="product-name" title="${r.product_name}">${r.product_name||'--'}</div>
          <span class="asin-link" onclick="event.stopPropagation();openAsin('${r.asin}')">${r.asin}</span></td>
      <td class="num">${fmt$(r.l52_revenue)}</td>
      <td class="num">${fmt$(r.p52_revenue)}</td>
      <td class="num ${changeClass(r.revenue_change_pct)}">${fmtD(r.revenue_change_pct)}</td>
      <td class="num">${fmtN(r.l52_units)}</td>
      <td class="num ${changeClass(r.units_change_pct)}">${fmtD(r.units_change_pct)}</td>
      <td class="num">${fmtN(r.l52_sessions)}</td>
      <td class="num">${fmtN(r.l52_impressions)}</td>
      <td class="num">${fmtN(r.l52_clicks)}</td>
      <td class="num">${fmtN(r.l52_cart_adds)}</td>
      <td class="num">${fmt$(r.l52_search_sales)}</td>
      <td class="num">${r.avg_repeat_pct > 0 ? r.avg_repeat_pct.toFixed(1) + '%' : '--'}</td>
    </tr>`).join('');
}

let selectedProductAsin = null;

async function selectProductRow(asin, productName) {
  document.querySelectorAll('#products-tbody tr').forEach(tr => tr.classList.remove('row-active'));
  const row = document.querySelector('#products-tbody tr[data-asin="' + asin + '"]');
  if (row) row.classList.add('row-active');
  selectedProductAsin = asin;

  const kwPanel = document.getElementById('product-kw-panel');
  kwPanel.classList.add('visible');

  document.getElementById('product-kw-title').textContent = 'Keywords for -- ' + (productName || asin);
  document.getElementById('product-kw-tbody').innerHTML = '<tr><td colspan="9" class="loading">Loading...</td></tr>';

  const kwData = await fetch('/api/asin/' + asin + '/keywords').then(r => r.json()).catch(() => null);

  // Render Keyword Portfolio
  try {
    const kwRows = kwData && kwData.keywords ? kwData.keywords : [];
    if (!kwRows.length) {
      document.getElementById('product-kw-tbody').innerHTML =
        '<tr><td colspan="9" style="color:var(--muted);padding:16px">No keyword data for this ASIN.</td></tr>';
    } else {
      document.getElementById('product-kw-tbody').innerHTML = kwRows.slice(0, 100).map(r => {
        const relScore = r.keyword_relevance != null
          ? `<div style="display:flex;align-items:center;gap:5px">
               <div style="width:40px;height:5px;background:var(--border);border-radius:3px">
                 <div style="width:${Math.round(r.keyword_relevance*100)}%;height:100%;background:var(--blue);border-radius:3px"></div>
               </div>
               <span style="font-size:0.72rem;color:var(--blue)">${(+r.keyword_relevance).toFixed(3)}</span>
             </div>`
          : '--';
        return `<tr>
          <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.search_query}">${r.search_query}</td>
          <td class="num">${relScore}</td>
          <td>${roleBadge(r.keyword_role || 'other')}</td>
          <td><span style="color:var(--muted);font-size:0.72rem">${r.keyword_type||'--'}</span></td>
          <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
          <td class="num">${fmtN(r.asin_purchases||0)}</td>
          <td class="num">${r.revenue_score ? fmt$(Math.round(r.revenue_score)) : '--'}</td>
          <td class="num">${trendArrow(r.share_trend)}</td>
          <td>${strategyBadge(r.strategy)}</td>
        </tr>`;
      }).join('');
    }
  } catch (e) {
    document.getElementById('product-kw-tbody').innerHTML =
      '<tr><td colspan="9" style="color:var(--red);padding:16px">Error loading keywords.</td></tr>';
  }

  kwPanel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}


// ----------- SEARCH TERMS -----------
let searchData = [];
let searchSortCol = 'l52_avg_click_share', searchSortDir = -1;

async function loadSearch() {
  searchData = await fetch('/api/search-terms').then(r=>r.json());
  renderSearch();
  document.querySelectorAll('#search-table th').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (!col) return;
      if (searchSortCol === col) searchSortDir *= -1;
      else { searchSortCol = col; searchSortDir = -1; }
      document.querySelectorAll('#search-table th').forEach(t => t.classList.remove('sort-asc','sort-desc'));
      th.classList.add(searchSortDir === 1 ? 'sort-asc' : 'sort-desc');
      renderSearch();
    });
  });
}

function sparkHtml(vals) {
  if (!vals || !vals.length) return '';
  const max = Math.max(...vals, 0.01);
  return `<div class="spark">${vals.map(v=>{
    const h = Math.max(2, Math.round((v/max)*20));
    const opacity = v > 0 ? 0.4 + 0.6*(v/max) : 0.15;
    return `<span style="height:${h}px;opacity:${opacity}"></span>`;
  }).join('')}</div>`;
}

function renderSearch() {
  const sorted = [...searchData].sort((a,b) => {
    const va = a[searchSortCol], vb = b[searchSortCol];
    if (typeof va === 'string') return searchSortDir * va.localeCompare(vb);
    return searchSortDir * ((+va||0) - (+vb||0));
  });
  document.getElementById('search-tbody').innerHTML = sorted.map(r => `
    <tr>
      <td>${r.search_term}</td>
      <td class="num">${r.l52_avg_search_freq_rank > 0 ? fmtN(r.l52_avg_search_freq_rank) : '--'}</td>
      <td class="num">${r.l52_avg_click_share.toFixed(1)}%</td>
      <td class="num">${r.p52_avg_click_share.toFixed(1)}%</td>
      <td class="num ${changeClass(r.click_share_change_pp)}">${fmtPP(r.click_share_change_pp)}</td>
      <td class="num">${r.l52_avg_conversion_share.toFixed(1)}%</td>
      <td class="num">${r.p52_avg_conversion_share.toFixed(1)}%</td>
      <td class="num">${r.l52_months_present}</td>
      <td>${sparkHtml(r.sparkline)}</td>
    </tr>`).join('');
}

// ----------- ASIN EXPLORER -----------
let asinList = [];
let explorerCharts = {};

async function initExplorer() {
  asinList = await fetch('/api/asins').then(r=>r.json());
  const input  = document.getElementById('asin-input');
  const acList = document.getElementById('ac-list');

  input.addEventListener('input', () => {
    const q = input.value.toLowerCase().trim();
    if (!q) { acList.classList.remove('open'); return; }
    const hits = asinList.filter(a =>
      a.asin.toLowerCase().includes(q) ||
      (a.product_name||'').toLowerCase().includes(q)
    ).slice(0, 12);
    if (!hits.length) { acList.classList.remove('open'); return; }
    acList.innerHTML = hits.map(a=>`
      <div class="ac-item" data-asin="${a.asin}">
        <div>${a.product_name||a.asin}</div>
        <div class="ac-asin">${a.asin} | ${a.sku||''} | ${a.price ? '$'+a.price : ''}</div>
      </div>`).join('');
    acList.classList.add('open');
  });

  acList.addEventListener('click', e => {
    const item = e.target.closest('.ac-item');
    if (!item) return;
    input.value = item.dataset.asin;
    acList.classList.remove('open');
    loadAsin(item.dataset.asin);
  });

  document.addEventListener('click', e => {
    if (!e.target.closest('.search-wrap')) acList.classList.remove('open');
  });
}

function openAsin(asin) {
  showTab('explorer');
  document.getElementById('asin-input').value = asin;
  document.getElementById('ac-list').classList.remove('open');
  loadAsin(asin);
}

async function loadAsin(asin) {
  document.getElementById('explorer-content').innerHTML = '<div class="loading">Loading...</div>';
  const [data, kwPortfolio] = await Promise.all([
    fetch('/api/asin/' + asin).then(r=>r.json()),
    fetch('/api/asin/' + asin + '/keywords').then(r=>r.json()).catch(()=>null),
  ]);
  renderAsin(data, kwPortfolio);
}

function renderAsin(data, kwPortfolio) {
  const lst     = data.listing || {};
  const monthly = data.monthly_sales || [];
  const terms   = data.search_terms || [];
  const catalog = data.catalog || [];
  const basket  = data.market_basket || [];
  const queries = data.top_queries || [];
  const sqpMo   = data.sqp_monthly || [];

  Object.values(explorerCharts).forEach(c => c.destroy());
  explorerCharts = {};

  const uniqueTerms = [...new Set(terms.map(t=>t.search_term))];

  const allMonths = monthly.map(r=>r.month);
  const aov = monthly.map(r => r.units > 0 ? r.revenue / r.units : null);

  const periodColor = m => m === 'L52' ? 'rgba(88,166,255,.75)' : 'rgba(188,140,255,.55)';
  const periodBorder = m => m === 'L52' ? '#58a6ff' : '#bc8cff';

  const sqpImpShare  = sqpMo.map(r => r.total_impressions > 0 ? (r.asin_impressions / r.total_impressions * 100) : null);
  const sqpClkShare  = sqpMo.map(r => r.total_clicks > 0      ? (r.asin_clicks      / r.total_clicks      * 100) : null);

  const catCtr       = catalog.map(r => r.impressions > 0 ? (r.clicks    / r.impressions * 100) : null);
  const catCartRate  = catalog.map(r => r.clicks > 0      ? (r.cart_adds / r.clicks      * 100) : null);
  const catPurRate   = catalog.map(r => r.cart_adds > 0   ? (r.purchases / r.cart_adds   * 100) : null);

  const l52rows = monthly.filter(r=>r.period==='L52');
  const p52rows = monthly.filter(r=>r.period==='P52');
  const sumF = (arr, f) => arr.reduce((s,r)=>s+(+r[f]||0), 0);
  const l52Rev = sumF(l52rows,'revenue'), p52Rev = sumF(p52rows,'revenue');
  const l52Units = sumF(l52rows,'units'), p52Units = sumF(p52rows,'units');
  const l52Sess  = sumF(l52rows,'sessions'), p52Sess = sumF(p52rows,'sessions');
  const pctChg = (a,b) => b ? ((a-b)/b*100).toFixed(1) : null;
  const kpiChg = (a,b) => { const v=pctChg(a,b); if(!v) return ''; const up=+v>=0; return `<span style="color:${up?'var(--green)':'var(--red)'};font-size:0.75rem;margin-left:4px">${up?'\u25b2':'\u25bc'}${Math.abs(v)}%</span>`; };

  let html = `
    <div class="product-card">
      <div>
        <h2>${lst.product_name || data.asin}</h2>
        <div class="product-meta">
          <div class="meta-item"><strong>ASIN</strong> ${data.asin}</div>
          <div class="meta-item"><strong>SKU</strong> ${lst.sku||'--'}</div>
          <div class="meta-item"><strong>Price</strong> ${lst.price ? '$'+lst.price : '--'}</div>
          <div class="meta-item"><strong>Status</strong> ${lst.status||'--'}</div>
          <div class="meta-item"><strong>Fulfillment</strong> ${lst.fulfillment||'--'}</div>
        </div>
      </div>
      <div style="display:flex;gap:24px;align-items:center;flex-shrink:0">
        <div style="text-align:right">
          <div style="font-size:0.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">L52 Revenue</div>
          <div style="font-size:1.3rem;font-weight:700">${fmt$(l52Rev)}</div>
          <div style="font-size:0.72rem;color:var(--muted)">P52: ${fmt$(p52Rev)} ${kpiChg(l52Rev,p52Rev)}</div>
        </div>
        <div style="text-align:right">
          <div style="font-size:0.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">L52 Units</div>
          <div style="font-size:1.3rem;font-weight:700">${fmtN(l52Units)}</div>
          <div style="font-size:0.72rem;color:var(--muted)">P52: ${fmtN(p52Units)} ${kpiChg(l52Units,p52Units)}</div>
        </div>
        <div style="text-align:right">
          <div style="font-size:0.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">L52 Sessions</div>
          <div style="font-size:1.3rem;font-weight:700">${fmtN(l52Sess)}</div>
          <div style="font-size:0.72rem;color:var(--muted)">P52: ${fmtN(p52Sess)} ${kpiChg(l52Sess,p52Sess)}</div>
        </div>
      </div>
    </div>

    <div class="section-head">Sales Performance</div>
    <div class="three-col">
      <div class="card"><div class="card-title">Revenue by Month</div><div class="chart-wrap"><canvas id="asin-chart-rev"></canvas></div></div>
      <div class="card"><div class="card-title">Units by Month</div><div class="chart-wrap"><canvas id="asin-chart-units"></canvas></div></div>
      <div class="card"><div class="card-title">Avg Order Value (AOV)</div><div class="chart-wrap"><canvas id="asin-chart-aov"></canvas></div></div>
    </div>

    <div class="section-head">Traffic &amp; Conversion</div>
    <div class="three-col">
      <div class="card"><div class="card-title">Sessions by Month</div><div class="chart-wrap"><canvas id="asin-chart-sess"></canvas></div></div>
      <div class="card"><div class="card-title">Conversion Rate %</div><div class="chart-wrap"><canvas id="asin-chart-cvr"></canvas></div></div>
      <div class="card"><div class="card-title">Buy Box %</div><div class="chart-wrap"><canvas id="asin-chart-bb"></canvas></div></div>
    </div>`;

  if (sqpMo.length) {
    html += `
    <div class="section-head">Search Visibility (SQP)</div>
    <div class="two-col">
      <div class="card"><div class="card-title">Impression Share % by Month</div><div class="chart-wrap"><canvas id="asin-chart-imp-share"></canvas></div></div>
      <div class="card"><div class="card-title">Click Share % by Month</div><div class="chart-wrap"><canvas id="asin-chart-clk-share"></canvas></div></div>
    </div>`;
  }

  if (catalog.length) {
    html += `
    <div class="section-head">Search Funnel</div>
    <div class="three-col">
      <div class="card"><div class="card-title">Impressions &amp; Clicks</div><div class="chart-wrap"><canvas id="asin-chart-imp"></canvas></div></div>
      <div class="card"><div class="card-title">Cart Adds &amp; Purchases</div><div class="chart-wrap"><canvas id="asin-chart-cart"></canvas></div></div>
      <div class="card"><div class="card-title">Funnel Rates -- CTR, Cart Rate, Purchase Rate</div><div class="chart-wrap"><canvas id="asin-chart-funnel-rates"></canvas></div></div>
    </div>`;
  }

  if (queries.length) {
    html += `<div class="section-head">Top Search Queries (L52)</div>
    <div class="card" style="padding:0"><div class="tbl-wrap"><table><thead><tr>
      <th>Query</th><th class="num">Impressions</th><th class="num">Clicks</th>
      <th class="num">Cart Adds</th><th class="num">Purchases</th>
      <th class="num">Imp Share</th><th class="num">Click Share</th>
    </tr></thead><tbody>
    ${queries.map(r=>`<tr>
      <td>${r.search_query}</td>
      <td class="num">${fmtN(r.impressions)}</td>
      <td class="num">${fmtN(r.clicks)}</td>
      <td class="num">${fmtN(r.cart_adds)}</td>
      <td class="num">${fmtN(r.purchases)}</td>
      <td class="num">${(+r.avg_imp_share).toFixed(2)}%</td>
      <td class="num">${(+r.avg_click_share).toFixed(2)}%</td>
    </tr>`).join('')}
    </tbody></table></div></div>`;
  }

  // Keyword Portfolio (from asin_keyword_scores)
  if (kwPortfolio && !kwPortfolio.error && kwPortfolio.keywords && kwPortfolio.keywords.length) {
    const kpis = kwPortfolio.kpis || {};
    const kwRows = kwPortfolio.keywords;
    const topRole = r => r.keyword_role || 'other';

    html += `<div class="section-head">Keyword Portfolio</div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
      <div class="kpi-card">
        <div class="kpi-label">Keywords Tracked</div>
        <div class="kpi-l52" style="color:var(--blue)">${fmtN(kpis.total_keywords||0)}</div>
        <div class="kpi-p52">across all search queries</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Traffic Concentration</div>
        <div class="kpi-l52" style="color:${(kpis.traffic_concentration||0)>80?'var(--yellow)':'var(--green)'}">
          ${kpis.traffic_concentration != null ? kpis.traffic_concentration.toFixed(1)+'%' : '--'}
        </div>
        <div class="kpi-p52">of clicks from top 10 keywords</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Weighted Avg CVR Index</div>
        <div class="kpi-l52" style="color:${(kpis.avg_cvr_index||0)>=1?'var(--green)':'var(--yellow)'}">
          ${kpis.avg_cvr_index != null ? (+kpis.avg_cvr_index).toFixed(2)+'x' : '--'}
        </div>
        <div class="kpi-p52">vs. marketplace average</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Search Revenue</div>
        <div class="kpi-l52" style="color:var(--text)">${kpis.total_search_revenue ? fmt$(kpis.total_search_revenue) : '--'}</div>
        <div class="kpi-p52">purchases x AOV (L52)</div>
      </div>
    </div>
    <div class="card" style="padding:0"><div class="tbl-wrap"><table id="kw-portfolio-table">
      <thead><tr>
        <th>Keyword</th>
        <th class="num" title="Weighted composite relevance score (0-1)">Relevance</th>
        <th>Role</th>
        <th>Type</th>
        <th class="num" title="Traffic contribution percentile within this ASIN">Traffic %ile</th>
        <th class="num">CVR Index</th>
        <th class="num">Volume</th>
        <th class="num">Purchases</th>
        <th class="num">Revenue</th>
        <th class="num">Trend</th>
        <th>Strategy</th>
      </tr></thead>
      <tbody>
      ${kwRows.slice(0,100).map(r => {
        const relScore = r.keyword_relevance != null
          ? `<div style="display:flex;align-items:center;gap:5px">
               <div style="width:40px;height:5px;background:var(--border);border-radius:3px">
                 <div style="width:${Math.round(r.keyword_relevance*100)}%;height:100%;background:var(--blue);border-radius:3px"></div>
               </div>
               <span style="font-size:0.72rem;color:var(--blue)">${(+r.keyword_relevance).toFixed(3)}</span>
             </div>`
          : '--';
        const tcPct = r.within_asin_traffic_pct != null
          ? `${Math.round(r.within_asin_traffic_pct*100)}th`
          : '--';
        return `<tr>
          <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.search_query}">${r.search_query}</td>
          <td class="num">${relScore}</td>
          <td>${roleBadge(topRole(r))}</td>
          <td><span style="color:var(--muted);font-size:0.72rem">${r.keyword_type||'--'}</span></td>
          <td class="num"><span style="color:var(--muted);font-size:0.72rem">${tcPct}</span></td>
          <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
          <td class="num">${r.volume ? fmtN(r.volume) : (r.search_volume ? fmtN(r.search_volume) : '--')}</td>
          <td class="num">${fmtN(r.asin_purchases||0)}</td>
          <td class="num">${r.revenue_score ? fmt$(Math.round(r.revenue_score)) : '--'}</td>
          <td class="num">${trendArrow(r.share_trend)}</td>
          <td>${strategyBadge(r.strategy)}</td>
        </tr>`;
      }).join('')}
      </tbody>
    </table></div></div>`;
  }

  if (basket.length) {
    html += `<div class="section-head">Market Basket -- Frequently Bought Together</div>
    <div class="card" style="padding:0"><div class="tbl-wrap"><table><thead><tr>
      <th>Purchased With</th><th class="num">Avg Combo %</th><th class="num">Months Seen</th>
    </tr></thead><tbody>
    ${basket.map(r=>`<tr>
      <td><div class="product-name" title="${r.product_name}">${r.product_name || r.purchased_with_asin}</div>
          <span class="asin-link" onclick="openAsin('${r.purchased_with_asin}')">${r.purchased_with_asin}</span></td>
      <td class="num">${r.avg_pct.toFixed(1)}%</td>
      <td class="num">${r.count}</td>
    </tr>`).join('')}
    </tbody></table></div></div>`;
  }

  html += `<div class="section-head">Sales &amp; Traffic Detail</div>
    <div class="card" style="padding:0"><div class="tbl-wrap"><table><thead><tr>
      <th>Month</th><th>Period</th><th class="num">Units</th><th class="num">Revenue</th>
      <th class="num">AOV</th><th class="num">Sessions</th><th class="num">CVR%</th><th class="num">Buy Box%</th>
    </tr></thead><tbody>
    ${monthly.map(r=>`<tr>
      <td>${r.month}</td>
      <td><span class="badge ${r.period==='L52'?'badge-l52':'badge-p52'}">${r.period}</span></td>
      <td class="num">${fmtN(r.units)}</td><td class="num">${fmt$(r.revenue)}</td>
      <td class="num">${r.units>0?fmt$(r.revenue/r.units):'--'}</td>
      <td class="num">${fmtN(r.sessions)}</td><td class="num">${fmtP(r.conversion_rate_pct)}</td>
      <td class="num">${fmtP(r.buy_box_pct)}</td>
    </tr>`).join('')}
    </tbody></table></div></div>`;

  if (terms.length) {
    html += `<div class="section-head">Search Term Appearances (${uniqueTerms.length} unique terms)</div>
    <div class="card" style="padding:0"><div class="tbl-wrap"><table><thead><tr>
      <th>Month</th><th>Period</th><th>Search Term</th>
      <th class="num">SFR</th><th class="num">Position</th>
      <th class="num">Click Share%</th><th class="num">Conv Share%</th>
    </tr></thead><tbody>
    ${terms.map(r=>`<tr>
      <td>${r.month}</td>
      <td><span class="badge ${r.period==='L52'?'badge-l52':'badge-p52'}">${r.period}</span></td>
      <td>${r.search_term}</td>
      <td class="num">${fmtN(r.search_freq_rank)}</td>
      <td class="num">#${r.click_share_rank}</td>
      <td class="num">${r.click_share_pct.toFixed(1)}%</td>
      <td class="num">${r.conversion_share_pct.toFixed(1)}%</td>
    </tr>`).join('')}
    </tbody></table></div></div>`;
  }

  document.getElementById('explorer-content').innerHTML = html;

  // Chart helpers
  const GRID = '#1e2533';

  function periodBarChart(id, field, label, fmtFn) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    explorerCharts[id] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: {
        labels: allMonths,
        datasets: [{
          label,
          data:            monthly.map(r => r[field]),
          backgroundColor: monthly.map(r => periodColor(r.period)),
          borderColor:     monthly.map(r => periodBorder(r.period)),
          borderWidth: 1, borderRadius: 3,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: c => ` ${label}: ${fmtFn(c.raw)}`, afterLabel: c => ` Period: ${monthly[c.dataIndex]?.period||''}` } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { maxRotation: 45, font: { size: 9 } } },
          y: { grid: { color: GRID }, ticks: { callback: v => fmtFn(v) } }
        }
      }
    });
  }

  function lineChart(id, datasets, yFmt, yLabel) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    explorerCharts[id] = new Chart(ctx.getContext('2d'), {
      type: 'line',
      data: { labels: allMonths, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: datasets.length > 1, position: 'top', labels: { boxWidth: 10, font: { size: 10 } } },
          tooltip: { mode: 'index', intersect: false, callbacks: { label: c => ` ${c.dataset.label}: ${yFmt ? yFmt(c.raw) : c.raw}` } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { maxRotation: 45, font: { size: 9 } } },
          y: { grid: { color: GRID }, ticks: { callback: v => yFmt ? yFmt(v) : v } }
        }
      }
    });
  }

  periodBarChart('asin-chart-rev',   'revenue', 'Revenue', fmt$);
  periodBarChart('asin-chart-units', 'units',   'Units',   fmtN);

  lineChart('asin-chart-aov', [{
    label: 'AOV',
    data: aov,
    borderColor: '#e3b341', backgroundColor: 'rgba(227,179,65,.08)',
    borderWidth: 2, tension: 0.3, fill: true, pointRadius: 4,
    pointBackgroundColor: monthly.map(r => periodBorder(r.period)),
  }], fmt$);

  periodBarChart('asin-chart-sess', 'sessions', 'Sessions', fmtN);

  lineChart('asin-chart-cvr', [{
    label: 'CVR %',
    data: monthly.map(r => r.conversion_rate_pct),
    borderColor: '#3fb950', backgroundColor: 'rgba(63,185,80,.08)',
    borderWidth: 2, tension: 0.3, fill: true, pointRadius: 4,
    pointBackgroundColor: monthly.map(r => periodBorder(r.period)),
  }], v => v != null ? v.toFixed(1)+'%' : '');

  lineChart('asin-chart-bb', [{
    label: 'Buy Box %',
    data: monthly.map(r => r.buy_box_pct),
    borderColor: '#56d4dd', backgroundColor: 'rgba(86,212,221,.08)',
    borderWidth: 2, tension: 0.3, fill: true, pointRadius: 4,
    pointBackgroundColor: monthly.map(r => periodBorder(r.period)),
  }], v => v != null ? v.toFixed(1)+'%' : '');

  if (sqpMo.length) {
    const sqpLabels = sqpMo.map(r => r.month);
    const sqpLineOpts = (id, vals, color, label) => {
      const ctx = document.getElementById(id);
      if (!ctx) return;
      explorerCharts[id] = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: { labels: sqpLabels, datasets: [{
          label,
          data: vals,
          borderColor: color, backgroundColor: color.replace(')',',0.08)').replace('rgb','rgba'),
          borderWidth: 2, tension: 0.3, fill: true, pointRadius: 4,
          pointBackgroundColor: sqpMo.map(r => periodBorder(r.period)),
        }]},
        options: {
          responsive: true, maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ` ${label}: ${c.raw != null ? c.raw.toFixed(2)+'%' : '--'}` } } },
          scales: {
            x: { grid: { display: false }, ticks: { maxRotation: 45, font: { size: 9 } } },
            y: { grid: { color: GRID }, ticks: { callback: v => v.toFixed(1)+'%' }, beginAtZero: true }
          }
        }
      });
    };
    sqpLineOpts('asin-chart-imp-share', sqpImpShare, '#bc8cff', 'Impression Share %');
    sqpLineOpts('asin-chart-clk-share', sqpClkShare, '#58a6ff', 'Click Share %');
  }

  if (catalog.length) {
    const catMonths = catalog.map(r => r.month);

    const catCtx1 = document.getElementById('asin-chart-imp');
    if (catCtx1) {
      explorerCharts['asin-chart-imp'] = new Chart(catCtx1.getContext('2d'), {
        type: 'bar',
        data: { labels: catMonths, datasets: [
          { label: 'Impressions', data: catalog.map(r=>r.impressions), backgroundColor: 'rgba(88,166,255,.6)',  borderRadius: 3 },
          { label: 'Clicks',      data: catalog.map(r=>r.clicks),      backgroundColor: 'rgba(86,212,221,.6)', borderRadius: 3 },
        ]},
        options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{position:'top',labels:{boxWidth:10}}, tooltip:{mode:'index'} }, scales:{ x:{grid:{display:false}}, y:{grid:{color:GRID}} } }
      });
    }

    const catCtx2 = document.getElementById('asin-chart-cart');
    if (catCtx2) {
      explorerCharts['asin-chart-cart'] = new Chart(catCtx2.getContext('2d'), {
        type: 'bar',
        data: { labels: catMonths, datasets: [
          { label: 'Cart Adds',  data: catalog.map(r=>r.cart_adds),  backgroundColor: 'rgba(227,179,65,.6)', borderRadius: 3 },
          { label: 'Purchases',  data: catalog.map(r=>r.purchases),  backgroundColor: 'rgba(63,185,80,.6)',  borderRadius: 3 },
        ]},
        options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{position:'top',labels:{boxWidth:10}}, tooltip:{mode:'index'} }, scales:{ x:{grid:{display:false}}, y:{grid:{color:GRID}} } }
      });
    }

    const frCtx = document.getElementById('asin-chart-funnel-rates');
    if (frCtx) {
      explorerCharts['asin-chart-funnel-rates'] = new Chart(frCtx.getContext('2d'), {
        type: 'line',
        data: { labels: catMonths, datasets: [
          { label: 'CTR %',          data: catCtr,      borderColor: '#58a6ff', borderWidth:2, tension:.3, fill:false, pointRadius:3 },
          { label: 'Cart Rate %',    data: catCartRate,  borderColor: '#e3b341', borderWidth:2, tension:.3, fill:false, pointRadius:3 },
          { label: 'Purchase Rate %',data: catPurRate,   borderColor: '#3fb950', borderWidth:2, tension:.3, fill:false, pointRadius:3 },
        ]},
        options: {
          responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{position:'top',labels:{boxWidth:10,font:{size:10}}}, tooltip:{mode:'index',intersect:false,callbacks:{label:c=>` ${c.dataset.label}: ${c.raw!=null?c.raw.toFixed(2)+'%':'--'}`}} },
          scales:{ x:{grid:{display:false},ticks:{maxRotation:45,font:{size:9}}}, y:{grid:{color:GRID},ticks:{callback:v=>v+'%'}} }
        }
      });
    }
  }
}

// ----------- KEYWORDS -----------
let kwData        = [];
let kwDataComputed = [];
let kwMonthlyData = {};
let kwAllMonths   = [];
let kwPeriod      = 'L1M';
let kwTrendMode   = 'yoy';
let kwSortCol     = 'brand_clicks';
let kwSortDir     = -1;
let kwStrategy    = 'All';
let kwTrackerChart = null;

const STRATEGY_COLORS = {
  Branded:'var(--blue)', Defend:'var(--cyan)', Grow:'var(--green)',
  Watch:'var(--yellow)', Deprioritize:'var(--muted)'
};

function strategyBadge(s) {
  const cls = s ? 'badge-' + s.toLowerCase() : 'badge-deprioritize';
  return `<span class="badge ${cls}">${s||'--'}</span>`;
}

function _pctRanks(arr) {
  const valid = arr.map((v,i) => ({v,i})).filter(x => x.v != null);
  valid.sort((a,b) => a.v - b.v);
  const out = new Array(arr.length).fill(0);
  for (let k = 0; k < valid.length; k++) {
    out[valid[k].i] = valid.length > 1 ? k / (valid.length - 1) : 0.5;
  }
  return out;
}

function classifyStrategies(data) {
  const shareVals = data.map(r => r.brand_purchase_share != null ? r.brand_purchase_share : null);
  const headVals  = data.map(r => {
    if (r.volume == null || r.brand_click_share == null) return null;
    return r.volume * (1.0 - r.brand_click_share / 100);
  });
  const sharePcts = _pctRanks(shareVals);
  const headPcts  = _pctRanks(headVals);

  for (let i = 0; i < data.length; i++) {
    const r = data[i];
    if (r.keyword_type === 'branded') { r.strategy = 'Branded'; continue; }
    if (sharePcts[i] >= 0.70 && headPcts[i] <= 0.30) { r.strategy = 'Defend'; continue; }
    if (r.cvr_index != null && r.cvr_index >= 1.0)    { r.strategy = 'Grow';   continue; }
    if (r.cvr_index != null && r.cvr_index >= 0.7)    { r.strategy = 'Watch';  continue; }
    r.strategy = 'Deprioritize';
  }
}

function roleBadge(r) {
  if (!r) return '--';
  const cls = 'badge role-' + r.toLowerCase();
  return `<span class="${cls}">${r}</span>`;
}

function trendArrow(v, unit='%') {
  if (v == null) return '<span class="trend-flat">--</span>';
  const pct = (v * 100).toFixed(1);
  const threshold = unit === 'pp' ? 0.005 : 0.05;
  if (v > threshold)  return `<span class="trend-up">\u25b2 ${pct}${unit}</span>`;
  if (v < -threshold) return `<span class="trend-down">\u25bc ${Math.abs(pct)}${unit}</span>`;
  return `<span class="trend-flat">~ ${pct}${unit}</span>`;
}

function cvrIndexFmt(v) {
  if (v == null) return '--';
  const n = +v;
  const color = n >= 1.5 ? 'var(--green)' : n >= 1.0 ? '#a0d4a0' : n >= 0.5 ? 'var(--yellow)' : 'var(--red)';
  return `<span style="color:${color};font-weight:600">${n.toFixed(2)}x</span>`;
}

function pctBar(v) {
  if (v == null) return '--';
  const pct = Math.round(v * 100);
  return `<span style="color:var(--muted);font-size:0.72rem">${pct}th</span>`;
}

async function loadKeywords() {
  const [rows, weights, monthly] = await Promise.all([
    fetch('/api/keywords?limit=5000').then(r=>r.json()),
    fetch('/api/scoring-weights').then(r=>r.json()),
    fetch('/api/keywords/monthly').then(r=>r.json()),
  ]);

  kwMonthlyData = {};
  const monthSet = new Set();
  for (const row of monthly) {
    if (!kwMonthlyData[row.search_query]) kwMonthlyData[row.search_query] = [];
    kwMonthlyData[row.search_query].push(row);
    monthSet.add(row.month);
  }
  kwAllMonths = [...monthSet].sort();

  renderScoringWeights(weights);
  kwData = rows;

  aggregateKeywords(kwPeriod);
  renderKwKPIs();
  applyFilters();
  renderCvrLeaders(kwDataComputed);
  renderLongTail(kwDataComputed);
}

function renderScoringWeights(w) {
  if (!w || w.error) return;

  function weightGroup(title, obj) {
    const rows = Object.entries(obj).map(([k, v]) => `
      <div class="weight-row">
        <span class="weight-label">${k}</span>
        <div style="display:flex;align-items:center;gap:6px">
          <div class="weight-bar-wrap"><div class="weight-bar" style="width:${Math.round(v*100)}%"></div></div>
          <span class="weight-val">${(v*100).toFixed(0)}%</span>
        </div>
      </div>`).join('');
    return `<div><div class="weights-group-title">${title}</div>${rows}</div>`;
  }

  function thresholdGroup(title, obj) {
    const rows = Object.entries(obj).map(([k, v]) => `
      <div class="weight-row">
        <span class="weight-label" style="font-size:0.7rem">${k.replace(/_/g,' ')}</span>
        <span class="weight-val">${(v*100).toFixed(0)}th pct</span>
      </div>`).join('');
    return `<div><div class="weights-group-title">${title}</div>${rows}</div>`;
  }

  document.getElementById('weights-content').innerHTML = `
    <div class="weights-grid">
      ${weightGroup('Keyword Relevance (KW -> ASIN)', w.kw_relevance||{})}
      ${weightGroup('ASIN Priority (ASIN -> KW)', w.asin_priority||{})}
      ${thresholdGroup('Strategy Thresholds (percentile)', w.role_thresholds||{})}
    </div>`;
}

function renderKwKPIs() {
  const data = kwDataComputed || [];
  const byStrat = {};
  for (const r of data) {
    const s = r.strategy || 'Deprioritize';
    if (!byStrat[s]) byStrat[s] = {cnt:0, pur:0};
    byStrat[s].cnt++;
    byStrat[s].pur += (r.brand_purchases || 0);
  }
  const trending = data.filter(r => r.share_trend != null && r.share_trend > 0.005).length;

  const cards = [
    { label:'Branded', val: fmtN(byStrat.Branded?.cnt||0), sub: fmtN(byStrat.Branded?.pur||0)+' purchases', color:'var(--blue)' },
    { label:'Defend', val: fmtN(byStrat.Defend?.cnt||0), sub: fmtN(byStrat.Defend?.pur||0)+' purchases', color:'var(--cyan)' },
    { label:'Grow', val: fmtN(byStrat.Grow?.cnt||0), sub: fmtN(byStrat.Grow?.pur||0)+' purchases', color:'var(--green)' },
    { label:'Watch', val: fmtN(byStrat.Watch?.cnt||0), sub: fmtN(byStrat.Watch?.pur||0)+' purchases', color:'var(--yellow)' },
    { label:'Trending Up', val: fmtN(trending), sub: 'share_trend > 0.5pp', color:'var(--purple)' },
  ];

  document.getElementById('kw-kpi-grid').innerHTML = cards.map(c => `
    <div class="kpi-card">
      <div class="kpi-label">${c.label}</div>
      <div class="kpi-l52" style="color:${c.color}">${c.val}</div>
      <div class="kpi-p52">${c.sub}</div>
    </div>`).join('');
}

function _windowMonths(period) {
  if (!kwAllMonths.length) return kwAllMonths;
  const n = period === 'L1M' ? 1 : period === 'L3M' ? 3 : period === 'L6M' ? 6 : kwAllMonths.length;
  return kwAllMonths.slice(-n);
}

function _daysInMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return new Date(y, m, 0).getDate();
}

function _comparisonMonths(currentMonths, mode) {
  if (mode === 'yoy') {
    return currentMonths.map(m => {
      const y = parseInt(m.slice(0,4)) - 1;
      return y.toString().padStart(4,'0') + m.slice(4);
    });
  }
  const n = currentMonths.length;
  return currentMonths
    .map(m => { const i = kwAllMonths.indexOf(m); return i - n >= 0 ? kwAllMonths[i - n] : null; })
    .filter(Boolean);
}

function setTrendMode(btn) {
  document.querySelectorAll('[data-trend]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  kwTrendMode = btn.dataset.trend;
  aggregateKeywords(kwPeriod);
  applyFilters();
}

function aggregateKeywords(period) {
  kwPeriod = period;
  const months = new Set(_windowMonths(period));

  const sorted = [...months].sort();
  const label = sorted.length ? `${sorted[0]} -- ${sorted[sorted.length-1]}` : '';
  const el = document.getElementById('kw-period-label');
  if (el) el.textContent = label;

  kwDataComputed = kwData.map(base => {
    const monthRows = (kwMonthlyData[base.search_query] || []).filter(r => months.has(r.month));
    if (!monthRows.length) {
      return { ...base, brand_impressions: 0, brand_clicks: 0, brand_purchases: 0,
               mkt_impressions: 0, mkt_clicks: 0, mkt_purchases: 0,
               brand_click_share: null, brand_purchase_share: null,
               brand_cvr: null, mkt_cvr: null, cvr_index: null,
               brand_ctr: null, mkt_ctr: null, ctr_index: null,
               volume: null, months_in_window: 0 };
    }

    const brand_impressions = monthRows.reduce((s,r) => s + (r.brand_impressions || 0), 0);
    const brand_clicks    = monthRows.reduce((s,r) => s + (r.brand_clicks    || 0), 0);
    const brand_purchases = monthRows.reduce((s,r) => s + (r.brand_purchases || 0), 0);
    const mkt_impressions = monthRows.reduce((s,r) => s + (r.mkt_impressions || 0), 0);
    const mkt_clicks    = monthRows.reduce((s,r) => s + (r.mkt_clicks    || 0), 0);
    const mkt_purchases = monthRows.reduce((s,r) => s + (r.mkt_purchases || 0), 0);
    const volume        = monthRows.reduce((s,r) => s + (r.volume        || 0), 0);

    const brand_cvr    = brand_clicks    > 0 ? brand_purchases / brand_clicks    * 100 : null;
    const mkt_cvr    = mkt_clicks    > 0 ? mkt_purchases / mkt_clicks    * 100 : null;
    const cvr_index  = (brand_cvr != null && mkt_cvr > 0) ? brand_cvr / mkt_cvr : null;
    const brand_ctr    = brand_impressions > 0 ? brand_clicks / brand_impressions * 100 : null;
    const mkt_ctr    = mkt_impressions > 0 ? mkt_clicks / mkt_impressions * 100 : null;
    const ctr_index  = (brand_ctr != null && mkt_ctr > 0) ? brand_ctr / mkt_ctr : null;
    const brand_click_share    = mkt_clicks    > 0 ? brand_clicks    / mkt_clicks    * 100 : null;
    const brand_purchase_share = mkt_purchases > 0 ? brand_purchases / mkt_purchases * 100 : null;

    const compMonthSet = new Set(_comparisonMonths([...months], kwTrendMode));
    const compRows = (kwMonthlyData[base.search_query] || []).filter(r => compMonthSet.has(r.month));
    let share_trend = null;
    let volume_trend = null;
    if (compRows.length) {
      const comp_mkt_pur = compRows.reduce((s,r) => s + (r.mkt_purchases || 0), 0);
      const comp_brand_pur = compRows.reduce((s,r) => s + (r.brand_purchases || 0), 0);
      const comp_purchase_share = comp_mkt_pur > 0 ? comp_brand_pur / comp_mkt_pur * 100 : null;
      if (comp_purchase_share != null && brand_purchase_share != null)
        share_trend = (brand_purchase_share - comp_purchase_share) / 100;
      const cur_vol_daily  = monthRows.reduce((s,r) => s + (r.volume || 0) / _daysInMonth(r.month), 0);
      const comp_vol_daily = compRows.reduce((s,r)  => s + (r.volume || 0) / _daysInMonth(r.month), 0);
      if (comp_vol_daily > 0) volume_trend = (cur_vol_daily - comp_vol_daily) / comp_vol_daily;
    }

    return {
      ...base,
      brand_impressions, brand_clicks, brand_purchases,
      mkt_impressions, mkt_clicks, mkt_purchases,
      brand_click_share, brand_purchase_share, brand_cvr, mkt_cvr, cvr_index,
      brand_ctr, mkt_ctr, ctr_index,
      volume: Math.round(volume) || null,
      months_of_data: base.months_of_data,
      months_in_window: monthRows.length,
      share_trend,
      volume_trend,
    };
  });

  classifyStrategies(kwDataComputed);
}

function setHorizon(btn) {
  document.querySelectorAll('.kw-horizon-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  aggregateKeywords(btn.dataset.period);
  renderKwKPIs();
  applyFilters();
  renderCvrLeaders(kwDataComputed);
  renderLongTail(kwDataComputed);
}

function setChipFilter(groupId, el) {
  document.querySelectorAll(`#${groupId} .chip`).forEach(c => c.classList.remove('active'));
  el.classList.add('active');
}

function filterKeywords(el) {
  document.querySelectorAll('#strategy-chips .chip').forEach(c=>c.classList.remove('active'));
  el.classList.add('active');
  kwStrategy = el.dataset.strategy;
  applyFilters();
}

function applyFilters() {
  const activeStratEl = document.querySelector('#strategy-chips .chip.active');
  kwStrategy = activeStratEl ? activeStratEl.dataset.strategy : 'All';

  const searchVal   = (document.getElementById('kw-search')?.value || '').toLowerCase().trim();
  const typeActive  = document.querySelector('#kw-type-chips .chip.active')?.dataset.kwtype || 'All';
  const tierActive  = document.querySelector('#kw-tier-chips .chip.active')?.dataset.kwtier || 'All';
  const cvrFilter   = document.getElementById('kw-cvr-filter')?.value || 'all';
  const minClicks   = parseInt(document.getElementById('kw-clicks-filter')?.value || '0', 10);
  const cannibalOn  = document.getElementById('kw-cannibal-filter')?.checked || false;

  const cannibalLabel = document.getElementById('kw-cannibal-label');
  if (cannibalLabel) cannibalLabel.className = 'kw-cannibal-toggle' + (cannibalOn ? ' active-label' : '');

  const src = kwDataComputed.length ? kwDataComputed : kwData;

  let filtered = src.filter(r => {
    if (kwStrategy !== 'All') {
      if (r.strategy !== kwStrategy) return false;
    }
    if (searchVal && !r.search_query.toLowerCase().includes(searchVal)) return false;
    if (typeActive !== 'All' && r.keyword_type !== typeActive) return false;
    if (tierActive !== 'All' && r.vol_tier !== tierActive) return false;
    if (cannibalOn && !r.cannibalization_flag) return false;
    if (cvrFilter === 'over'  && !(r.cvr_index > 1))  return false;
    if (cvrFilter === 'under' && !(r.cvr_index != null && r.cvr_index < 1)) return false;
    if (minClicks > 0 && (r.brand_clicks || 0) < minClicks) return false;
    return true;
  });

  const countEl = document.getElementById('kw-filter-count');
  if (countEl) countEl.textContent = `${filtered.length.toLocaleString()} keyword${filtered.length !== 1 ? 's' : ''}`;

  renderKwTable(filtered);
}

function sortKwTable(col) {
  if (kwSortCol === col) { kwSortDir *= -1; }
  else { kwSortCol = col; kwSortDir = -1; }

  document.querySelectorAll('#kw-table th').forEach(th => {
    th.classList.remove('sort-asc','sort-desc');
    if (th.dataset.kwcol === col) th.classList.add(kwSortDir === -1 ? 'sort-desc':'sort-asc');
  });

  applyFilters();
}

function renderKwTable(rows) {
  const sorted = [...rows].sort((a, b) => {
    let av = a[kwSortCol], bv = b[kwSortCol];
    if (av == null) av = kwSortDir === -1 ? -Infinity : Infinity;
    if (bv == null) bv = kwSortDir === -1 ? -Infinity : Infinity;
    if (typeof av === 'string') return kwSortDir * av.localeCompare(bv);
    return kwSortDir * (av - bv);
  });

  const tbody = document.getElementById('kw-tbody');
  tbody.innerHTML = sorted.slice(0, 500).map(r => {
    const asinCountCell = r.asin_count != null
      ? `<span style="color:${r.cannibalization_flag ? 'var(--yellow)' : 'var(--muted)'}">
           ${r.asin_count}${r.cannibalization_flag ? ' !' : ''}
         </span>`
      : '--';
    return `<tr class="kw-row" data-query="${r.search_query.replace(/"/g,'&quot;')}"
              data-cannibal="${r.cannibalization_flag||0}"
              onclick="selectKwRow(this,'${r.search_query.replace(/'/g,"\\'")}')">
      <td>${r.search_query}</td>
      <td class="num">${r.volume ? fmtN(r.volume) : '--'}</td>
      <td><span style="color:var(--muted);font-size:0.72rem">${r.vol_tier||'--'}</span></td>
      <td>${strategyBadge(r.strategy)}</td>
      <td class="num">${asinCountCell}</td>
      <td class="num">${r.brand_click_share != null ? fmtP(r.brand_click_share) : '--'}</td>
      <td class="num">${r.brand_purchase_share != null ? fmtP(r.brand_purchase_share) : '--'}</td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
      <td class="num">${cvrIndexFmt(r.ctr_index)}</td>
      <td class="num">${r.brand_cvr != null ? fmtP(r.brand_cvr) : '--'}</td>
      <td class="num">${r.mkt_cvr != null ? fmtP(r.mkt_cvr) : '--'}</td>
      <td class="num">${trendArrow(r.share_trend, 'pp')}</td>
      <td class="num">${trendArrow(r.volume_trend, '%')}</td>
      <td><span class="asin-link" onclick="event.stopPropagation();navigateToAsin('${r.hero_asin||''}')">${r.hero_asin||'--'}</span></td>
      <td class="num">${r.brand_clicks ? fmtN(r.brand_clicks) : '--'}</td>
    </tr>`;
  }).join('');

  document.querySelectorAll('#kw-table th[data-kwcol]').forEach(th => {
    th.onclick = () => sortKwTable(th.dataset.kwcol);
  });
}

// Click a keyword row -> show Share Tracker + ASIN Allocation
async function selectKwRow(el, query) {
  document.querySelectorAll('.kw-row').forEach(r => r.classList.remove('kw-row-selected'));
  el.classList.add('kw-row-selected');

  const isCannibal = el.dataset.cannibal === '1';

  const tracker = document.getElementById('share-tracker');
  tracker.classList.add('visible');
  document.getElementById('tracker-title').textContent = query;
  document.getElementById('tracker-sub').textContent = 'Loading monthly share data...';

  const allocWrap = document.getElementById('asin-allocation');
  allocWrap.classList.add('visible');
  document.getElementById('allocation-title').textContent = `ASIN Allocation -- "${query}"`;
  document.getElementById('allocation-sub').textContent = 'Brand ASINs ranked by priority score for this keyword';
  const cannibalBadge = document.getElementById('cannibal-badge');
  cannibalBadge.style.display = isCannibal ? 'inline-block' : 'none';
  document.getElementById('allocation-tbody').innerHTML =
    '<tr><td colspan="10" class="loading">Loading...</td></tr>';

  const [shareData, asinData] = await Promise.all([
    fetch('/api/keyword-share?q=' + encodeURIComponent(query)).then(r=>r.json()),
    fetch('/api/keyword/' + encodeURIComponent(query) + '/asins').then(r=>r.json()),
  ]);

  renderAllocationTable(asinData);

  const data = shareData;

  if (!data.length) {
    document.getElementById('tracker-sub').textContent = 'No monthly data available.';
    return;
  }

  const row = kwData.find(r => r.search_query === query) || {};
  document.getElementById('tracker-sub').textContent =
    `Strategy: ${row.strategy||'--'} | CVR Index: ${row.cvr_index != null ? (+row.cvr_index).toFixed(2)+'x' : '--'} | ` +
    `${data.length} months of data`;

  if (kwTrackerChart) { kwTrackerChart.destroy(); kwTrackerChart = null; }

  const ctx = document.getElementById('chart-share-tracker').getContext('2d');
  kwTrackerChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(r => r.month),
      datasets: [
        {
          label: 'Purchase Share %',
          data: data.map(r => r.brand_purchase_share),
          borderColor: '#3fb950', backgroundColor: 'rgba(63,185,80,.12)',
          borderWidth: 2, tension: .3, fill: true, pointRadius: 4,
          yAxisID: 'y',
        },
        {
          label: 'Click Share %',
          data: data.map(r => r.brand_click_share),
          borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,.08)',
          borderWidth: 2, tension: .3, fill: false, pointRadius: 4,
          yAxisID: 'y',
        },
        {
          label: 'Brand CVR %',
          data: data.map(r => r.brand_cvr),
          borderColor: '#e3b341', backgroundColor: 'rgba(227,179,65,.08)',
          borderWidth: 1.5, tension: .3, fill: false, pointRadius: 3,
          borderDash: [4, 3],
          yAxisID: 'y2',
        },
        {
          label: 'Market CVR %',
          data: data.map(r => r.mkt_cvr),
          borderColor: '#8b949e', backgroundColor: 'transparent',
          borderWidth: 1.5, tension: .3, fill: false, pointRadius: 3,
          borderDash: [4, 3],
          yAxisID: 'y2',
        },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', labels: { boxWidth: 10, font: { size: 11 } } },
        tooltip: { callbacks: { label: c => `${c.dataset.label}: ${c.raw != null ? (+c.raw).toFixed(2)+'%' : '--'}` } },
      },
      scales: {
        x: { grid: { display: false } },
        y: {
          position: 'left', grid: { color: '#1e2533' },
          title: { display: true, text: 'Share %', font: { size: 10 } },
          ticks: { callback: v => v.toFixed(1)+'%' },
        },
        y2: {
          position: 'right', grid: { display: false },
          title: { display: true, text: 'CVR %', font: { size: 10 } },
          ticks: { callback: v => v != null ? v.toFixed(1)+'%' : '' },
        }
      }
    }
  });

  tracker.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderAllocationTable(rows) {
  if (!rows || !rows.length) {
    document.getElementById('allocation-tbody').innerHTML =
      '<tr><td colspan="10" style="color:var(--muted);padding:16px">No ASIN data available for this keyword yet. Run the build pipeline first.</td></tr>';
    return;
  }
  document.getElementById('allocation-tbody').innerHTML = rows.map((r, i) => {
    const rankColor = i === 0 ? 'var(--green)' : i === 1 ? 'var(--blue)' : 'var(--muted)';
    const scoreBar = r.asin_priority != null
      ? `<div style="display:flex;align-items:center;gap:6px">
           <div style="width:50px;height:6px;background:var(--border);border-radius:3px">
             <div style="width:${Math.round(r.asin_priority*100)}%;height:100%;background:${rankColor};border-radius:3px"></div>
           </div>
           <span style="color:${rankColor};font-weight:600;font-size:0.78rem">${(+r.asin_priority).toFixed(3)}</span>
         </div>`
      : '--';
    const domPct = r.within_kw_dominance_pct != null
      ? `${Math.round(r.within_kw_dominance_pct*100)}th`
      : '--';
    const name = r.product_name ? `<span title="${r.product_name}" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block">${r.product_name}</span>` : '--';
    return `<tr>
      <td><span class="asin-link" onclick="navigateToAsin('${r.asin||''}')">${r.asin||'--'}</span></td>
      <td>${name}</td>
      <td class="num">${scoreBar}</td>
      <td class="num"><span style="color:var(--muted);font-size:0.72rem">${domPct}</span></td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
      <td class="num">${r.asin_cvr != null ? (r.asin_cvr*100).toFixed(1)+'%' : '--'}</td>
      <td class="num">${r.aov != null ? '$'+(+r.aov).toFixed(2) : '--'}</td>
      <td class="num">${r.revenue_score != null ? '$'+fmtN(Math.round(r.revenue_score)) : '--'}</td>
      <td class="num">${fmtN(r.asin_purchases||0)}</td>
      <td>${roleBadge(r.keyword_role)}</td>
    </tr>`;
  }).join('');
}

function renderCvrLeaders(rows) {
  const grow = rows
    .filter(r => r.strategy === 'Grow' && r.cvr_index != null && r.volume != null)
    .sort((a, b) => (b.volume * b.cvr_index) - (a.volume * a.cvr_index))
    .slice(0, 20);

  document.getElementById('cvr-leaders-tbody').innerHTML = grow.map(r => `
    <tr class="kw-row" onclick="selectKwRow(this,'${r.search_query.replace(/'/g,"\\'")}')">
      <td>${r.search_query}</td>
      <td class="num">${fmtN(r.volume)}</td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
      <td class="num">${r.brand_cvr != null ? fmtP(r.brand_cvr) : '--'}</td>
      <td class="num">${r.mkt_cvr != null ? fmtP(r.mkt_cvr) : '--'}</td>
      <td class="num">${r.brand_click_share != null ? fmtP(r.brand_click_share) : '--'}</td>
      <td class="num">${fmtN(r.brand_purchases||0)}</td>
    </tr>`).join('');
}

function renderLongTail(rows) {
  const lt = rows
    .filter(r => r.vol_tier === 'long-tail' && (r.brand_cvr||0) > 20 && (r.brand_purchases||0) >= 3)
    .sort((a, b) => (b.brand_cvr||0) - (a.brand_cvr||0))
    .slice(0, 50);

  document.getElementById('longtail-tbody').innerHTML = lt.map(r => `
    <tr class="kw-row" onclick="selectKwRow(this,'${r.search_query.replace(/'/g,"\\'")}')">
      <td>${r.search_query}</td>
      <td class="num">${r.volume ? fmtN(r.volume) : '--'}</td>
      <td class="num" style="color:var(--green)">${fmtP(r.brand_cvr)}</td>
      <td class="num">${fmtN(r.brand_purchases||0)}</td>
      <td class="num">${r.brand_purchase_share != null ? fmtP(r.brand_purchase_share) : '--'}</td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
    </tr>`).join('');
}

// Simple goal modal (prompt-based for now)
async function openGoalModal(query, currentShare) {
  const target = prompt(`Set purchase share goal for:\n"${query}"\n\nCurrent: ${(+currentShare).toFixed(2)}%\n\nEnter target % (e.g. 5.0):`);
  if (!target || isNaN(+target)) return;
  const priority = prompt('Priority? (high / med / low)', 'med') || 'med';
  const notes = prompt('Notes (optional):') || '';
  await fetch('/api/keyword-goals', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ search_query: query, target_purchase_share: +target, priority, notes }),
  });
  kwData = await fetch('/api/keywords?limit=5000').then(r=>r.json());
  aggregateKeywords(kwPeriod);
  applyFilters();
}

function navigateToAsin(asin) {
  if (!asin) return;
  showTab('explorer');
  document.getElementById('asin-input').value = asin;
  loadAsin(asin);
}

// Boot
// ─── Advertising Tab ──────────────────────────────────────────────────────
let _adsKwData = [];
let _adsCurrentTerm = null;
let _monthlyRevenue = {};       // month -> revenue
let _adsMonthlyByTerm = {};     // search_term -> [{month, ad_spend, ...}, ...]
let _adsSummaryMonthly = [];    // [{month, spend, clicks, ...}, ...]
let _adsAllMonths = [];         // sorted months present in ad data
let _adsSelectedMonths = [];    // months currently selected in the ad picker
let _sqpAllMonths = [];         // sorted months present in SQP data
let _sqpSelectedMonths = [];    // months currently selected in the SQP picker
let adsSortCol = 'ad_sales';
let adsSortDir = -1;

function sortAdsTable(col) {
  if (adsSortCol === col) { adsSortDir *= -1; }
  else { adsSortCol = col; adsSortDir = -1; }
  document.querySelectorAll('#ads-kw-table th').forEach(th => {
    th.classList.remove('sort-asc','sort-desc');
    if (th.dataset.adcol === col) th.classList.add(adsSortDir === -1 ? 'sort-desc' : 'sort-asc');
  });
  filterAdsKeywords();
}

function _adsWindowMonths(period) {
  if (!_adsAllMonths.length) return _adsAllMonths;
  const n = period === 'L1M' ? 1 : period === 'L3M' ? 3 : period === 'L6M' ? 6 : _adsAllMonths.length;
  return _adsAllMonths.slice(-n);
}

function _sqpWindowMonths(period) {
  if (!kwAllMonths.length) return kwAllMonths;
  const n = period === 'L1M' ? 1 : period === 'L3M' ? 3 : period === 'L6M' ? 6 : kwAllMonths.length;
  return kwAllMonths.slice(-n);
}

let _adsWindowPeriod = 'L1M';
let _sqpWindowPeriod = 'L1M';

function setAdsHorizon(btn) {
  document.querySelectorAll('[data-adwindow]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _adsWindowPeriod = btn.dataset.adwindow;
  _adsSelectedMonths = _adsWindowMonths(_adsWindowPeriod);
  recomputeAdsOrganic();
  recomputeAdsSummary();
  filterAdsKeywords();
}

function setSqpHorizon(btn) {
  document.querySelectorAll('[data-sqpwindow]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _sqpWindowPeriod = btn.dataset.sqpwindow;
  _sqpSelectedMonths = _sqpWindowMonths(_sqpWindowPeriod);
  recomputeAdsOrganic();
  filterAdsKeywords();
}

function _initAdsHorizons() {
  _adsSelectedMonths = _adsWindowMonths(_adsWindowPeriod);
  _sqpSelectedMonths = _sqpWindowMonths(_sqpWindowPeriod);
}

function recomputeAdsOrganic() {
  // Update ad period label
  const adSet = new Set(_adsSelectedMonths);
  const adSorted = [...adSet].sort();
  const adLabel = adSorted.length ? adSorted[0] + ' \u2014 ' + adSorted[adSorted.length-1] : 'none selected';
  const adEl = document.getElementById('ads-period-label');
  if (adEl) adEl.textContent = adLabel;

  // Update SQP period label
  const sqpSet = new Set(_sqpSelectedMonths);
  const sqpSorted = [...sqpSet].sort();
  const sqpLabel = sqpSorted.length ? sqpSorted[0] + ' \u2014 ' + sqpSorted[sqpSorted.length-1] : 'none selected';
  const sqpEl = document.getElementById('sqp-period-label');
  if (sqpEl) sqpEl.textContent = sqpLabel;

  const allTerms = Object.keys(_adsMonthlyByTerm);

  _adsKwData = allTerms.map(term => {
    // Ad rows filtered to ad-selected months
    const adRows = (_adsMonthlyByTerm[term] || []).filter(m => adSet.has(m.month));
    // Full SQP rows filtered to SQP-selected months (for CVR index, volume)
    const sqpRowsFull = (kwMonthlyData[term] || []).filter(m => sqpSet.has(m.month));

    // Overlap: intersection of ad-selected and sqp-selected months where BOTH have data
    const adMonthSet  = new Set(adRows.map(m => m.month));
    const sqpMonthSet = new Set(sqpRowsFull.map(m => m.month));
    const overlapMonths = new Set([...adMonthSet].filter(m => sqpMonthSet.has(m)));

    // Ad metrics over ad-selected months (unchanged)
    const ad_spend  = adRows.reduce((s,m) => s + (m.ad_spend||0), 0);
    const ad_impressions = adRows.reduce((s,m) => s + (m.ad_impressions||0), 0);
    const ad_clicks = adRows.reduce((s,m) => s + (m.ad_clicks||0), 0);
    const ad_orders = adRows.reduce((s,m) => s + (m.ad_orders||0), 0);
    const ad_sales  = adRows.reduce((s,m) => s + (m.ad_sales||0), 0);
    const ad_acos   = ad_sales > 0 ? ad_spend / ad_sales : null;
    const ad_roas   = ad_spend > 0 ? ad_sales / ad_spend : null;
    const num_campaigns = Math.max(...adRows.map(m => m.num_campaigns||0), 0);
    const ad_type_set = new Set();
    adRows.forEach(m => { if (m.ad_type_list) m.ad_type_list.split(',').forEach(t => ad_type_set.add(t)); });
    const ad_type_list = [...ad_type_set].sort().join(',');
    const best_impression_rank  = Math.min(...adRows.map(m => m.best_impression_rank).filter(v => v != null), Infinity);
    const best_impression_share = Math.max(...adRows.map(m => m.best_impression_share).filter(v => v != null), -Infinity);

    // Full SQP metrics from SQP-selected months (CVR index, volume)
    let volume = null, cvr_index = null, sqp_brand_clicks = null;
    if (sqpRowsFull.length > 0) {
      volume = sqpRowsFull.reduce((s,m) => s + (m.volume||0), 0);
      const brand_clicks = sqpRowsFull.reduce((s,m) => s + (m.brand_clicks||0), 0);
      sqp_brand_clicks = brand_clicks;
      const brand_purchases = sqpRowsFull.reduce((s,m) => s + (m.brand_purchases||0), 0);
      const mkt_clicks = sqpRowsFull.reduce((s,m) => s + (m.mkt_clicks||0), 0);
      const mkt_purchases = sqpRowsFull.reduce((s,m) => s + (m.mkt_purchases||0), 0);
      const brand_cvr = brand_clicks > 0 ? brand_purchases / brand_clicks * 100 : null;
      const mkt_cvr = mkt_clicks > 0 ? mkt_purchases / mkt_clicks * 100 : null;
      cvr_index = (brand_cvr != null && mkt_cvr > 0) ? brand_cvr / mkt_cvr : null;
    }

    // Organic units = SQP brand_purchases - ad_units using ONLY overlap months
    let organic_units = null, organic_clicks = null, organic_cvr = null;
    if (overlapMonths.size > 0) {
      const overlapSqp = sqpRowsFull.filter(m => overlapMonths.has(m.month));
      const overlapAds = adRows.filter(m => overlapMonths.has(m.month));
      const sqp_clicks    = overlapSqp.reduce((s,m) => s + (m.brand_clicks||0), 0);
      const sqp_purchases = overlapSqp.reduce((s,m) => s + (m.brand_purchases||0), 0);
      const overlap_ad_units = overlapAds.reduce((s,m) => s + (m.ad_units||0), 0);
      organic_units  = Math.max(0, sqp_purchases - overlap_ad_units);
      organic_clicks = Math.max(0, sqp_clicks - overlapAds.reduce((s,m) => s + (m.ad_clicks||0), 0));
      organic_cvr = organic_clicks > 0 ? organic_units / organic_clicks * 100 : null;
    }

    const strategy = _adsStrategyByTerm[term] || null;
    const keyword_type = _adsKwTypeByTerm[term] || null;

    return {
      search_term: term, ad_type_list, ad_spend, ad_impressions, ad_clicks,
      ad_orders, ad_sales, ad_acos, ad_roas, num_campaigns,
      best_impression_rank: best_impression_rank === Infinity ? null : best_impression_rank,
      best_impression_share: best_impression_share === -Infinity ? null : best_impression_share,
      organic_volume: volume != null ? Math.round(volume) || null : null,
      sqp_brand_clicks: sqp_brand_clicks || null,
      organic_units, organic_clicks: organic_clicks || null, organic_cvr,
      cvr_index, strategy, keyword_type
    };
  });
}

function recomputeAdsSummary() {
  const selectedSet = new Set(_adsSelectedMonths);
  const filtered = _adsSummaryMonthly.filter(m => selectedSet.has(m.month));

  let totalSpend=0, totalSales=0, totalClicks=0, totalImpressions=0, totalOrders=0;
  filtered.forEach(r => {
    totalSpend += r.spend||0; totalSales += r.sales||0;
    totalClicks += r.clicks||0; totalImpressions += r.impressions||0;
    totalOrders += r.orders||0;
  });

  // TACOS: revenue from the same months that have ad data
  let totalRevenue = 0;
  selectedSet.forEach(m => { totalRevenue += _monthlyRevenue[m] || 0; });
  const tacos = totalRevenue > 0 ? (totalSpend / totalRevenue * 100).toFixed(1) + '%' : '--';
  const acos = totalSales > 0 ? (totalSpend / totalSales * 100).toFixed(1) + '%' : '--';
  const roas = totalSpend > 0 ? (totalSales / totalSpend).toFixed(2) + 'x' : '--';
  const acosColor = totalSales > 0 && totalSpend/totalSales <= 0.25 ? 'var(--green)' : totalSales > 0 && totalSpend/totalSales <= 0.40 ? 'var(--yellow)' : 'var(--red)';
  const grid = document.getElementById('ads-kpi-grid');
  grid.innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Total Ad Spend</div>
      <div class="kpi-l52">${fmt$(totalSpend)}</div>
      <div class="kpi-p52">${filtered.length} month${filtered.length!==1?'s':''}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Total Ad Sales</div>
      <div class="kpi-l52">${fmt$(totalSales)}</div>
      <div class="kpi-p52">${fmtN(totalOrders)} orders</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Blended ACOS</div>
      <div class="kpi-l52" style="color:${acosColor}">${acos}</div>
      <div class="kpi-p52">Ad Spend / Ad Sales</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">TACOS</div>
      <div class="kpi-l52" style="color:var(--blue)">${tacos}</div>
      <div class="kpi-p52">${fmt$(totalSpend)} spend / ${fmt$(totalRevenue)} revenue</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Blended ROAS</div>
      <div class="kpi-l52">${roas}</div>
      <div class="kpi-p52">${fmtN(totalClicks)} clicks | ${fmtN(totalImpressions)} impr.</div>
    </div>
  `;
}

let _adsStrategyByTerm = {};
let _adsKwTypeByTerm = {};

async function loadAdvertising() {
  // Ensure monthly SQP data is loaded (shared with Keywords tab)
  if (!kwAllMonths.length) {
    const monthly = await fetch('/api/keywords/monthly').then(r => r.json());
    const monthSet = new Set();
    for (const row of monthly) {
      if (!kwMonthlyData[row.search_query]) kwMonthlyData[row.search_query] = [];
      kwMonthlyData[row.search_query].push(row);
      monthSet.add(row.month);
    }
    kwAllMonths = [...monthSet].sort();
  }

  // Fetch strategy/keyword_type from keyword_targets (existing endpoint)
  const ktData = await fetch('/api/keywords?limit=5000').then(r => r.json());
  _adsStrategyByTerm = {};
  _adsKwTypeByTerm = {};
  ktData.forEach(r => {
    const t = (r.search_query || '').toLowerCase();
    _adsStrategyByTerm[t] = r.strategy || null;
    _adsKwTypeByTerm[t] = r.keyword_type || null;
  });

  // Fetch monthly ad data + monthly summary + revenue in parallel
  const [adsMonthly, summaryMonthly, revData] = await Promise.all([
    fetch('/api/ads/keywords/monthly').then(r => r.json()),
    fetch('/api/ads/summary/monthly').then(r => r.json()),
    fetch('/api/revenue-by-month').then(r => r.json()),
  ]);

  // Build monthly ad data lookup by term
  _adsMonthlyByTerm = {};
  const adsMonthSet = new Set();
  for (const row of adsMonthly) {
    if (!_adsMonthlyByTerm[row.search_term]) _adsMonthlyByTerm[row.search_term] = [];
    _adsMonthlyByTerm[row.search_term].push(row);
    adsMonthSet.add(row.month);
  }
  _adsAllMonths = [...adsMonthSet].sort();

  _adsSummaryMonthly = summaryMonthly;

  _monthlyRevenue = {};
  revData.forEach(r => { _monthlyRevenue[r.month] = r.revenue || 0; });

  _initAdsHorizons();
  recomputeAdsOrganic();
  recomputeAdsSummary();
  filterAdsKeywords();
}

function filterAdsKeywords() {
  const search = (document.getElementById('ads-kw-search')?.value || '').toLowerCase();
  const stratEl = document.querySelector('#ads-strategy-chips .chip.active');
  const strategy = stratEl ? stratEl.dataset.adstrat : 'All';
  const typeEl = document.querySelector('#ads-adtype-chips .chip.active');
  const adType = typeEl ? typeEl.dataset.adtype : 'All';
  const minClicks = parseInt(document.getElementById('ads-clicks-filter')?.value || '0', 10);
  const minSqpClicks = parseInt(document.getElementById('sqp-clicks-filter')?.value || '0', 10);

  const filtered = _adsKwData.filter(r => {
    if (search && !(r.search_term||'').toLowerCase().includes(search)) return false;
    if (strategy !== 'All') {
      if (strategy === '') { if (r.strategy) return false; }
      else { if (r.strategy !== strategy) return false; }
    }
    if (adType !== 'All' && !(r.ad_type_list||'').includes(adType)) return false;
    if (minClicks > 0 && (r.ad_clicks || 0) < minClicks) return false;
    if (minSqpClicks > 0 && (r.sqp_brand_clicks || 0) < minSqpClicks) return false;
    return true;
  });

  // Client-side sort
  filtered.sort((a, b) => {
    let av = a[adsSortCol], bv = b[adsSortCol];
    if (av == null) av = adsSortDir === -1 ? -Infinity : Infinity;
    if (bv == null) bv = adsSortDir === -1 ? -Infinity : Infinity;
    if (typeof av === 'string') return adsSortDir * av.localeCompare(bv);
    return adsSortDir * (av - bv);
  });

  document.getElementById('ads-filter-count').textContent = `${filtered.length} of ${_adsKwData.length}`;

  function acosFmt(v) {
    if (v == null) return '--';
    const pct = (v * 100).toFixed(1);
    const color = v <= 0.20 ? 'var(--green)' : v <= 0.35 ? 'var(--yellow)' : 'var(--red)';
    return `<span style="color:${color};font-weight:600">${pct}%</span>`;
  }

  document.getElementById('ads-kw-tbody').innerHTML = filtered.map(r => `
    <tr class="kw-row" style="cursor:pointer" onclick="drillAdsKeyword('${(r.search_term||'').replace(/'/g,"\\'")}',this)">
      <td>${r.search_term||''}</td>
      <td><span style="color:var(--muted);font-size:0.72rem">${r.ad_type_list||''}</span></td>
      <td class="num">${r.ad_spend!=null?fmt$(r.ad_spend):'--'}</td>
      <td class="num">${r.ad_clicks!=null?fmtN(r.ad_clicks):'--'}</td>
      <td class="num">${r.ad_sales!=null?fmt$(r.ad_sales):'--'}</td>
      <td class="num">${acosFmt(r.ad_acos)}</td>
      <td class="num">${r.ad_roas!=null?`<span style="font-weight:600">${r.ad_roas.toFixed(2)}x</span>`:'--'}</td>
      <td class="num">${r.num_campaigns||''}</td>
      <td class="num">${r.best_impression_share!=null?r.best_impression_share.toFixed(1)+'%':'--'}</td>
      <td>${strategyBadge(r.strategy)}</td>
      <td class="num">${r.organic_volume!=null?fmtN(r.organic_volume):'--'}</td>
      <td class="num">${r.organic_units!=null?fmtN(r.organic_units):'--'}</td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
    </tr>
  `).join('');

  // Attach header sort handlers
  document.querySelectorAll('#ads-kw-table th[data-adcol]').forEach(th => {
    th.style.cursor = 'pointer';
    th.onclick = () => sortAdsTable(th.dataset.adcol);
  });

  // Hide drill-downs when filtering
  document.getElementById('ads-product-detail').classList.remove('visible');
  document.getElementById('ads-campaign-detail').classList.remove('visible');
}

async function drillAdsKeyword(term, rowEl) {
  _adsCurrentTerm = term;
  document.querySelectorAll('#ads-kw-table .kw-row').forEach(r => r.classList.remove('kw-row-selected'));
  if (rowEl) rowEl.classList.add('kw-row-selected');

  const panel = document.getElementById('ads-product-detail');
  document.getElementById('ads-product-header').textContent = `ASINs bidding on: "${term}"`;
  panel.classList.add('visible');
  document.getElementById('ads-campaign-detail').classList.remove('visible');

  const res = await fetch(`/api/ads/keyword/${encodeURIComponent(term)}/products`);
  const data = await res.json();

  function roleBadge(role) {
    if (!role) return '--';
    const colors = {core:'var(--green)',supporting:'var(--blue)',opportunistic:'var(--yellow)'};
    return `<span style="color:${colors[role]||'var(--muted)'};font-size:0.72rem;font-weight:600">${role}</span>`;
  }

  document.getElementById('ads-product-tbody').innerHTML = data.length ? data.map(r => `
    <tr class="kw-row" style="cursor:pointer" onclick="drillAdsCampaigns('${(term).replace(/'/g,"\\'")}','${r.asin}',this)">
      <td><span class="asin-link">${r.asin||''}</span></td>
      <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${(r.product_name||'').replace(/"/g,'&quot;')}">${r.product_name||'--'}</td>
      <td class="num">${r.ad_spend!=null?fmt$(r.ad_spend):'--'}</td>
      <td class="num">${r.ad_sales!=null?fmt$(r.ad_sales):'--'}</td>
      <td class="num">${r.ad_acos!=null?`<span style="color:${r.ad_acos<=0.20?'var(--green)':r.ad_acos<=0.35?'var(--yellow)':'var(--red)'};font-weight:600">${(r.ad_acos*100).toFixed(1)}%</span>`:'--'}</td>
      <td class="num">${r.num_campaigns||''}</td>
      <td>${roleBadge(r.keyword_role)}</td>
      <td class="num">${r.organic_purchase_share!=null?fmtP(r.organic_purchase_share*100):'--'}</td>
      <td class="num">${cvrIndexFmt(r.cvr_index)}</td>
    </tr>
  `).join('') : '<tr><td colspan="9" style="text-align:center;color:var(--dim)">No ASIN-level data</td></tr>';
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

async function drillAdsCampaigns(term, asin, rowEl) {
  document.querySelectorAll('#ads-product-table .kw-row').forEach(r => r.classList.remove('kw-row-selected'));
  if (rowEl) rowEl.classList.add('kw-row-selected');

  const panel = document.getElementById('ads-campaign-detail');
  document.getElementById('ads-campaign-header').textContent = `Campaigns: "${term}" \u2192 ${asin}`;
  panel.classList.add('visible');

  const res = await fetch(`/api/ads/keyword/${encodeURIComponent(term)}/product/${asin}/campaigns`);
  const data = await res.json();

  document.getElementById('ads-campaign-tbody').innerHTML = data.length ? data.map(r => `
    <tr>
      <td style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${(r.campaign_name||'').replace(/"/g,'&quot;')}">${r.campaign_name||''}</td>
      <td><span style="color:var(--muted);font-size:0.72rem">${r.ad_type||''}</span></td>
      <td><span style="color:var(--muted);font-size:0.72rem">${r.match_type||'--'}</span></td>
      <td class="num">${r.spend!=null?fmt$(r.spend):'--'}</td>
      <td class="num">${r.cpc!=null?'$'+r.cpc.toFixed(2):'--'}</td>
      <td class="num">${r.clicks!=null?fmtN(r.clicks):'--'}</td>
      <td class="num">${r.orders!=null?fmtN(r.orders):'--'}</td>
      <td class="num">${r.acos!=null?`<span style="color:${r.acos<=0.20?'var(--green)':r.acos<=0.35?'var(--yellow)':'var(--red)'};font-weight:600">${(r.acos*100).toFixed(1)}%</span>`:'--'}</td>
      <td class="num">${r.impression_share!=null?r.impression_share.toFixed(1)+'%':'--'}</td>
    </tr>
  `).join('') : '<tr><td colspan="9" style="text-align:center;color:var(--dim)">No campaign data found</td></tr>';
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

loaded['overview'] = true;
loadOverview();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5052))
    print(f"Nire Beauty Analytics Dashboard -> http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)
