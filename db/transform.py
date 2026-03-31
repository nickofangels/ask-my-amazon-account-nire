"""
transform.py — Build derived dashboard tables from raw Supabase tables.

This is NOT a port of DWC's load.py (which reads JSON files from disk).
Instead, it reads from the 5 raw tables that backfill.py populates and
writes to the derived tables the dashboard queries.

Transforms:
  1. sales_and_traffic     → sales_traffic_asin   (rename cols, derive month, assign period)
  2. sqp_report            → search_query_performance (add month, period, compute shares)
  3. search_catalog_performance → catalog_performance (rename cols, add month, period)
  4. Populate period_meta   from config.PERIOD_META
  5. Populate data_coverage by counting rows per table/month

Usage:
    python -m db.transform
"""

from datetime import datetime

from config import month_to_period, PERIOD_META
from schema import get_conn


def _derive_month_sql():
    """SQL expression to derive YYYY-MM from start_date."""
    return "TO_CHAR(start_date::DATE, 'YYYY-MM')"


def transform_sales_traffic_asin(conn):
    """Raw sales_and_traffic → derived sales_traffic_asin.

    Renames: ordered_product_sales→revenue, units_ordered→units,
             buy_box_percentage→buy_box_pct, unit_session_percentage→conversion_rate
    Derives: month from start_date
    """
    cur = conn.cursor()
    print("  sales_and_traffic → sales_traffic_asin")

    # Get distinct months from raw data
    cur.execute(f"SELECT DISTINCT {_derive_month_sql()} AS month FROM sales_and_traffic ORDER BY month")
    months = [r[0] for r in cur.fetchall()]

    total = 0
    for month in months:
        period = month_to_period(month)
        now = datetime.now().isoformat()

        cur.execute(f"""
            INSERT INTO sales_traffic_asin
                (month, asin, parent_asin, period, units, revenue,
                 sessions, page_views, conversion_rate, buy_box_pct, pulled_at)
            SELECT
                {_derive_month_sql()} AS month,
                asin,
                MAX(parent_asin) AS parent_asin,
                %s AS period,
                SUM(COALESCE(units_ordered, 0)) AS units,
                SUM(COALESCE(ordered_product_sales, 0)) AS revenue,
                SUM(COALESCE(sessions, 0)) AS sessions,
                SUM(COALESCE(page_views, 0)) AS page_views,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(AVG(COALESCE(unit_session_percentage, 0))::NUMERIC, 2)
                     ELSE 0 END AS conversion_rate,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(AVG(COALESCE(buy_box_percentage, 0))::NUMERIC, 2)
                     ELSE 0 END AS buy_box_pct,
                %s AS pulled_at
            FROM sales_and_traffic
            WHERE {_derive_month_sql()} = %s
              AND asin != ''
            GROUP BY {_derive_month_sql()}, asin
            ON CONFLICT (month, asin) DO UPDATE SET
                parent_asin     = EXCLUDED.parent_asin,
                period          = EXCLUDED.period,
                units           = EXCLUDED.units,
                revenue         = EXCLUDED.revenue,
                sessions        = EXCLUDED.sessions,
                page_views      = EXCLUDED.page_views,
                conversion_rate = EXCLUDED.conversion_rate,
                buy_box_pct     = EXCLUDED.buy_box_pct,
                pulled_at       = EXCLUDED.pulled_at
        """, (period, now, month))

        total += cur.rowcount

    conn.commit()
    print(f"    {total} rows across {len(months)} months")
    cur.close()


def transform_search_query_performance(conn):
    """Raw sqp_report → derived search_query_performance.

    Adds: month, period
    Computes: total_click_rate, asin_cart_add_share, asin_purchase_share
    (these fields exist in DWC's schema but backfill.py doesn't store them)
    """
    cur = conn.cursor()
    print("  sqp_report → search_query_performance")

    cur.execute(f"SELECT DISTINCT {_derive_month_sql()} AS month FROM sqp_report ORDER BY month")
    months = [r[0] for r in cur.fetchall()]

    total = 0
    for month in months:
        period = month_to_period(month)
        now = datetime.now().isoformat()

        cur.execute(f"""
            INSERT INTO search_query_performance
                (month, asin, search_query, search_query_score, search_query_volume,
                 total_impressions, asin_impressions, asin_impression_share,
                 total_clicks, total_click_rate, asin_clicks, asin_click_share,
                 total_cart_adds, asin_cart_adds, asin_cart_add_share,
                 total_purchases, asin_purchases, asin_purchase_share,
                 period, pulled_at)
            SELECT
                {_derive_month_sql()} AS month,
                asin,
                search_query,
                search_query_score,
                search_query_volume,
                total_impressions,
                asin_impressions,
                asin_impression_share,
                total_clicks,
                CASE WHEN COALESCE(total_impressions, 0) > 0
                     THEN ROUND((total_clicks::FLOAT / total_impressions * 100)::NUMERIC, 4)
                     ELSE 0 END AS total_click_rate,
                asin_clicks,
                asin_click_share,
                total_cart_adds,
                asin_cart_adds,
                CASE WHEN COALESCE(total_cart_adds, 0) > 0
                     THEN ROUND((asin_cart_adds::FLOAT / total_cart_adds * 100)::NUMERIC, 4)
                     ELSE 0 END AS asin_cart_add_share,
                total_purchases,
                asin_purchases,
                CASE WHEN COALESCE(total_purchases, 0) > 0
                     THEN ROUND((asin_purchases::FLOAT / total_purchases * 100)::NUMERIC, 4)
                     ELSE 0 END AS asin_purchase_share,
                %s AS period,
                %s AS pulled_at
            FROM sqp_report
            WHERE {_derive_month_sql()} = %s
            ON CONFLICT (month, asin, search_query) DO UPDATE SET
                search_query_score    = EXCLUDED.search_query_score,
                search_query_volume   = EXCLUDED.search_query_volume,
                total_impressions     = EXCLUDED.total_impressions,
                asin_impressions      = EXCLUDED.asin_impressions,
                asin_impression_share = EXCLUDED.asin_impression_share,
                total_clicks          = EXCLUDED.total_clicks,
                total_click_rate      = EXCLUDED.total_click_rate,
                asin_clicks           = EXCLUDED.asin_clicks,
                asin_click_share      = EXCLUDED.asin_click_share,
                total_cart_adds       = EXCLUDED.total_cart_adds,
                asin_cart_adds        = EXCLUDED.asin_cart_adds,
                asin_cart_add_share   = EXCLUDED.asin_cart_add_share,
                total_purchases       = EXCLUDED.total_purchases,
                asin_purchases        = EXCLUDED.asin_purchases,
                asin_purchase_share   = EXCLUDED.asin_purchase_share,
                period                = EXCLUDED.period,
                pulled_at             = EXCLUDED.pulled_at
        """, (period, now, month))

        total += cur.rowcount

    conn.commit()
    print(f"    {total} rows across {len(months)} months")
    cur.close()


def transform_catalog_performance(conn):
    """Raw search_catalog_performance → derived catalog_performance.

    Renames: impression_count→impressions, click_count→clicks,
             cart_add_count→cart_adds, purchase_count→purchases
    Adds: month, period
    """
    cur = conn.cursor()
    print("  search_catalog_performance → catalog_performance")

    cur.execute(f"""
        SELECT DISTINCT {_derive_month_sql()} AS month
        FROM search_catalog_performance ORDER BY month
    """)
    months = [r[0] for r in cur.fetchall()]

    total = 0
    for month in months:
        period = month_to_period(month)
        now = datetime.now().isoformat()

        cur.execute(f"""
            INSERT INTO catalog_performance
                (month, asin, impressions, clicks, click_rate, cart_adds,
                 purchases, conversion_rate, search_traffic_sales, period, pulled_at)
            SELECT
                {_derive_month_sql()} AS month,
                asin,
                COALESCE(impression_count, 0) AS impressions,
                COALESCE(click_count, 0) AS clicks,
                COALESCE(click_rate, 0) AS click_rate,
                COALESCE(cart_add_count, 0) AS cart_adds,
                COALESCE(purchase_count, 0) AS purchases,
                COALESCE(conversion_rate, 0) AS conversion_rate,
                COALESCE(search_traffic_sales, 0) AS search_traffic_sales,
                %s AS period,
                %s AS pulled_at
            FROM search_catalog_performance
            WHERE {_derive_month_sql()} = %s
            ON CONFLICT (month, asin) DO UPDATE SET
                impressions          = EXCLUDED.impressions,
                clicks               = EXCLUDED.clicks,
                click_rate           = EXCLUDED.click_rate,
                cart_adds            = EXCLUDED.cart_adds,
                purchases            = EXCLUDED.purchases,
                conversion_rate      = EXCLUDED.conversion_rate,
                search_traffic_sales = EXCLUDED.search_traffic_sales,
                period               = EXCLUDED.period,
                pulled_at            = EXCLUDED.pulled_at
        """, (period, now, month))

        total += cur.rowcount

    conn.commit()
    print(f"    {total} rows across {len(months)} months")
    cur.close()


def populate_period_meta(conn):
    """Write L52/P52 period boundaries from config.PERIOD_META."""
    cur = conn.cursor()
    print("  Populating period_meta")

    for period, meta in PERIOD_META.items():
        cur.execute("""
            INSERT INTO period_meta (period, start_date, end_date, label)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (period) DO UPDATE SET
                start_date = EXCLUDED.start_date,
                end_date   = EXCLUDED.end_date,
                label      = EXCLUDED.label
        """, (period, meta["start_date"], meta["end_date"], meta["label"]))

    conn.commit()
    print(f"    {len(PERIOD_META)} periods written")
    cur.close()


def populate_data_coverage(conn):
    """Count rows per table/month and write to data_coverage."""
    cur = conn.cursor()
    print("  Populating data_coverage")

    # Tables to check: (data_type label, table name, date column)
    sources = [
        ("sales_traffic_asin",       "sales_traffic_asin",       "month"),
        ("search_query_performance", "search_query_performance", "month"),
        ("catalog_performance",      "catalog_performance",      "month"),
        ("sales_traffic_daily",      "sales_traffic_daily",      "LEFT(date, 7)"),
        ("search_terms",             "search_terms",             "month"),
    ]

    now = datetime.now().isoformat()
    total = 0

    for data_type, table, month_expr in sources:
        try:
            cur.execute(f"""
                SELECT {month_expr} AS month, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {month_expr}
                ORDER BY month
            """)
            for row in cur.fetchall():
                month_val, cnt = row[0], row[1]
                period = month_to_period(month_val)
                cur.execute("""
                    INSERT INTO data_coverage (data_type, month, period, row_count, is_complete)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (data_type, month) DO UPDATE SET
                        period    = EXCLUDED.period,
                        row_count = EXCLUDED.row_count
                """, (data_type, month_val, period, cnt, cnt > 0))
                total += 1
        except Exception as exc:
            conn.rollback()
            print(f"    Skipping {data_type}: {exc}")

    conn.commit()
    print(f"    {total} coverage entries written")
    cur.close()


def run_all():
    """Run all transforms."""
    print("Running transforms...")
    conn = get_conn()

    transform_sales_traffic_asin(conn)
    transform_search_query_performance(conn)
    transform_catalog_performance(conn)
    populate_period_meta(conn)
    populate_data_coverage(conn)

    conn.close()
    print("All transforms complete.")


if __name__ == "__main__":
    run_all()
