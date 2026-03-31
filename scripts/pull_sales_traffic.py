"""
pull_sales_traffic.py — Fetch daily brand-level sales & traffic data.

Source: GET_SALES_AND_TRAFFIC_REPORT (daily granularity, no ASIN breakdown)
Target: sales_traffic_daily table

The existing backfill.py pulls monthly ASIN-level data into sales_and_traffic.
This script adds daily brand-level data for the dashboard's trend charts.

Usage:
    python -m scripts.pull_sales_traffic
"""

import json
from datetime import datetime

from auth import validate
from config import PULL_MONTHS, month_to_period
from schema import get_conn
from scripts.api_client import download_report


def pull_sales_traffic():
    validate()
    print(f"Pulling daily sales & traffic ({len(PULL_MONTHS)} months)...")

    conn = get_conn()
    cur = conn.cursor()

    for start, end, label in PULL_MONTHS:
        print(f"  {label}: {start} to {end}")

        try:
            content = download_report(
                report_type="GET_SALES_AND_TRAFFIC_REPORT",
                start=start,
                end=end,
                report_options={
                    "dateGranularity": "DAY",
                    "asinGranularity": "SKU",
                },
            )
        except Exception as exc:
            print(f"    SKIP — {exc}")
            continue

        data = json.loads(content)
        daily_entries = data.get("salesAndTrafficByDate", [])
        if not daily_entries:
            print(f"    No daily data for {label}")
            continue

        now = datetime.now().isoformat()
        rows = []
        for day in daily_entries:
            d = day.get("date", "")
            traffic = day.get("trafficByDate", {})
            sales = day.get("salesByDate", {})
            revenue_obj = sales.get("orderedProductSales", {})

            rows.append((
                d,
                month_to_period(d[:7]) if len(d) >= 7 else "other",
                float(revenue_obj.get("amount", 0) or 0),
                int(sales.get("unitsOrdered", 0) or 0),
                int(sales.get("totalOrderItems", 0) or 0),
                int(sales.get("unitsRefunded", 0) or 0),
                int(traffic.get("sessions", 0) or 0),
                int(traffic.get("pageViews", 0) or 0),
                float(traffic.get("buyBoxPercentage", 0) or 0),
                float(traffic.get("unitSessionPercentage", 0) or 0),
                now,
            ))

        cur.executemany(
            """
            INSERT INTO sales_traffic_daily
                (date, period, revenue, units, total_order_items, units_refunded,
                 sessions, page_views, buy_box_pct, conversion_rate, pulled_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                period           = EXCLUDED.period,
                revenue          = EXCLUDED.revenue,
                units            = EXCLUDED.units,
                total_order_items= EXCLUDED.total_order_items,
                units_refunded   = EXCLUDED.units_refunded,
                sessions         = EXCLUDED.sessions,
                page_views       = EXCLUDED.page_views,
                buy_box_pct      = EXCLUDED.buy_box_pct,
                conversion_rate  = EXCLUDED.conversion_rate,
                pulled_at        = EXCLUDED.pulled_at
            """,
            rows,
        )
        conn.commit()
        print(f"    {len(rows)} daily rows inserted")

    cur.close()
    conn.close()
    print("Daily sales & traffic pull complete.")


if __name__ == "__main__":
    pull_sales_traffic()
