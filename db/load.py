"""
db/load.py — Load all raw SP-API data from disk into Supabase (PostgreSQL).

Reads raw/*.json and raw/listings/listings.tsv and upserts every row into
the database using INSERT ... ON CONFLICT DO UPDATE so this script is safe
to re-run repeatedly (idempotent).

Port of DWC's db/load.py adapted for PostgreSQL + Nire's data shapes.

Usage:
    python -m db.load                       # load everything
    python -m db.load --only sales          # load only sales/traffic
    python -m db.load --only sqp            # load only SQP
    python -m db.load --only catalog        # load only catalog performance
    python -m db.load --only repeat         # load only repeat purchase
    python -m db.load --only basket         # load only market basket
    python -m db.load --only search_terms   # load only search terms
    python -m db.load --only listings       # load only listings
    python -m db.load --month 2025-06       # reload one specific month
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from datetime import datetime, date, timezone
from pathlib import Path

import psycopg2.extras

# Ensure project root is on the path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import (
    SALES_TRAFFIC_DIR,
    SEARCH_TERMS_DIR,
    SEARCH_QUERY_PERF_DIR,
    CATALOG_PERF_DIR,
    MARKET_BASKET_DIR,
    REPEAT_PURCHASE_DIR,
    LISTINGS_TSV,
    month_to_period,
    PERIOD_META,
)
from schema import get_conn, init_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _upsert(conn, sql: str, rows: list[tuple], page_size: int = 500) -> None:
    """Batch upsert using execute_values for PostgreSQL performance."""
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
    conn.commit()


def _log_pull(conn, report_type: str, month: str | None, source_file: str, row_count: int, now: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pull_log (report_type, month, source_file, row_count, pulled_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (report_type, month, source_file, row_count, now),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Sales & Traffic loader
# ---------------------------------------------------------------------------

def load_sales_traffic(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(SALES_TRAFFIC_DIR.glob("*.json"))
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [sales_traffic] No files found in {SALES_TRAFFIC_DIR}")
        return

    total_asin = 0

    for json_file in files:
        month = json_file.stem

        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [sales_traffic] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        # ---- Per-ASIN monthly rows ------------------------------------------
        # Only load ASIN rows if the file has CHILD granularity.
        # PARENT granularity gives aggregated parent-level data that would
        # be confusing if mixed with child-level rows in the same table.
        asin_entries = data.get("salesAndTrafficByAsin", [])
        is_child_granularity = bool(asin_entries) and "childAsin" in asin_entries[0]

        if asin_entries and not is_child_granularity:
            print(f"  {month}: WARNING — PARENT granularity, skipping ASIN rows (re-pull with CHILD)")

        asin_agg: dict[str, dict] = {}
        for asin_entry in (asin_entries if is_child_granularity else []):
            child = str(asin_entry.get("childAsin", "")).strip().upper()
            parent = str(asin_entry.get("parentAsin", "")).strip().upper()
            asin = child
            if not asin:
                continue

            sales = asin_entry.get("salesByAsin", {})
            traffic = asin_entry.get("trafficByAsin", {})
            ops = sales.get("orderedProductSales") or {}

            units = int(sales.get("unitsOrdered", 0) or 0)
            revenue = float(ops.get("amount", 0) or 0)
            sessions = int(traffic.get("sessions", 0) or 0)
            pvs = int(traffic.get("pageViews", 0) or 0)
            cvr = float(traffic.get("unitSessionPercentage", 0) or 0)
            bb = float(traffic.get("buyBoxPercentage", 0) or 0)

            if asin not in asin_agg:
                asin_agg[asin] = {
                    "parent": parent or None,
                    "units": 0, "revenue": 0.0, "sessions": 0, "pvs": 0,
                    "cvr_sum": 0.0, "bb_sum": 0.0, "count": 0,
                }
            a = asin_agg[asin]
            a["units"] += units
            a["revenue"] += revenue
            a["sessions"] += sessions
            a["pvs"] += pvs
            a["cvr_sum"] += cvr
            a["bb_sum"] += bb
            a["count"] += 1

        asin_rows = []
        for asin, a in asin_agg.items():
            n = a["count"]
            asin_rows.append((
                month, asin, a["parent"], month_to_period(month),
                a["units"], a["revenue"], a["sessions"], a["pvs"],
                round(a["cvr_sum"] / n, 2) if n else 0.0,
                round(a["bb_sum"] / n, 2) if n else 0.0,
                now,
            ))

        if asin_rows:
            _upsert(conn,
                """INSERT INTO sales_traffic_asin
                   (month, asin, parent_asin, period, units, revenue,
                    sessions, page_views, conversion_rate, buy_box_pct, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, asin) DO UPDATE SET
                    parent_asin=EXCLUDED.parent_asin, period=EXCLUDED.period,
                    units=EXCLUDED.units, revenue=EXCLUDED.revenue,
                    sessions=EXCLUDED.sessions, page_views=EXCLUDED.page_views,
                    conversion_rate=EXCLUDED.conversion_rate, buy_box_pct=EXCLUDED.buy_box_pct,
                    pulled_at=EXCLUDED.pulled_at""",
                asin_rows,
            )
            _log_pull(conn, "sales_traffic_asin", month, json_file.name, len(asin_rows), now)
            total_asin += len(asin_rows)

        print(f"  {month}: {len(asin_rows)} ASIN rows")

    print(f"  -> Loaded {total_asin:,} ASIN rows")


# ---------------------------------------------------------------------------
# Search Terms loader
# ---------------------------------------------------------------------------

def load_search_terms(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(SEARCH_TERMS_DIR.glob("*.json"))
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [search_terms] No files found in {SEARCH_TERMS_DIR}")
        return

    # Get active ASINs for filtering (search_terms files are unfiltered marketplace data)
    try:
        from config import get_active_asins
        active_asins = set(a.upper() for a in get_active_asins())
        print(f"  [search_terms] Filtering to {len(active_asins)} active ASINs")
    except Exception as exc:
        print(f"  [search_terms] WARNING: Could not get active ASINs ({exc}), loading all rows")
        active_asins = None

    total = 0

    for json_file in files:
        month = json_file.stem
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [search_terms] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        items = data.get("dataByDepartmentAndSearchTerm", [])
        rows = []
        for item in items:
            search_term = str(item.get("searchTerm", "")).strip()
            clicked_asin = str(item.get("clickedAsin", "")).strip().upper()
            if not search_term or not clicked_asin:
                continue
            # Filter to brand ASINs if we have the list
            if active_asins is not None and clicked_asin not in active_asins:
                continue
            rows.append((
                month,
                search_term,
                int(item.get("searchFrequencyRank", 0) or 0),
                clicked_asin,
                str(item.get("clickedItemName", "")),
                int(item.get("clickShareRank", 1) or 1),
                float(item.get("clickShare", 0) or 0),
                float(item.get("conversionShare", 0) or 0),
                month_to_period(month),
                now,
            ))

        if rows:
            _upsert(conn,
                """INSERT INTO search_terms
                   (month, search_term, search_freq_rank, clicked_asin, product_title,
                    click_share_rank, click_share, conversion_share, period, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, search_term, clicked_asin) DO UPDATE SET
                    search_freq_rank=EXCLUDED.search_freq_rank, product_title=EXCLUDED.product_title,
                    click_share_rank=EXCLUDED.click_share_rank, click_share=EXCLUDED.click_share,
                    conversion_share=EXCLUDED.conversion_share, period=EXCLUDED.period,
                    pulled_at=EXCLUDED.pulled_at""",
                rows,
            )
            _log_pull(conn, "search_terms", month, json_file.name, len(rows), now)
            total += len(rows)
            print(f"  {month}: {len(rows)} rows (from {len(items):,} total)")
        else:
            print(f"  {month}: 0 rows (from {len(items):,} total)")

    print(f"  -> Loaded {total:,} search term rows")


# ---------------------------------------------------------------------------
# Search Query Performance loader
# ---------------------------------------------------------------------------

def load_search_query_perf(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(SEARCH_QUERY_PERF_DIR.glob("*.json"))
    # Skip old test files
    files = [f for f in files if not f.name.startswith("old_test")]
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [sqp] No files found in {SEARCH_QUERY_PERF_DIR}")
        return

    total = 0
    for json_file in files:
        month = json_file.stem
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [sqp] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        rows = []
        for entry in data.get("dataByAsin", []):
            asin = str(entry.get("asin", "")).strip().upper()
            sqd = entry.get("searchQueryData", {})
            imp = entry.get("impressionData", {})
            clk = entry.get("clickData", {})
            cart = entry.get("cartAddData", {})
            pur = entry.get("purchaseData", {})
            query = str(sqd.get("searchQuery", "")).strip()
            if not asin or not query:
                continue
            rows.append((
                month, asin, query,
                int(sqd.get("searchQueryScore", 0) or 0),
                int(sqd.get("searchQueryVolume", 0) or 0),
                int(imp.get("totalQueryImpressionCount", 0) or 0),
                int(imp.get("asinImpressionCount", 0) or 0),
                float(imp.get("asinImpressionShare", 0) or 0),
                int(clk.get("totalClickCount", 0) or 0),
                float(clk.get("totalClickRate", 0) or 0),
                int(clk.get("asinClickCount", 0) or 0),
                float(clk.get("asinClickShare", 0) or 0),
                int(cart.get("totalCartAddCount", 0) or 0),
                int(cart.get("asinCartAddCount", 0) or 0),
                float(cart.get("asinCartAddShare", 0) or 0),
                int(pur.get("totalPurchaseCount", 0) or 0),
                int(pur.get("asinPurchaseCount", 0) or 0),
                float(pur.get("asinPurchaseShare", 0) or 0),
                month_to_period(month),
                now,
            ))

        if rows:
            _upsert(conn,
                """INSERT INTO search_query_performance
                   (month, asin, search_query, search_query_score, search_query_volume,
                    total_impressions, asin_impressions, asin_impression_share,
                    total_clicks, total_click_rate, asin_clicks, asin_click_share,
                    total_cart_adds, asin_cart_adds, asin_cart_add_share,
                    total_purchases, asin_purchases, asin_purchase_share,
                    period, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, asin, search_query) DO UPDATE SET
                    search_query_score=EXCLUDED.search_query_score,
                    search_query_volume=EXCLUDED.search_query_volume,
                    total_impressions=EXCLUDED.total_impressions,
                    asin_impressions=EXCLUDED.asin_impressions,
                    asin_impression_share=EXCLUDED.asin_impression_share,
                    total_clicks=EXCLUDED.total_clicks,
                    total_click_rate=EXCLUDED.total_click_rate,
                    asin_clicks=EXCLUDED.asin_clicks,
                    asin_click_share=EXCLUDED.asin_click_share,
                    total_cart_adds=EXCLUDED.total_cart_adds,
                    asin_cart_adds=EXCLUDED.asin_cart_adds,
                    asin_cart_add_share=EXCLUDED.asin_cart_add_share,
                    total_purchases=EXCLUDED.total_purchases,
                    asin_purchases=EXCLUDED.asin_purchases,
                    asin_purchase_share=EXCLUDED.asin_purchase_share,
                    period=EXCLUDED.period,
                    pulled_at=EXCLUDED.pulled_at""",
                rows,
            )
            _log_pull(conn, "search_query_performance", month, json_file.name, len(rows), now)
            total += len(rows)
            print(f"  {month}: {len(rows):,} rows")

    print(f"  -> Loaded {total:,} search query performance rows")


# ---------------------------------------------------------------------------
# Catalog Performance loader
# ---------------------------------------------------------------------------

def load_catalog_perf(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(CATALOG_PERF_DIR.glob("*.json"))
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [catalog_perf] No files found in {CATALOG_PERF_DIR}")
        return

    total = 0
    for json_file in files:
        month = json_file.stem
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [catalog_perf] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        rows = []
        for entry in data.get("dataByAsin", []):
            asin = str(entry.get("asin", "")).strip().upper()
            if not asin:
                continue
            imp = entry.get("impressionData", {})
            clk = entry.get("clickData", {})
            cart = entry.get("cartAddData", {})
            pur = entry.get("purchaseData", {})
            sales_amt = (pur.get("searchTrafficSales") or {}).get("amount", 0)
            rows.append((
                month, asin,
                int(imp.get("impressionCount", 0) or 0),
                int(clk.get("clickCount", 0) or 0),
                float(clk.get("clickRate", 0) or 0),
                int(cart.get("cartAddCount", 0) or 0),
                int(pur.get("purchaseCount", 0) or 0),
                float(pur.get("conversionRate", 0) or 0),
                float(sales_amt or 0),
                month_to_period(month),
                now,
            ))

        if rows:
            _upsert(conn,
                """INSERT INTO catalog_performance
                   (month, asin, impressions, clicks, click_rate, cart_adds,
                    purchases, conversion_rate, search_traffic_sales, period, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, asin) DO UPDATE SET
                    impressions=EXCLUDED.impressions, clicks=EXCLUDED.clicks,
                    click_rate=EXCLUDED.click_rate, cart_adds=EXCLUDED.cart_adds,
                    purchases=EXCLUDED.purchases, conversion_rate=EXCLUDED.conversion_rate,
                    search_traffic_sales=EXCLUDED.search_traffic_sales,
                    period=EXCLUDED.period, pulled_at=EXCLUDED.pulled_at""",
                rows,
            )
            _log_pull(conn, "catalog_performance", month, json_file.name, len(rows), now)
            total += len(rows)
            print(f"  {month}: {len(rows)} rows")

    print(f"  -> Loaded {total:,} catalog performance rows")


# ---------------------------------------------------------------------------
# Repeat Purchase loader
# ---------------------------------------------------------------------------

def load_repeat_purchase(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(REPEAT_PURCHASE_DIR.glob("*.json"))
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [repeat_purchase] No files found in {REPEAT_PURCHASE_DIR}")
        return

    total = 0
    for json_file in files:
        month = json_file.stem
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [repeat_purchase] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        rows = []
        for entry in data.get("dataByAsin", []):
            asin = str(entry.get("asin", "")).strip().upper()
            if not asin:
                continue
            rev_obj = entry.get("repeatPurchaseRevenue") or {}
            rows.append((
                month, asin,
                month_to_period(month),
                int(entry.get("orders", 0) or 0),
                int(entry.get("uniqueCustomers", 0) or 0),
                float(entry.get("repeatCustomersPctTotal", 0) or 0),
                float(rev_obj.get("amount", 0) or 0),
                float(entry.get("repeatPurchaseRevenuePctTotal", 0) or 0),
                now,
            ))

        if rows:
            _upsert(conn,
                """INSERT INTO repeat_purchase
                   (month, asin, period, orders, unique_customers, repeat_customers_pct,
                    repeat_purchase_revenue, repeat_purchase_revenue_pct, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, asin) DO UPDATE SET
                    period=EXCLUDED.period, orders=EXCLUDED.orders,
                    unique_customers=EXCLUDED.unique_customers,
                    repeat_customers_pct=EXCLUDED.repeat_customers_pct,
                    repeat_purchase_revenue=EXCLUDED.repeat_purchase_revenue,
                    repeat_purchase_revenue_pct=EXCLUDED.repeat_purchase_revenue_pct,
                    pulled_at=EXCLUDED.pulled_at""",
                rows,
            )
            _log_pull(conn, "repeat_purchase", month, json_file.name, len(rows), now)
            total += len(rows)
            print(f"  {month}: {len(rows)} rows")

    print(f"  -> Loaded {total:,} repeat purchase rows")


# ---------------------------------------------------------------------------
# Market Basket loader
# ---------------------------------------------------------------------------

def load_market_basket(conn, only_month: str | None = None) -> None:
    now = _now()
    files = sorted(MARKET_BASKET_DIR.glob("*.json"))
    if only_month:
        files = [f for f in files if f.stem == only_month]

    if not files:
        print(f"  [market_basket] No files found in {MARKET_BASKET_DIR}")
        return

    total = 0
    for json_file in files:
        month = json_file.stem
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [market_basket] WARNING: Cannot read {json_file.name}: {exc}")
            continue

        rows = []
        for entry in data.get("dataByAsin", []):
            asin = str(entry.get("asin", "")).strip().upper()
            pw_asin = str(entry.get("purchasedWithAsin", "")).strip().upper()
            if not asin or not pw_asin:
                continue
            rows.append((
                month, asin, pw_asin,
                int(entry.get("purchasedWithRank", 0) or 0),
                float(entry.get("combinationPct", 0) or 0),
                month_to_period(month),
                now,
            ))

        if rows:
            _upsert(conn,
                """INSERT INTO market_basket
                   (month, asin, purchased_with_asin, purchased_with_rank,
                    combination_pct, period, pulled_at)
                   VALUES %s
                   ON CONFLICT (month, asin, purchased_with_asin) DO UPDATE SET
                    purchased_with_rank=EXCLUDED.purchased_with_rank,
                    combination_pct=EXCLUDED.combination_pct,
                    period=EXCLUDED.period, pulled_at=EXCLUDED.pulled_at""",
                rows,
            )
            _log_pull(conn, "market_basket", month, json_file.name, len(rows), now)
            total += len(rows)
            print(f"  {month}: {len(rows)} rows")

    print(f"  -> Loaded {total:,} market basket rows")


# ---------------------------------------------------------------------------
# Listings loader
# ---------------------------------------------------------------------------

def load_listings(conn) -> None:
    now = _now()

    if not LISTINGS_TSV.exists():
        print(f"  [listings] {LISTINGS_TSV} not found — skipping")
        return

    try:
        content = LISTINGS_TSV.read_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    except Exception as exc:
        print(f"  [listings] ERROR reading listings.tsv: {exc}")
        return

    raw_rows = list(reader)
    if not raw_rows:
        print("  [listings] listings.tsv is empty")
        return

    def _norm(col: str) -> str:
        return col.strip().lower().replace(" ", "_").replace("-", "_")

    rename = {
        "asin1": "asin",
        "seller_sku": "sku",
        "item_name": "product_name",
        "price": "price",
        "quantity": "quantity",
        "open_date": "open_date",
        "status": "status",
        "fulfillment_channel": "fulfillment",
    }

    rows = []
    for raw in raw_rows:
        normed = {_norm(k): v for k, v in raw.items()}
        mapped = {rename.get(k, k): v for k, v in normed.items()}
        asin = str(mapped.get("asin", "")).strip().upper()
        sku = str(mapped.get("sku", "")).strip()
        if not asin:
            continue
        rows.append((
            asin,
            sku,
            str(mapped.get("product_name", "")).strip(),
            str(mapped.get("price", "")).strip(),
            str(mapped.get("quantity", "")).strip(),
            str(mapped.get("open_date", "")).strip(),
            str(mapped.get("status", "")).strip(),
            str(mapped.get("fulfillment", "")).strip(),
            now,
        ))

    if rows:
        _upsert(conn,
            """INSERT INTO listings
               (asin, sku, product_name, price, quantity, open_date, status, fulfillment, pulled_at)
               VALUES %s
               ON CONFLICT (asin, sku) DO UPDATE SET
                product_name=EXCLUDED.product_name, price=EXCLUDED.price,
                quantity=EXCLUDED.quantity, open_date=EXCLUDED.open_date,
                status=EXCLUDED.status, fulfillment=EXCLUDED.fulfillment,
                pulled_at=EXCLUDED.pulled_at""",
            rows,
        )
        _log_pull(conn, "listings", None, "listings.tsv", len(rows), now)
        unique_asins = len(set(r[0] for r in rows))
        print(f"  -> Loaded {len(rows):,} listing rows ({unique_asins} unique ASINs)")


# ---------------------------------------------------------------------------
# Period metadata & data coverage
# ---------------------------------------------------------------------------

def load_period_meta(conn) -> None:
    now = _now()
    with conn.cursor() as cur:
        for period, meta in PERIOD_META.items():
            cur.execute(
                """INSERT INTO period_meta (period, start_date, end_date, label)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (period) DO UPDATE SET
                    start_date=EXCLUDED.start_date, end_date=EXCLUDED.end_date,
                    label=EXCLUDED.label""",
                (period, meta["start_date"], meta["end_date"], meta["label"]),
            )
    conn.commit()
    print(f"  -> Period meta: {', '.join(PERIOD_META.keys())}")


def load_data_coverage(conn) -> None:
    """Count rows per table/month and populate data_coverage."""
    tables = [
        ("sales_traffic_asin", "month"),
        ("search_query_performance", "month"),
        ("catalog_performance", "month"),
        ("repeat_purchase", "month"),
        ("market_basket", "month"),
        ("search_terms", "month"),
    ]
    with conn.cursor() as cur:
        for table, month_col in tables:
            cur.execute(f"""
                SELECT {month_col}, COUNT(*) FROM {table}
                GROUP BY {month_col} ORDER BY {month_col}
            """)
            for month, count in cur.fetchall():
                period = month_to_period(month)
                cur.execute(
                    """INSERT INTO data_coverage (data_type, month, period, row_count, is_complete)
                       VALUES (%s, %s, %s, %s, TRUE)
                       ON CONFLICT (data_type, month) DO UPDATE SET
                        period=EXCLUDED.period, row_count=EXCLUDED.row_count,
                        is_complete=EXCLUDED.is_complete""",
                    (table, month, period, count),
                )
    conn.commit()
    print("  -> Data coverage updated")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

LOADER_MAP = {
    "sales": ("Sales & Traffic", load_sales_traffic),
    "sqp": ("Search Query Performance", load_search_query_perf),
    "catalog": ("Catalog Performance", load_catalog_perf),
    "repeat": ("Repeat Purchase", load_repeat_purchase),
    "basket": ("Market Basket", load_market_basket),
    "search_terms": ("Search Terms", load_search_terms),
    "listings": ("Listings", None),  # special case (no month arg)
}


def load_all(conn, only: str | None = None, only_month: str | None = None) -> None:
    """Run all loaders (or a single one if --only is specified)."""

    if only and only not in LOADER_MAP:
        print(f"Unknown --only value: {only}")
        print(f"Valid: {', '.join(LOADER_MAP.keys())}")
        return

    loaders_to_run = [only] if only else list(LOADER_MAP.keys())

    for key in loaders_to_run:
        label, loader = LOADER_MAP[key]
        print(f"\n{'=' * 60}")
        print(f"Loading {label}...")
        print(f"{'=' * 60}")

        if key == "listings":
            load_listings(conn)
        elif loader:
            loader(conn, only_month=only_month)

    # Always update meta tables
    print(f"\n{'=' * 60}")
    print("Updating metadata...")
    print(f"{'=' * 60}")
    load_period_meta(conn)
    load_data_coverage(conn)


def main():
    parser = argparse.ArgumentParser(description="Load raw SP-API data into Supabase")
    parser.add_argument("--only", choices=list(LOADER_MAP.keys()),
                        help="Load only one report type")
    parser.add_argument("--month", help="Load only one month (YYYY-MM)")
    parser.add_argument("--init", action="store_true",
                        help="Initialize schema before loading")
    args = parser.parse_args()

    if args.init:
        init_db()

    conn = get_conn()
    try:
        load_all(conn, only=args.only, only_month=args.month)
    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
