"""
update.py — Pull the latest month's data and rebuild scored tables.

This is the incremental update path: pull new reports from SP-API,
save to raw/, load into Supabase, and rebuild the scoring pipeline.

Usage:
    python scripts/update.py                  # pull latest month + rebuild
    python scripts/update.py --month 2026-03  # pull a specific month + rebuild
    python scripts/update.py --load-only      # skip pull, just reload from disk + rebuild
    python scripts/update.py --rebuild-only   # skip pull + load, just rebuild scores
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import full_month_bounds, get_sqp_asins, PULL_MONTHS

RAW_DIR = Path(__file__).resolve().parent.parent / "raw"

# Report types and their raw/ subdirectories
FAST_REPORTS = [
    ("sales_traffic", "GET_SALES_AND_TRAFFIC_REPORT",
     {"dateGranularity": "DAY", "asinGranularity": "CHILD"}),
    ("search_catalog_performance", "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT",
     {"reportPeriod": "MONTH"}),
    ("market_basket", "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
     {"reportPeriod": "MONTH"}),
    ("repeat_purchase", "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
     {"reportPeriod": "MONTH"}),
    ("search_terms", "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
     {"reportPeriod": "MONTH"}),
]


def _latest_month() -> str:
    """Return the most recent complete month label."""
    today = date.today()
    if today.month == 1:
        return f"{today.year - 1:04d}-12"
    return f"{today.year:04d}-{today.month - 1:02d}"


def pull_month(month: str) -> None:
    """Pull all report types for a single month from SP-API."""
    from scripts.api_client import download_report
    from auth import MARKETPLACE_ID

    start, end = full_month_bounds(month)
    print(f"\nPulling {month} ({start} to {end})")

    # Fast reports (1-2 min each)
    for folder, report_type, opts in FAST_REPORTS:
        out = RAW_DIR / folder / f"{month}.json"
        out.parent.mkdir(parents=True, exist_ok=True)

        print(f"  {folder}...", end=" ", flush=True)
        try:
            content = download_report(
                report_type=report_type,
                start=start,
                end=end,
                report_options=opts,
                max_wait=600,
                poll_interval=10,
            )
            out.write_text(content, encoding="utf-8")
            size = len(content) // 1024
            print(f"OK ({size}KB)")
        except Exception as exc:
            print(f"FAIL: {exc}")

        time.sleep(2)

    # SQP (slow — batched ASINs, ~30-60 min)
    try:
        asins = get_sqp_asins()
    except Exception:
        print("  sqp: SKIP (no ASINs configured)")
        return

    asin_str = " ".join(asins)
    out = RAW_DIR / "sqp" / f"{month}.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"  sqp ({len(asins)} ASINs)...", end=" ", flush=True)
    try:
        content = download_report(
            report_type="GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
            start=start,
            end=end,
            report_options={"reportPeriod": "MONTH", "asin": asin_str},
            max_wait=5400,  # 90 min timeout for SQP
            poll_interval=30,
        )
        out.write_text(content, encoding="utf-8")
        size = len(content) // 1024
        print(f"OK ({size}KB)")
    except Exception as exc:
        print(f"FAIL: {exc}")

    # Listings (always refresh)
    from scripts.api_client import download_report as dl
    listings_out = RAW_DIR / "listings" / "listings.tsv"
    listings_out.parent.mkdir(parents=True, exist_ok=True)
    print(f"  listings...", end=" ", flush=True)
    try:
        content = dl(
            report_type="GET_MERCHANT_LISTINGS_ALL_DATA",
            start=start,
            end=end,
            max_wait=300,
            poll_interval=10,
        )
        listings_out.write_text(content, encoding="utf-8")
        print(f"OK")
    except Exception as exc:
        print(f"FAIL: {exc}")


def load_month(month: str | None = None) -> None:
    """Load data from raw/ files into Supabase."""
    from schema import get_conn, init_db
    from db.load import load_all

    init_db()
    conn = get_conn()
    try:
        load_all(conn, only_month=month)
    finally:
        conn.close()


def rebuild_scores() -> None:
    """Rebuild the scored tables (asin_keyword_scores + keyword_targets)."""
    print("\n" + "=" * 60)
    print("Rebuilding scored tables...")
    print("=" * 60)

    from db.build_asin_keywords import main as build_asin_keywords
    build_asin_keywords()

    from db.build_keywords import main as build_keywords
    build_keywords()


def main():
    parser = argparse.ArgumentParser(description="Incremental update: pull + load + rebuild")
    parser.add_argument("--month", help="Month to pull (YYYY-MM). Default: latest complete month")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip SP-API pull, just reload from disk + rebuild")
    parser.add_argument("--rebuild-only", action="store_true",
                        help="Skip pull + load, just rebuild scored tables")
    args = parser.parse_args()

    month = args.month or _latest_month()
    t0 = time.time()

    if args.rebuild_only:
        rebuild_scores()
    elif args.load_only:
        load_month(month)
        rebuild_scores()
    else:
        pull_month(month)
        load_month(month)
        rebuild_scores()

    elapsed = time.time() - t0
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    print(f"\nUpdate complete in {mins}m {secs}s")


if __name__ == "__main__":
    main()
