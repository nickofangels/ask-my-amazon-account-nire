"""
repull_sales_traffic.py — One-off: re-download sales_traffic with CHILD+DAY granularity.

The original pull used PARENT granularity (Amazon's default), which gives
9 parent-level rows instead of per-child-ASIN data. This script re-pulls
all 23 months with asinGranularity=CHILD so we get childAsin-level breakdowns.

Usage:
    python scripts/repull_sales_traffic.py           # re-pull all months
    python scripts/repull_sales_traffic.py --force    # overwrite even if already CHILD
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.api_client import download_report
from config import PULL_MONTHS, full_month_bounds

RAW_DIR = Path(__file__).resolve().parent.parent / "raw" / "sales_traffic"


def _already_child(filepath: Path) -> bool:
    """Check if an existing file already has CHILD granularity."""
    if not filepath.exists():
        return False
    try:
        data = json.loads(filepath.read_text(encoding="utf-8"))
        asin_rows = data.get("salesAndTrafficByAsin", [])
        if asin_rows and "childAsin" in asin_rows[0]:
            return True
    except Exception:
        pass
    return False


def main():
    parser = argparse.ArgumentParser(description="Re-pull sales_traffic with CHILD granularity")
    parser.add_argument("--force", action="store_true", help="Re-pull even if file already has CHILD data")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    months = [(lbl, *full_month_bounds(lbl)) for _, _, lbl in PULL_MONTHS]
    print(f"Re-pulling {len(months)} months with asinGranularity=CHILD + dateGranularity=DAY")
    print(f"Output: {RAW_DIR}/")
    print()

    done = 0
    skipped = 0
    failed = []

    for i, (label, start, end) in enumerate(months):
        out = RAW_DIR / f"{label}.json"

        if not args.force and _already_child(out):
            print(f"  [{i+1}/{len(months)}] {label}: already CHILD — skipping")
            skipped += 1
            continue

        print(f"  [{i+1}/{len(months)}] {label}: pulling...", end=" ", flush=True)

        try:
            content = download_report(
                report_type="GET_SALES_AND_TRAFFIC_REPORT",
                start=start,
                end=end,
                report_options={
                    "dateGranularity": "DAY",
                    "asinGranularity": "CHILD",
                },
                max_wait=600,
                poll_interval=10,
            )
            out.write_text(content, encoding="utf-8")
            size = len(content) // 1024
            print(f"OK ({size}KB)")
            done += 1
        except Exception as exc:
            print(f"FAIL: {exc}")
            failed.append(label)

        # Pace between requests
        if i < len(months) - 1:
            time.sleep(2)

    print(f"\nDone: {done} downloaded, {skipped} skipped, {len(failed)} failed")
    if failed:
        print(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
