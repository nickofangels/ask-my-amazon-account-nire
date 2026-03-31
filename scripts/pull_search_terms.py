"""
pull_search_terms.py — Fetch Brand Analytics search terms and insert into search_terms table.

Source: GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT
Target: search_terms table

Filters the marketplace-wide report to only rows where a Nire Beauty ASIN
was clicked (using the listings table or fallback ASINs).

Usage:
    python -m scripts.pull_search_terms
"""

import json
from datetime import datetime

from auth import validate
from config import PULL_MONTHS, month_to_period, get_active_asins
from schema import get_conn
from scripts.api_client import download_report


def pull_search_terms():
    validate()

    our_asins = set(a.upper() for a in get_active_asins())
    if not our_asins:
        print("WARNING: No active ASINs found. Search terms won't be filtered.")
        return

    print(f"Pulling search terms ({len(PULL_MONTHS)} months, "
          f"filtering to {len(our_asins)} ASINs)...")

    conn = get_conn()
    cur = conn.cursor()

    for start, end, label in PULL_MONTHS:
        print(f"  {label}: {start} to {end}")

        try:
            content = download_report(
                report_type="GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
                start=start,
                end=end,
                report_options={"reportPeriod": "MONTH"},
            )
        except Exception as exc:
            print(f"    SKIP — {exc}")
            continue

        data = json.loads(content)
        entries = data.get("dataByDepartmentAndSearchTerm", [])
        if not entries:
            print(f"    No search term data for {label}")
            continue

        now = datetime.now().isoformat()
        period = month_to_period(label)
        rows = []

        for entry in entries:
            clicked_asin = (entry.get("clickedAsin") or "").strip().upper()
            if clicked_asin not in our_asins:
                continue
            rows.append((
                label,
                entry.get("searchTerm", ""),
                int(entry.get("searchFrequencyRank", 0) or 0),
                clicked_asin,
                entry.get("clickedItemName") or entry.get("productTitle") or "",
                int(entry.get("clickShareRank", 0) or 0),
                float(entry.get("clickShare", 0) or 0),
                float(entry.get("conversionShare", 0) or 0),
                period,
                now,
            ))

        if not rows:
            print(f"    No Nire Beauty terms found for {label}")
            continue

        cur.executemany(
            """
            INSERT INTO search_terms
                (month, search_term, search_freq_rank, clicked_asin,
                 product_title, click_share_rank, click_share,
                 conversion_share, period, pulled_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (month, search_term, clicked_asin) DO UPDATE SET
                search_freq_rank = EXCLUDED.search_freq_rank,
                product_title    = EXCLUDED.product_title,
                click_share_rank = EXCLUDED.click_share_rank,
                click_share      = EXCLUDED.click_share,
                conversion_share = EXCLUDED.conversion_share,
                period           = EXCLUDED.period,
                pulled_at        = EXCLUDED.pulled_at
            """,
            rows,
        )
        conn.commit()
        print(f"    {len(rows)} search term rows inserted")

    cur.close()
    conn.close()
    print("Search terms pull complete.")


if __name__ == "__main__":
    pull_search_terms()
