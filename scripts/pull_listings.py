"""
pull_listings.py — Fetch merchant listings and insert into the listings table.

Source: GET_MERCHANT_LISTINGS_ALL_DATA (TSV, current snapshot)

Usage:
    python -m scripts.pull_listings
"""

import csv
import io
from datetime import datetime

from auth import validate
from schema import get_conn
from scripts.api_client import download_report


def pull_listings():
    validate()
    print("Pulling merchant listings...")

    content = download_report(
        report_type="GET_MERCHANT_LISTINGS_ALL_DATA",
        # Listings report doesn't use date range, but the SDK requires them.
        # Use a dummy range; the API ignores it for this report type.
        start=datetime.now().date(),
        end=datetime.now().date(),
    )

    # Parse TSV — header row uses varied naming, so normalize keys
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    rows = []
    now = datetime.now().isoformat()

    for raw_row in reader:
        # Normalize keys: lowercase, replace spaces/hyphens with underscores
        row = {
            k.strip().lower().replace(" ", "_").replace("-", "_"): v
            for k, v in raw_row.items()
        }
        asin = (row.get("asin1") or "").strip().upper()
        if not asin:
            continue
        rows.append((
            asin,
            (row.get("seller_sku") or row.get("sku") or "").strip(),
            row.get("item_name") or row.get("product_name") or "",
            row.get("price") or "",
            row.get("quantity") or "",
            row.get("open_date") or "",
            row.get("status") or row.get("item_status") or "",
            row.get("fulfillment_channel") or row.get("fulfillment") or "",
            now,
        ))

    if not rows:
        print("  No listings found in report.")
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO listings (asin, sku, product_name, price, quantity,
                              open_date, status, fulfillment, pulled_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin, sku) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            price        = EXCLUDED.price,
            quantity     = EXCLUDED.quantity,
            open_date    = EXCLUDED.open_date,
            status       = EXCLUDED.status,
            fulfillment  = EXCLUDED.fulfillment,
            pulled_at    = EXCLUDED.pulled_at
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Inserted/updated {len(rows)} listings.")


if __name__ == "__main__":
    pull_listings()
