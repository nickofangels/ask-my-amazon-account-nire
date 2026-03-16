"""
Validates that SP-API credentials are correctly configured and the full
pipeline works: auth → API call → save raw response → print summary.

Run this once after filling in your .env file:
    python test_auth.py
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

from auth import CREDENTIALS, MARKETPLACE, validate

def call_with_backoff(fn, retries=5):
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if '429' in str(e) or 'QuotaExceeded' in str(e):
                wait = 2 ** attempt
                print(f"Rate limited. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Max retries exceeded")

def main():
    print("Validating credentials...")
    validate()
    print("Credentials present.\n")

    from sp_api.api import Orders

    os.makedirs('data/orders', exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)

    print(f"Fetching orders from {start.date()} to {end.date()}...")

    client = Orders(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    response = call_with_backoff(
        lambda: client.get_orders(
            LastUpdatedAfter=start.isoformat(),
            LastUpdatedBefore=end.isoformat(),
        )
    )

    orders = response.payload.get('Orders', [])
    print(f"Orders returned: {len(orders)}")

    timestamp = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    filename = f"data/orders/orders_{timestamp}.json"
    with open(filename, 'w') as f:
        json.dump(response.payload, f, indent=2, default=str)

    print(f"Raw response saved to: {filename}")
    print("\nSetup validated successfully. You're ready to start querying.")

if __name__ == '__main__':
    main()
