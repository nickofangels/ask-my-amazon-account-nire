"""
Quick access probe — fires one create_report call per candidate report type
and immediately prints a pass/fail table.

No polling. No downloading. Run time: ~15-30 seconds.

Usage:
    python probe_reports.py
"""

from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID, validate
from sp_api.api import Reports

validate()

client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

# ---------------------------------------------------------------------------
# Report definitions — (label, reportType, reportOptions, dataStartTime, dataEndTime)
# ---------------------------------------------------------------------------

MONTH_START = '2026-02-01T00:00:00Z'
MONTH_END   = '2026-02-28T23:59:59Z'
WEEK_START  = '2026-02-02T00:00:00Z'  # Sunday
WEEK_END    = '2026-02-08T23:59:59Z'  # Saturday
ORDER_START = '2026-01-01T00:00:00Z'
ORDER_END   = '2026-02-28T23:59:59Z'

REPORTS = [
    # --- Brand Analytics ---
    (
        'Search Query Performance (SQP)',
        'GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT',
        {'reportPeriod': 'MONTH', 'asin': 'B01FQZNFYG'},
        MONTH_START, MONTH_END,
    ),
    (
        'Search Catalog Performance',
        'GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT',
        {'reportPeriod': 'MONTH'},
        MONTH_START, MONTH_END,
    ),
    (
        'Search Terms (marketplace-wide keywords)',
        'GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT',
        {'reportPeriod': 'WEEK'},
        WEEK_START, WEEK_END,
    ),
    (
        'Market Basket Analysis',
        'GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT',
        {'reportPeriod': 'MONTH'},
        MONTH_START, MONTH_END,
    ),
    (
        'Repeat Purchase',
        'GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT',
        {'reportPeriod': 'MONTH'},
        MONTH_START, MONTH_END,
    ),
    # --- Seller Retail Analytics ---
    (
        'Sales and Traffic (Business Report)',
        'GET_SALES_AND_TRAFFIC_REPORT',
        {'dateGranularity': 'MONTH', 'asinGranularity': 'PARENT'},
        MONTH_START, MONTH_END,
    ),
    # --- Orders / FBA ---
    (
        'All Orders by Order Date (flat file)',
        'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
        None,
        ORDER_START, ORDER_END,
    ),
    (
        'FBA Fulfilled Shipments',
        'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL',
        None,
        ORDER_START, ORDER_END,
    ),
    (
        'FBA Customer Shipment Sales',
        'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA',
        None,
        ORDER_START, ORDER_END,
    ),
    # --- Promos & Returns ---
    (
        'Promotion Performance',
        'GET_PROMOTION_PERFORMANCE_REPORT',
        None,
        ORDER_START, ORDER_END,
    ),
    (
        'Coupon Performance',
        'GET_COUPON_PERFORMANCE_REPORT',
        None,
        ORDER_START, ORDER_END,
    ),
    (
        'Returns by Return Date',
        'GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE',
        None,
        ORDER_START, ORDER_END,
    ),
    # --- Settlement ---
    (
        'Settlement Report (flat file)',
        'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE',
        None,
        ORDER_START, ORDER_END,
    ),
]

# ---------------------------------------------------------------------------
# Probe each report
# ---------------------------------------------------------------------------

NO_ACCESS_SIGNALS = [
    'not authorized', 'access denied', 'unauthorized',
    'brand analytics', 'insufficient', 'forbidden',
    'not eligible', 'not enabled', 'seller is not',
]

MISCONFIGURED_SIGNALS = [
    'invalid', 'bad request', 'malformed', 'validation',
    'reportoptions', 'daterange', 'not a valid', 'parameter',
]

results = []

print(f"\nProbing {len(REPORTS)} report types against marketplace "
      f"{MARKETPLACE.marketplace_id}...\n")

for label, report_type, options, start, end in REPORTS:
    kwargs = dict(
        reportType=report_type,
        dataStartTime=start,
        dataEndTime=end,
        marketplaceIds=[MARKETPLACE_ID],
    )
    if options:
        kwargs['reportOptions'] = options

    try:
        resp = client.create_report(**kwargs)
        report_id = (
            resp.payload.get('reportId')
            if isinstance(resp.payload, dict)
            else str(resp.payload)
        )
        results.append(('ACCESSIBLE', label, report_type, report_id))
    except Exception as exc:
        msg = str(exc).lower()
        if any(sig in msg for sig in NO_ACCESS_SIGNALS):
            status = 'NO ACCESS'
        elif any(sig in msg for sig in MISCONFIGURED_SIGNALS):
            status = 'MISCONFIGURED'
        else:
            status = 'ERROR'
        results.append((status, label, report_type, str(exc)[:120]))

# ---------------------------------------------------------------------------
# Print results table
# ---------------------------------------------------------------------------

STATUS_ICONS = {
    'ACCESSIBLE':    '✓',
    'NO ACCESS':     '✗',
    'MISCONFIGURED': '?',
    'ERROR':         '!',
}

col_w = max(len(r[1]) for r in results) + 2

print(f"{'STATUS':<16} {'REPORT':<{col_w}} DETAIL")
print('-' * (16 + col_w + 60))

for status, label, report_type, detail in results:
    icon = STATUS_ICONS.get(status, ' ')
    print(f"{icon} {status:<14} {label:<{col_w}} {detail}")

print()

accessible = [r for r in results if r[0] == 'ACCESSIBLE']
blocked    = [r for r in results if r[0] == 'NO ACCESS']
other      = [r for r in results if r[0] not in ('ACCESSIBLE', 'NO ACCESS')]

print(f"Summary: {len(accessible)} accessible  |  "
      f"{len(blocked)} blocked  |  {len(other)} misconfigured/error")

if accessible:
    print("\nReport types you can build fetchers for:")
    for _, label, report_type, _ in accessible:
        print(f"  {report_type}")
