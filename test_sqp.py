"""
test_sqp.py — Full SQP diagnostic script.

Runs 6 checks to pinpoint why Brand Analytics / SQP reports fail.
No polling or downloading — just create_report calls. Runs in ~60 seconds.

Usage:
    python test_sqp.py
"""

import sys
import time
import traceback
from datetime import date, timedelta

from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID, validate
from backfill import SQP_ASINS
from sp_api.api import Reports


def _mask(val):
    """Show first 4 chars + *** for credential preview."""
    if not val:
        return "(NOT SET)"
    return val[:4] + "***"


def _last_complete_month():
    """Return (start, end) for the most recent complete calendar month."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start, last_month_end


# ── Step 1: Credential check ─────────────────────────────────────────────────

def check_credentials():
    print("=" * 60)
    print("STEP 1: Credential check")
    print("=" * 60)
    try:
        validate()
        print("  validate() passed")
    except EnvironmentError as e:
        print(f"  FAIL: {e}")
        return False

    print(f"  refresh_token:    {_mask(CREDENTIALS.get('refresh_token'))}")
    print(f"  lwa_app_id:       {_mask(CREDENTIALS.get('lwa_app_id'))}")
    print(f"  lwa_client_secret:{_mask(CREDENTIALS.get('lwa_client_secret'))}")
    print(f"  role_arn:         {_mask(CREDENTIALS.get('role_arn'))}")
    print(f"  marketplace:      {MARKETPLACE_ID}")
    return True


# ── Step 2: Basic API access ─────────────────────────────────────────────────

def test_basic_api(client):
    print()
    print("=" * 60)
    print("STEP 2: Basic API access (Merchant Listings)")
    print("=" * 60)
    try:
        resp = client.create_report(
            reportType="GET_MERCHANT_LISTINGS_ALL_DATA",
            marketplaceIds=[MARKETPLACE_ID],
        )
        report_id = (
            resp.payload.get("reportId")
            if isinstance(resp.payload, dict)
            else str(resp.payload)
        )
        print(f"  PASS — report ID: {report_id}")
        return True
    except Exception as exc:
        print(f"  FAIL — {exc.__class__.__name__}: {exc}")
        return False


# ── Step 3: Single ASIN SQP test ─────────────────────────────────────────────

def test_sqp_single(client, asin, start, end):
    """Attempt one SQP create_report. Returns (success, detail)."""
    try:
        resp = client.create_report(
            reportType="GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
            dataStartTime=start.isoformat() + "T00:00:00Z",
            dataEndTime=end.isoformat() + "T23:59:59Z",
            marketplaceIds=[MARKETPLACE_ID],
            reportOptions={"reportPeriod": "MONTH", "asin": asin},
        )
        report_id = (
            resp.payload.get("reportId")
            if isinstance(resp.payload, dict)
            else str(resp.payload)
        )
        return True, f"report ID: {report_id}"
    except Exception as exc:
        detail = (
            f"{exc.__class__.__name__}: {exc}\n"
            f"  Traceback:\n{traceback.format_exc()}"
        )
        return False, detail


# ── Step 4: SQP without Role ARN ─────────────────────────────────────────────

def test_sqp_no_role_arn(asin, start, end):
    print()
    print("=" * 60)
    print("STEP 4: SQP without Role ARN")
    print("=" * 60)

    if not CREDENTIALS.get("role_arn"):
        print("  (role_arn is already not set — skipping this test)")
        return

    creds_no_arn = {k: v for k, v in CREDENTIALS.items() if k != "role_arn"}
    client_no_arn = Reports(credentials=creds_no_arn, marketplace=MARKETPLACE)

    ok, detail = test_sqp_single(client_no_arn, asin, start, end)
    if ok:
        print(f"  PASS (without ARN) — {detail}")
        print("  --> Role ARN may be CAUSING the failure. Try removing SP_API_ROLE_ARN from .env")
    else:
        print(f"  FAIL (without ARN) — {detail}")
        print("  --> Fails both with and without ARN. Issue is likely account-level (Brand Registry enrollment).")


# ── Step 5: All ASINs ─────────────────────────────────────────────────────────

def test_all_asins(client, start, end):
    print()
    print("=" * 60)
    print(f"STEP 5: Testing all {len(SQP_ASINS)} ASINs")
    print("=" * 60)

    results = []
    for i, asin in enumerate(SQP_ASINS, 1):
        ok, detail = test_sqp_single(client, asin, start, end)
        status = "PASS" if ok else "FAIL"
        results.append((asin, status, detail))
        # Truncate detail for table display
        short = detail.split("\n")[0][:80]
        print(f"  [{i}/{len(SQP_ASINS)}] {asin}  {status}  {short}")
        if i < len(SQP_ASINS):
            time.sleep(2)  # avoid QuotaExceeded masking real errors

    passed = sum(1 for _, s, _ in results if s == "PASS")
    print(f"\n  Summary: {passed}/{len(results)} ASINs passed")

    if passed == 0:
        print("  --> No ASINs work. Likely an account-level issue (Brand Registry, permissions).")
    elif passed < len(results):
        print("  --> Some ASINs fail. Those may be inactive or not brand-registered.")
        failed = [a for a, s, _ in results if s == "FAIL"]
        print(f"  --> Failed ASINs: {', '.join(failed)}")

    return results


# ── Step 6: All Brand Analytics report types ──────────────────────────────────

def probe_brand_analytics(client):
    print()
    print("=" * 60)
    print("STEP 6: All Brand Analytics report types")
    print("=" * 60)

    start, end = _last_complete_month()
    start_iso = start.isoformat() + "T00:00:00Z"
    end_iso = end.isoformat() + "T23:59:59Z"

    ba_reports = [
        (
            "Search Query Performance (SQP)",
            "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
            {"reportPeriod": "MONTH", "asin": SQP_ASINS[0]},
        ),
        (
            "Search Catalog Performance",
            "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT",
            {"reportPeriod": "MONTH"},
        ),
        (
            "Search Terms",
            "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
            {"reportPeriod": "MONTH"},
        ),
        (
            "Market Basket",
            "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
            {"reportPeriod": "MONTH"},
        ),
        (
            "Repeat Purchase",
            "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
            {"reportPeriod": "MONTH"},
        ),
    ]

    for label, report_type, options in ba_reports:
        print(f"\n  {label} ({report_type})")
        try:
            resp = client.create_report(
                reportType=report_type,
                dataStartTime=start_iso,
                dataEndTime=end_iso,
                marketplaceIds=[MARKETPLACE_ID],
                reportOptions=options,
            )
            report_id = (
                resp.payload.get("reportId")
                if isinstance(resp.payload, dict)
                else str(resp.payload)
            )
            print(f"    PASS — report ID: {report_id}")
        except Exception as exc:
            print(f"    FAIL — {exc.__class__.__name__}: {exc}")
        time.sleep(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("SQP Diagnostic Tool for Nire Beauty")
    print("=" * 60)

    # Step 1
    if not check_credentials():
        print("\nCannot proceed without valid credentials. Fix .env and retry.")
        sys.exit(1)

    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)
    start, end = _last_complete_month()
    print(f"\n  Test period: {start} to {end}")

    # Step 2
    test_basic_api(client)

    # Step 3
    print()
    print("=" * 60)
    print(f"STEP 3: Single ASIN SQP test ({SQP_ASINS[0]})")
    print("=" * 60)
    ok, detail = test_sqp_single(client, SQP_ASINS[0], start, end)
    if ok:
        print(f"  PASS — {detail}")
    else:
        print(f"  FAIL — {detail}")

    # Step 4 (only if step 3 failed)
    if not ok:
        test_sqp_no_role_arn(SQP_ASINS[0], start, end)

    # Step 5
    test_all_asins(client, start, end)

    # Step 6
    probe_brand_analytics(client)

    # Final summary
    print()
    print("=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)
    print("Share this full output for debugging. Key things to check:")
    print("  1. Is Nire Beauty enrolled in Amazon Brand Registry?")
    print("  2. Does the SP-API app have Brand Analytics permissions?")
    print("  3. Is SP_API_ROLE_ARN correct (or should it be removed)?")
    print("  4. Are the ASINs in SQP_ASINS active and brand-registered?")


if __name__ == "__main__":
    main()
