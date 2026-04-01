"""
pull_raw.py — Download all SP-API reports to local raw/ files.

Lessons learned (Nire Beauty, 2026-03):
  - createReport burst limit: 15, then ~1/min sustained. Use 5s pacing post-burst
    with exponential backoff on QuotaExceeded.
  - SQP reports process SEQUENTIALLY on Amazon's side: one at a time, ~30-60 min each.
    All other report types use separate queues and finish in 1-2 min.
  - Queue flooding is the #1 risk: creating too many SQP reports just creates a
    multi-hour backlog. Create SQP FIRST so they start processing while fast reports
    are being created and finishing.
  - Batched SQP: multiple ASINs can be space-separated in one request (200 char limit),
    reducing per-ASIN requests dramatically.
  - Reports stay downloadable for 72 hours after completion.
  - getReport/getReports (read) uses a separate quota bucket from createReport (write),
    but has its own limits. Use batch getReports (1 call per type) instead of individual
    getReport (1 call per report) to avoid burning read quota on status checks.
  - Download phase needs backoff too — downloading 100 reports at 0.3s pacing causes
    QuotaExceeded storms. Use 2s pacing + exponential backoff on failures.

Usage:
    python scripts/pull_raw.py --create              # Queue all reports (SQP first)
    python scripts/pull_raw.py --create --only fast   # Queue only fast reports
    python scripts/pull_raw.py --create --only sqp    # Queue only SQP
    python scripts/pull_raw.py --download             # Download everything that's DONE
    python scripts/pull_raw.py --status               # Show queue status
    python scripts/pull_raw.py --cancel-stale         # Cancel non-manifest IN_QUEUE reports
    python scripts/pull_raw.py --pull                  # Classic: create + wait + download (blocking)

Environment:
    Reads marketplace from .env (SP_API_MARKETPLACE_ID or SP_API_MARKETPLACE_ID_US).
    ASINs come from sqp_asins in .env (comma-separated). Set during account setup.
    Falls back to listings table if available, errors if neither is set.
    Date range comes from config.PULL_MONTHS (rolling 2-year window).
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.api_client import get_client, download_report
from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID
from config import PULL_MONTHS, full_month_bounds, get_sqp_asins
from sp_api.api import Reports

RAW_DIR = Path(__file__).resolve().parent.parent / "raw"
MANIFEST = RAW_DIR / "_manifest.json"

# ── Rate limit constants (from empirical testing) ────────────────────────────
CREATE_BURST = 15          # createReport burst allowance
CREATE_PACE_BURST = 1      # seconds between creates during burst
CREATE_PACE_SUSTAINED = 5  # seconds between creates after burst exhausted
BACKOFF_BASE = 30          # first backoff wait (seconds)
BACKOFF_MAX_ATTEMPTS = 6   # max retries per create
DOWNLOAD_PACE = 2          # seconds between download API calls (was 0.3, caused QuotaExceeded storms)
DOWNLOAD_BACKOFF_BASE = 10 # seconds to wait after QuotaExceeded during download
DOWNLOAD_MAX_RETRIES = 4   # max retries per report download

# ── SQP timing (from empirical testing) ──────────────────────────────────────
# SQP processes ONE AT A TIME on Amazon's backend, ~30-60 min each.
# Fast reports (all other types) use separate queues and finish in 1-2 min.
SQP_POLL_INTERVAL = 30     # seconds between status checks for SQP
SQP_MAX_WAIT = 3600        # max wait per SQP report in blocking mode (1 hour)
FAST_POLL_INTERVAL = 10    # seconds between status checks for fast reports
FAST_MAX_WAIT = 300        # max wait per fast report in blocking mode (5 min)

# SQP data availability: Amazon only retains ~17 months of SQP data.
# Use 16 months as safety margin. Other BA reports go back further.
SQP_MAX_LOOKBACK_MONTHS = 16


# ── Helpers ──────────────────────────────────────────────────────────────────

def _save(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _exists(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def _complete_months() -> list[tuple[date, date, str]]:
    """PULL_MONTHS excluding the current (incomplete) month."""
    today = date.today()
    return [(s, e, lbl) for s, e, lbl in PULL_MONTHS
            if not (s.year == today.year and s.month == today.month)]


def _fmt_elapsed(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def _load_manifest() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {"reports": {}}


def _save_manifest(manifest: dict):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2))


def _create_with_backoff(client, create_kwargs, label):
    """Create a report with exponential backoff on QuotaExceeded.
    Returns report_id on success, None on failure."""
    for attempt in range(BACKOFF_MAX_ATTEMPTS):
        try:
            res = client.create_report(**create_kwargs)
            report_id = (
                res.payload.get("reportId")
                if isinstance(res.payload, dict)
                else str(res.payload)
            )
            return report_id
        except Exception as exc:
            if "QuotaExceeded" in str(exc) and attempt < BACKOFF_MAX_ATTEMPTS - 1:
                wait = BACKOFF_BASE * (2 ** attempt)
                print(f"  QuotaExceeded, waiting {wait}s (attempt {attempt+1}/{BACKOFF_MAX_ATTEMPTS})...")
                time.sleep(wait)
            else:
                print(f"  FAILED: {label} — {exc}")
                return None
    return None


def _download_report_content(client, report_id):
    """Given a DONE report_id, download and return the content string."""
    import httpx, gzip

    resp = client.get_report(report_id)
    doc_id = resp.payload["reportDocumentId"]
    doc_resp = client.get_report_document(doc_id)
    url = doc_resp.payload["url"]

    dl = httpx.get(url, timeout=120)
    raw = dl.content
    if len(raw) > 1 and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


# ── Report definitions ───────────────────────────────────────────────────────

def _build_report_list(only: str, sqp_asins: list[str] | None = None) -> list[dict]:
    """Build list of all reports we need to create.

    When only='all', SQP reports come FIRST so they start processing ASAP
    (they use a sequential queue and take 30-60 min each).

    sqp_asins: passed in from caller (loaded from .env / listings table).
               Required when only includes SQP.
    """
    months = _complete_months()
    sqp_reports = []
    fast_reports = []

    # Listings (one-shot, no date range)
    if only in ("all", "fast", "listings"):
        fast_reports.append({
            "key": "listings/listings",
            "file": "listings/listings.tsv",
            "label": "Listings",
            "create_kwargs": {
                "reportType": "GET_MERCHANT_LISTINGS_ALL_DATA",
                "marketplaceIds": [MARKETPLACE_ID],
            },
        })

    # Monthly reports (non-SQP) — these use separate queues, finish in 1-2 min
    monthly_types = []
    if only in ("all", "fast", "sales"):
        monthly_types.append(("sales_traffic", "Sales & Traffic",
                              "GET_SALES_AND_TRAFFIC_REPORT",
                              {"dateGranularity": "DAY", "asinGranularity": "CHILD"}))
    if only in ("all", "fast", "search_terms"):
        monthly_types.append(("search_terms", "Search Terms",
                              "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
                              {"reportPeriod": "MONTH"}))
    if only in ("all", "fast", "catalog"):
        monthly_types.append(("search_catalog_performance", "Catalog Perf",
                              "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT",
                              {"reportPeriod": "MONTH"}))
    if only in ("all", "fast", "basket"):
        monthly_types.append(("market_basket", "Market Basket",
                              "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
                              {"reportPeriod": "MONTH"}))
    if only in ("all", "fast", "repeat"):
        monthly_types.append(("repeat_purchase", "Repeat Purchase",
                              "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
                              {"reportPeriod": "MONTH"}))

    for folder, label, report_type, opts in monthly_types:
        for start, end, lbl in months:
            month_start, month_end = full_month_bounds(lbl)
            kwargs = {
                "reportType": report_type,
                "dataStartTime": month_start.strftime("%Y-%m-%dT00:00:00Z"),
                "dataEndTime": month_end.strftime("%Y-%m-%dT23:59:59Z"),
                "marketplaceIds": [MARKETPLACE_ID],
            }
            if opts:
                kwargs["reportOptions"] = dict(opts)
            fast_reports.append({
                "key": f"{folder}/{lbl}",
                "file": f"{folder}/{lbl}.json",
                "label": f"{label} {lbl}",
                "create_kwargs": kwargs,
            })

    # SQP — batched: ASINs space-separated per request (200 char limit on asin field).
    # If too many ASINs to fit in one request, auto-split into batches.
    # Each batch = separate report = separate ~30-60 min in the sequential queue.
    # SQP processes sequentially, so create these FIRST.
    # IMPORTANT: SQP data only goes back ~17 months. Cap at SQP_MAX_LOOKBACK_MONTHS.
    # Order: newest first — recent data is more valuable and confirms the pipeline works.
    if only in ("all", "sqp"):
        if not sqp_asins:
            raise ValueError(
                "No ASINs provided for SQP reports. "
                "Set SQP_ASINS in .env (comma-separated)."
            )

        # Filter months to SQP lookback limit
        today = date.today()
        cutoff_year = today.year
        cutoff_month = today.month - SQP_MAX_LOOKBACK_MONTHS
        while cutoff_month <= 0:
            cutoff_month += 12
            cutoff_year -= 1
        sqp_cutoff = date(cutoff_year, cutoff_month, 1)
        sqp_months = [(s, e, lbl) for s, e, lbl in months
                      if full_month_bounds(lbl)[0] >= sqp_cutoff]
        if len(sqp_months) < len(months):
            skipped = len(months) - len(sqp_months)
            print(f"NOTE: Skipping {skipped} months for SQP (>{SQP_MAX_LOOKBACK_MONTHS} months old, "
                  f"Amazon only retains ~17 months of SQP data)")

        # Reverse: newest first — recent data is more valuable
        sqp_months = list(reversed(sqp_months))

        # Split ASINs into batches that fit within 200 char limit
        SQP_ASIN_CHAR_LIMIT = 200
        asin_batches = []
        current_batch = []
        current_len = 0
        for asin in sqp_asins:
            # +1 for the space separator (except first in batch)
            needed = len(asin) + (1 if current_batch else 0)
            if current_len + needed > SQP_ASIN_CHAR_LIMIT and current_batch:
                asin_batches.append(current_batch)
                current_batch = [asin]
                current_len = len(asin)
            else:
                current_batch.append(asin)
                current_len += needed
        if current_batch:
            asin_batches.append(current_batch)

        if len(asin_batches) > 1:
            print(f"NOTE: {len(sqp_asins)} ASINs split into {len(asin_batches)} batches "
                  f"(200 char limit). Each batch = separate SQP report per month.")
            print(f"  This means {len(asin_batches)}x more SQP reports in the queue.")
            for i, batch in enumerate(asin_batches):
                print(f"  Batch {i+1}: {len(batch)} ASINs ({len(' '.join(batch))} chars)")

        for start, end, lbl in sqp_months:
            month_start, month_end = full_month_bounds(lbl)
            for batch_idx, batch in enumerate(asin_batches):
                asin_str = " ".join(batch)
                # Use batch suffix in key/file only if multiple batches
                suffix = f"_b{batch_idx+1}" if len(asin_batches) > 1 else ""
                batch_label = f" batch {batch_idx+1}/{len(asin_batches)}" if len(asin_batches) > 1 else ""
                sqp_reports.append({
                    "key": f"sqp/{lbl}{suffix}",
                    "file": f"sqp/{lbl}{suffix}.json",
                    "label": f"SQP {lbl} ({len(batch)} ASINs{batch_label})",
                    "create_kwargs": {
                        "reportType": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
                        "dataStartTime": month_start.strftime("%Y-%m-%dT00:00:00Z"),
                        "dataEndTime": month_end.strftime("%Y-%m-%dT23:59:59Z"),
                        "marketplaceIds": [MARKETPLACE_ID],
                        "reportOptions": {"reportPeriod": "MONTH", "asin": asin_str},
                    },
                })

    # SQP first when pulling all — they need the most processing time
    if only == "all":
        return sqp_reports + fast_reports
    return sqp_reports + fast_reports


# ── Phase 1: Create ─────────────────────────────────────────────────────────

def cmd_create(only: str, sqp_asins: list[str] = ()):
    """Queue all report creates, save report IDs to manifest.

    Order: SQP first (slow processor), then fast reports.
    Pacing: burst of 15 at 1s intervals, then 5s sustained to avoid
    excessive QuotaExceeded backoffs.
    """
    manifest = _load_manifest()
    all_reports = _build_report_list(only, sqp_asins=sqp_asins)
    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    # Skip reports that already have files or report IDs
    to_create = []
    for r in all_reports:
        out = RAW_DIR / r["file"]
        if _exists(out):
            continue
        if r["key"] in manifest["reports"]:
            continue
        to_create.append(r)

    if not to_create:
        print(f"Nothing to create — {len(all_reports)} reports already queued or downloaded.")
        return

    print(f"Creating {len(to_create)} reports (burst: {CREATE_BURST}, "
          f"then 1 per {CREATE_PACE_SUSTAINED}s)...")
    created = 0
    for i, r in enumerate(to_create):
        report_id = _create_with_backoff(client, r["create_kwargs"], r["label"])
        if report_id:
            manifest["reports"][r["key"]] = {
                "report_id": report_id,
                "file": r["file"],
                "label": r["label"],
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "CREATED",
            }
            _save_manifest(manifest)
            created += 1
            print(f"  [{created}/{len(to_create)}] {r['label']} → {report_id}")

        # Pace: burst phase then sustained
        if i < len(to_create) - 1:
            time.sleep(CREATE_PACE_BURST if i < CREATE_BURST - 1 else CREATE_PACE_SUSTAINED)

    print(f"\nCreated {created}/{len(to_create)} reports. Run --download to collect results.")


# ── Phase 2: Download ───────────────────────────────────────────────────────

def _batch_status_check(client, manifest) -> dict[str, str]:
    """Get status of all non-terminal reports using batch getReports (1 call per type).

    Returns {report_id: status} for all reports found.
    Much cheaper than calling getReport 139 times individually.
    """
    # Map manifest key prefixes to SP-API report type strings
    PREFIX_TO_REPORT_TYPE = {
        "listings": "GET_MERCHANT_LISTINGS_ALL_DATA",
        "sales_traffic": "GET_SALES_AND_TRAFFIC_REPORT",
        "search_terms": "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
        "search_catalog_performance": "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT",
        "market_basket": "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
        "repeat_purchase": "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
        "sqp": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
    }

    # Collect only report types that have pending reports
    pending_types = set()
    ids_to_check = set()
    for key, entry in manifest["reports"].items():
        out = RAW_DIR / entry["file"]
        if _exists(out) or entry.get("status") in ("DOWNLOADED", "FATAL", "CANCELLED"):
            continue
        ids_to_check.add(entry["report_id"])
        prefix = key.split("/")[0]
        if prefix in PREFIX_TO_REPORT_TYPE:
            pending_types.add(PREFIX_TO_REPORT_TYPE[prefix])

    if not ids_to_check:
        return {}

    statuses = {}
    for rtype in pending_types:
        try:
            res = client.get_reports(reportTypes=[rtype], pageSize=100)
            reports = res.payload.get("reports", []) if isinstance(res.payload, dict) else res.payload
            for r in reports:
                rid = r.get("reportId")
                if rid in ids_to_check:
                    statuses[rid] = r.get("processingStatus", "UNKNOWN")
            time.sleep(DOWNLOAD_PACE)
        except Exception as exc:
            print(f"  Batch status check for {rtype.split('_')[-2]}: {exc}")

    return statuses


def _download_with_backoff(client, report_id, label):
    """Download a DONE report with exponential backoff on QuotaExceeded.
    Returns content string on success, None on failure."""
    for attempt in range(DOWNLOAD_MAX_RETRIES):
        try:
            return _download_report_content(client, report_id)
        except Exception as exc:
            if "QuotaExceeded" in str(exc) and attempt < DOWNLOAD_MAX_RETRIES - 1:
                wait = DOWNLOAD_BACKOFF_BASE * (2 ** attempt)
                print(f"    QuotaExceeded, backing off {wait}s...")
                time.sleep(wait)
            else:
                print(f"  {label}: download error — {exc}")
                return None
    return None


def cmd_download():
    """Check manifest for DONE reports, download them.

    Uses batch getReports (7 API calls) instead of individual getReport (139 calls)
    to check statuses. Downloads with exponential backoff on QuotaExceeded.
    """
    manifest = _load_manifest()
    if not manifest["reports"]:
        print("No reports in manifest. Run --create first.")
        return

    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)
    done = 0
    pending = 0
    failed = 0
    already = 0

    # Step 1: Batch status check (7 API calls instead of 139)
    statuses = _batch_status_check(client, manifest)

    # Step 2: Process each manifest entry
    for key, entry in list(manifest["reports"].items()):
        out = RAW_DIR / entry["file"]
        if _exists(out):
            already += 1
            continue

        # Skip terminal states from manifest (no API call needed)
        if entry.get("status") in ("FATAL", "CANCELLED"):
            failed += 1
            continue

        report_id = entry["report_id"]
        status = statuses.get(report_id, entry.get("status", "UNKNOWN"))

        if status == "DONE":
            content = _download_with_backoff(client, report_id, entry["label"])
            if content is not None:
                _save(out, content)
                entry["status"] = "DOWNLOADED"
                _save_manifest(manifest)
                size = len(content) // 1024
                print(f"  {entry['label']}: downloaded ({size}KB)")
                done += 1
                time.sleep(DOWNLOAD_PACE)
            else:
                failed += 1
        elif status in ("FATAL", "CANCELLED"):
            print(f"  {entry['label']}: {status}")
            entry["status"] = status
            _save_manifest(manifest)
            failed += 1
        else:
            pending += 1

    print(f"\nDownloaded: {done}  Already had: {already}  Pending: {pending}  Failed: {failed}")
    if pending:
        print(f"Run --download again later to collect the remaining {pending} reports.")


# ── Status ───────────────────────────────────────────────────────────────────

def cmd_status():
    """Show status of all reports in manifest."""
    manifest = _load_manifest()
    if not manifest["reports"]:
        print("No reports in manifest. Run --create first.")
        return

    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)
    counts = {}

    for key, entry in manifest["reports"].items():
        out = RAW_DIR / entry["file"]
        if _exists(out):
            status = "DOWNLOADED"
        elif entry.get("status") in ("FATAL", "CANCELLED"):
            status = entry["status"]
        else:
            try:
                resp = client.get_report(entry["report_id"])
                status = resp.payload.get("processingStatus", "UNKNOWN")
            except:
                status = "POLL_ERROR"
            time.sleep(DOWNLOAD_PACE)

        counts[status] = counts.get(status, 0) + 1

    total = len(manifest["reports"])
    print(f"Total reports: {total}")
    for status, count in sorted(counts.items()):
        bar = "#" * count
        print(f"  {status:15s} {count:4d}  {bar}")

    downloaded = counts.get("DOWNLOADED", 0)
    done = counts.get("DONE", 0)
    if done:
        print(f"\n{done} reports ready to download — run --download")
    if downloaded == total:
        print(f"\nAll {total} reports downloaded!")


# ── Cancel stale reports ─────────────────────────────────────────────────────

def cmd_cancel_stale():
    """Cancel IN_QUEUE reports that aren't in our manifest (old tests, etc).

    This clears the SQP queue so our reports process sooner.
    Only cancels SQP reports since those are the bottleneck.
    """
    manifest = _load_manifest()
    our_ids = {v["report_id"] for v in manifest["reports"].values()}

    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)
    res = client.get_reports(
        reportTypes=["GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"],
        pageSize=100,
    )
    reports = res.payload.get("reports", []) if isinstance(res.payload, dict) else res.payload

    to_cancel = [
        r for r in reports
        if r.get("processingStatus") == "IN_QUEUE"
        and r.get("reportId") not in our_ids
    ]

    if not to_cancel:
        print("No stale reports to cancel.")
        return

    print(f"Cancelling {len(to_cancel)} stale SQP reports not in our manifest...")
    cancelled = 0
    for r in to_cancel:
        rid = r.get("reportId")
        try:
            client.cancel_report(rid)
            print(f"  Cancelled {rid} (created {r.get('createdTime', '?')[:19]})")
            cancelled += 1
        except Exception as exc:
            print(f"  Failed to cancel {rid}: {exc}")
        time.sleep(0.5)

    print(f"\nCancelled {cancelled}/{len(to_cancel)} stale reports.")


# ── Classic pull (blocking) ──────────────────────────────────────────────────

def cmd_pull(only: str, sqp_asins: list[str] = ()):
    """Classic mode: create, wait, download each report sequentially."""
    all_reports = _build_report_list(only, sqp_asins=sqp_asins)
    t0 = time.time()
    done = 0
    skipped = 0
    failed = []

    print(f"Pulling {len(all_reports)} reports sequentially...")
    print()

    for i, r in enumerate(all_reports):
        out = RAW_DIR / r["file"]
        if _exists(out):
            skipped += 1
            continue

        elapsed = _fmt_elapsed(time.time() - t0)
        seq = i + 1
        print(f"  [{seq}/{len(all_reports)}] {r['label']} ({elapsed})...", end=" ", flush=True)

        # SQP is much slower than other report types
        is_sqp = "SEARCH_QUERY_PERFORMANCE" in r["create_kwargs"]["reportType"]
        max_wait = SQP_MAX_WAIT if is_sqp else FAST_MAX_WAIT
        poll_interval = SQP_POLL_INTERVAL if is_sqp else FAST_POLL_INTERVAL

        try:
            kwargs = r["create_kwargs"]
            report_type = kwargs["reportType"]

            start_str = kwargs.get("dataStartTime")
            end_str = kwargs.get("dataEndTime")
            if start_str and end_str:
                start = date.fromisoformat(start_str[:10])
                end = date.fromisoformat(end_str[:10])
            else:
                start = end = date.today()

            content = download_report(
                report_type=report_type,
                start=start,
                end=end,
                report_options=kwargs.get("reportOptions"),
                max_wait=max_wait,
                poll_interval=poll_interval,
            )
            _save(out, content)
            size = len(content) // 1024
            print(f"OK ({size}KB)")
            done += 1
        except Exception as exc:
            print(f"FAIL: {exc}")
            failed.append(r["label"])

        if i < len(all_reports) - 1:
            time.sleep(2)

    total_time = _fmt_elapsed(time.time() - t0)
    print(f"\nDone: {done} downloaded, {skipped} skipped, {len(failed)} failed ({total_time})")
    if failed:
        print(f"Failed: {', '.join(failed[:20])}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Download SP-API reports to raw/ files")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create", action="store_true", help="Queue report creates, save manifest")
    mode.add_argument("--download", action="store_true", help="Download DONE reports from manifest")
    mode.add_argument("--status", action="store_true", help="Show queue status")
    mode.add_argument("--cancel-stale", action="store_true",
                      help="Cancel non-manifest IN_QUEUE SQP reports (clear old tests)")
    mode.add_argument("--pull", action="store_true", help="Classic: create + wait + download (blocking)")
    parser.add_argument("--only", choices=[
        "all", "fast", "sqp", "listings", "sales", "search_terms",
        "catalog", "basket", "repeat",
    ], default="all", help="Which reports to pull")
    args = parser.parse_args()

    RAW_DIR.mkdir(exist_ok=True)
    months = _complete_months()

    # Load ASINs if needed for SQP-related commands
    needs_asins = args.only in ("all", "sqp") and (args.create or args.pull)
    asins = []
    if needs_asins:
        asins = get_sqp_asins()

    print(f"Output dir:  {RAW_DIR}")
    print(f"Marketplace: {MARKETPLACE_ID}")
    print(f"Months:      {len(months)} complete ({months[0][2]} → {months[-1][2]})")
    if asins:
        print(f"SQP ASINs:   {len(asins)} ({' '.join(asins[:3])}{'...' if len(asins) > 3 else ''})")
    print()

    if args.create:
        cmd_create(args.only, asins)
    elif args.download:
        cmd_download()
    elif args.status:
        cmd_status()
    elif args.cancel_stale:
        cmd_cancel_stale()
    elif args.pull:
        cmd_pull(args.only, asins)


if __name__ == "__main__":
    main()
