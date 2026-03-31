"""
api_client.py — Thin SP-API client wrapper.

All credentials come from auth.py. This module provides convenience
functions for creating API clients and downloading reports.
"""

import gzip
import io
import time

import requests
from sp_api.api import Reports

from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID


def get_client(api_class=Reports):
    """Return an instantiated SP-API client."""
    return api_class(credentials=CREDENTIALS, marketplace=MARKETPLACE)


def download_report(report_type, start, end, report_options=None,
                    poll_interval=20, max_wait=600):
    """Create, poll, and download a report. Returns decoded content string.

    Args:
        report_type: SP-API report type identifier
        start: date object for dataStartTime
        end: date object for dataEndTime
        report_options: dict of reportOptions (optional)
        poll_interval: seconds between poll attempts
        max_wait: total seconds before timeout
    """
    client = get_client()

    # ── Create report ─────────────────────────────────────────────────────
    create_kwargs = {
        "reportType": report_type,
        "dataStartTime": start.strftime("%Y-%m-%dT00:00:00Z"),
        "dataEndTime": end.strftime("%Y-%m-%dT23:59:59Z"),
        "marketplaceIds": [MARKETPLACE_ID],
    }
    if report_options:
        create_kwargs["reportOptions"] = report_options

    # Retry on QuotaExceeded (up to 6 attempts)
    report_id = None
    for attempt in range(6):
        try:
            res = client.create_report(**create_kwargs)
            report_id = (
                res.payload.get("reportId")
                if isinstance(res.payload, dict)
                else str(res.payload)
            )
            break
        except Exception as exc:
            if "QuotaExceeded" in str(exc) and attempt < 5:
                wait = 60 * (2 ** attempt)
                print(f"  QuotaExceeded, waiting {wait}s (attempt {attempt + 1}/6)")
                time.sleep(wait)
            else:
                raise

    # ── Poll until done ───────────────────────────────────────────────────
    doc_id = None
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            status_res = client.get_report(reportId=report_id)
            status = status_res.payload.get("processingStatus", "")
            if status == "DONE":
                doc_id = status_res.payload["reportDocumentId"]
                break
            if status in ("FATAL", "CANCELLED"):
                raise RuntimeError(f"Report {report_id} ended with status {status}")
        except Exception as exc:
            if "processingStatus" not in str(exc):
                raise

    if not doc_id:
        raise TimeoutError(f"Report {report_id} did not complete in {max_wait}s")

    # ── Download ──────────────────────────────────────────────────────────
    for attempt in range(3):
        try:
            doc_res = client.get_report_document(reportDocumentId=doc_id)
            url = doc_res.payload["url"]
            compressed = "GZIP" in (doc_res.payload.get("compressionAlgorithm") or "")
            resp = requests.get(url, timeout=(15, 180))
            resp.raise_for_status()
            raw = resp.content
            if compressed or (len(raw) > 1 and raw[:2] == b"\x1f\x8b"):
                raw = gzip.decompress(raw)
            # Try UTF-8 first, fall back to cp1252
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                return raw.decode("cp1252")
        except Exception:
            if attempt == 2:
                raise
            time.sleep(15)
