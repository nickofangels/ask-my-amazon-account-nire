"""
Master backfill — pulls up to 2 years of data from 5 SP-API report types
and stores them in Supabase (PostgreSQL).

Reports (run in this order — fast first, SQP last):
  sales_and_traffic          GET_SALES_AND_TRAFFIC_REPORT            (~1 min/month)
  search_catalog             GET_BRAND_ANALYTICS_SEARCH_CATALOG_...  (~4 min/month)
  market_basket              GET_BRAND_ANALYTICS_MARKET_BASKET_...   (~1 min/month)
  repeat_purchase            GET_BRAND_ANALYTICS_REPEAT_PURCHASE_... (~2 min/month)
  sqp                        GET_BRAND_ANALYTICS_SEARCH_QUERY_...    (slow/unpredictable)

Resilience:
  - Outer retry loop: up to 3 passes, 30-min cool-down between passes
  - Failed months are retried on the next pass, not abandoned
  - SQP uses single-ASIN requests (API requirement), sequential, 8 brush ASINs only

Usage:
    python backfill.py                              # full 2-year backfill
    python backfill.py --test                       # Jan 2026 only
    python backfill.py --reports sqp,market_basket  # specific reports
    python backfill.py --start 2025-01 --end 2025-06
"""

import argparse
import gzip
import io
import json
import os
import sys
import time
from calendar import monthrange
from datetime import date, datetime

import httpx
from dotenv import load_dotenv

import schema
from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID, validate
from sp_api.api import Reports

sys.stdout.reconfigure(line_buffering=True)
load_dotenv()
validate()

ALL_REPORTS = [
    'sales_and_traffic',
    'search_catalog',
    'market_basket',
    'repeat_purchase',
    'sqp',              # last — most unpredictable queue
]

MAX_PASSES = 3          # outer retry loop passes
PASS_COOLDOWN = 1800    # 30 min between outer retry passes


# ── Date utilities ─────────────────────────────────────────────────────────────

def month_chunks(start_ym, end_ym):
    """Return list of (first_day, last_day) for each calendar month in range."""
    chunks = []
    y, m = start_ym
    while (y, m) <= end_ym:
        first = date(y, m, 1)
        last = date(y, m, monthrange(y, m)[1])
        chunks.append((first, last))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return chunks


# ── Retry helpers ──────────────────────────────────────────────────────────────

def with_retry(fn, retries=3, backoff_base=30, label=''):
    """
    Call fn(). On failure retry up to `retries` times.
    Wait = backoff_base * 2^attempt between tries.
    """
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            wait = backoff_base * (2 ** attempt)
            print(f'  [{label}] Attempt {attempt + 1}/{retries} failed: {e}')
            if attempt < retries - 1:
                print(f'  [{label}] Retrying in {wait}s...')
                time.sleep(wait)
            else:
                print(f'  [{label}] All {retries} attempts exhausted.')
                raise


def create_report_with_retry(client, label='', **kwargs):
    """
    Wrap client.create_report(**kwargs) with QuotaExceeded-specific retry.
    6 attempts, exponential backoff starting at 60s (60, 120, 240, 480, 960, 1920s).
    All other exceptions are re-raised immediately.
    """
    for attempt in range(6):
        try:
            return client.create_report(**kwargs)
        except Exception as exc:
            if 'QuotaExceeded' in str(exc):
                wait = 60 * (2 ** attempt)
                print(f'  [{label}] QuotaExceeded — waiting {wait}s before retry ({attempt + 1}/6)...')
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f'[{label}] QuotaExceeded after 6 retries')


# ── Batch insert helper ────────────────────────────────────────────────────────

def batch_insert(conn, sql, rows, batch_size=1000):
    """Insert rows in batches to stay within Supabase statement limits."""
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        with conn.cursor() as cur:
            cur.executemany(sql, chunk)
        conn.commit()


# ── Safe rollback helper ───────────────────────────────────────────────────────

def safe_rollback(conn):
    """Roll back, reconnecting if the connection has died."""
    try:
        conn.rollback()
    except Exception:
        conn = schema.get_conn()
    return conn


# ── Status file writer ─────────────────────────────────────────────────────────

STATUS_FILE = '/tmp/backfill_status.json'


def write_status(label, status, extra=None):
    """Write per-report status to STATUS_FILE so monitor.py can read it."""
    try:
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
        except Exception:
            data = {}
        data[label] = {
            'status': status,
            'updated_at': datetime.now().isoformat(),
            **(extra or {}),
        }
        with open(STATUS_FILE, 'w') as f:
            json.dump(data, f)
    except Exception:
        pass


# ── Shared downloader ──────────────────────────────────────────────────────────

def poll_and_download(client, report_id, label='Report', timeout=1200):
    """
    Poll until DONE, download via S3, return decoded text.
    timeout: seconds to wait before raising TimeoutError (default 20 min).
    Includes 3-attempt poll retry for transient network errors and
    3-attempt download retry with fresh pre-signed URLs on each attempt.
    """
    start = time.time()
    write_status(label, 'IN_QUEUE', {'started_at': datetime.now().isoformat()})
    document_id = None
    while True:
        elapsed = int(time.time() - start)
        if time.time() - start > timeout:
            write_status(label, 'TIMEOUT', {'elapsed_s': elapsed})
            raise TimeoutError(
                f'{label} timed out after {timeout // 60} min ({elapsed}s elapsed)'
            )

        # Poll with retry: 3 attempts, 5s apart; treat 3 consecutive failures as UNKNOWN
        status = 'UNKNOWN'
        resp = None
        for _poll_try in range(3):
            try:
                resp = client.get_report(reportId=report_id)
                status = resp.payload['processingStatus']
                break
            except Exception as poll_exc:
                if _poll_try < 2:
                    print(f'  [{label}] Poll attempt {_poll_try + 1} failed ({poll_exc}), retrying in 5s...')
                    time.sleep(5)
                else:
                    print(f'  [{label}] Poll failed 3 times — treating as UNKNOWN, continuing...')

        print(f'  [{label}] {status} ({elapsed}s)')
        write_status(label, status, {'elapsed_s': elapsed})

        if status == 'DONE':
            document_id = resp.payload['reportDocumentId']
            write_status(label, 'DOWNLOADING', {'elapsed_s': elapsed})
            break
        if status in ('FATAL', 'CANCELLED'):
            detail = resp.payload.get('errorDetails', status) if resp else status
            write_status(label, 'FAILED', {'reason': status, 'detail': str(detail)})
            raise RuntimeError(
                f'{label} failed with status: {status} — {detail}'
            )
        time.sleep(30)

    # Download with retry: 3 attempts, re-fetch the pre-signed URL each time
    for _dl_try in range(1, 4):
        try:
            doc = client.get_report_document(reportDocumentId=document_id)
            meta = doc.payload
            raw = httpx.get(meta['url'], timeout=120).content
            if meta.get('compressionAlgorithm') == 'GZIP':
                raw = gzip.decompress(raw)
            return raw.decode('utf-8', errors='replace')
        except Exception as dl_exc:
            if _dl_try < 3:
                print(f'  [{label}] Download attempt {_dl_try} failed ({dl_exc}), retrying in 15s...')
                time.sleep(15)
            else:
                raise RuntimeError(f'[{label}] Download failed after 3 attempts: {dl_exc}')


# ── ASIN helpers ───────────────────────────────────────────────────────────────

def fetch_all_asins(client):
    """Return sorted list of all seller ASINs via merchant listings report."""
    import csv
    print('Fetching ASIN list from merchant listings...')

    def _fetch():
        resp = create_report_with_retry(
            client, label='Listings',
            reportType='GET_MERCHANT_LISTINGS_ALL_DATA',
            marketplaceIds=[MARKETPLACE_ID],
        )
        return poll_and_download(client, resp.payload['reportId'], 'Listings')

    content = with_retry(_fetch, label='Listings')
    asins = set()
    for row in csv.DictReader(io.StringIO(content), delimiter='\t'):
        asin = row.get('asin1') or row.get('ASIN') or row.get('asin')
        if asin:
            asins.add(asin.strip())
    asins = sorted(asins)
    print(f'  Found {len(asins)} ASIN(s)')
    return asins


# 6 active Nire Beauty ASINs (confirmed with Erin — Luxe England & Lüso are dead)
SQP_ASINS = [
    'B01FQZNFYG',  # $75  15pc Professional Makeup Brush Set (main, 4 SKUs)
    'B0B63QMTBQ',  # $75  15pc Glitter Makeup Brushes
    'B0CHMQGG2F',  # $75  15pc Pink Makeup Brushes
    'B08B9124NB',  # $75  White 15pc Professional Makeup Brush Set
    'B089MFSYWT',  # $75  15pc Professional Makeup Brush Set (variant)
    'B01N0ELK49',  # $30  Eye Brush Set
]


# ── Fetchers ───────────────────────────────────────────────────────────────────
# Each fetcher accepts a list of chunks and returns a list of chunks that failed.

def fetch_sales_and_traffic(client, chunks):
    print(f'\n=== sales_and_traffic ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    failed_chunks = []
    INSERT_SQL = '''
        INSERT INTO sales_and_traffic
            (marketplace, asin, parent_asin, sku, start_date, end_date,
             units_ordered, ordered_product_sales, currency, total_order_items,
             sessions, page_views, buy_box_percentage, unit_session_percentage,
             downloaded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (marketplace, asin, sku, start_date, end_date) DO NOTHING
    '''
    try:
        for start, end in chunks:
            label = f'SAT {start}'
            print(f'\n→ {label}')

            def _run(start=start, end=end):
                resp = create_report_with_retry(
                    client, label=label,
                    reportType='GET_SALES_AND_TRAFFIC_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    marketplaceIds=[MARKETPLACE_ID],
                    reportOptions={'dateGranularity': 'MONTH', 'asinGranularity': 'CHILD'},
                )
                return poll_and_download(client, resp.payload['reportId'], label)

            try:
                content = with_retry(_run, label=label)
                data = json.loads(content)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for entry in data.get('salesAndTrafficByAsin', []):
                    sb = entry.get('salesByAsin', {})
                    tb = entry.get('trafficByAsin', {})
                    ops = sb.get('orderedProductSales', {})
                    rows.append((
                        MARKETPLACE_ID,
                        entry.get('childAsin') or entry.get('parentAsin', ''),
                        entry.get('parentAsin', ''),
                        entry.get('sku', ''),
                        start.isoformat(),
                        end.isoformat(),
                        sb.get('unitsOrdered'),
                        ops.get('amount'),
                        ops.get('currencyCode'),
                        sb.get('totalOrderItems'),
                        tb.get('sessions'),
                        tb.get('pageViews'),
                        tb.get('buyBoxPercentage'),
                        tb.get('unitSessionPercentage'),
                        downloaded_at,
                    ))
                batch_insert(conn, INSERT_SQL, rows)
                total += len(rows)
                write_status(label, 'INSERTED', {'rows': len(rows)})
                print(f'  {len(rows)} rows inserted')
            except Exception as e:
                write_status(label, 'FAILED', {'error': str(e)})
                conn = safe_rollback(conn)
                print(f'  FAILED: {e}')
                failed_chunks.append((start, end))
    finally:
        conn.close()
    print(f'sales_and_traffic done: {total} rows total, {len(failed_chunks)} failed months')
    return failed_chunks


def _fetch_sqp_single(client, asin, start, end, a_idx, n_asins):
    """Fetch SQP for ONE ASIN + ONE month. Returns list of row tuples."""
    label = f'SQP {asin} {start}'
    print(f'\n→ {label} ({a_idx}/{n_asins})')

    def _run():
        resp = create_report_with_retry(
            client, label=label,
            reportType='GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT',
            dataStartTime=start.isoformat() + 'T00:00:00Z',
            dataEndTime=end.isoformat() + 'T23:59:59Z',
            marketplaceIds=[MARKETPLACE_ID],
            reportOptions={'reportPeriod': 'MONTH', 'asin': asin},
        )
        return poll_and_download(
            client, resp.payload['reportId'], label, timeout=1200
        )

    content = with_retry(_run, retries=5, backoff_base=60, label=label)
    data = json.loads(content)
    records = (
        data if isinstance(data, list)
        else data.get('dataByAsin', data.get('records', []))
    )
    downloaded_at = datetime.now().isoformat()
    rows = []
    for r in records:
        sq = r.get('searchQueryData', {})
        imp = r.get('impressionData', {})
        clk = r.get('clickData', {})
        cart = r.get('cartAddData', {})
        purch = r.get('purchaseData', {})
        rows.append((
            MARKETPLACE_ID,
            r.get('asin') or asin,
            r.get('startDate'),
            r.get('endDate'),
            sq.get('searchQuery'),
            sq.get('searchQueryScore'),
            sq.get('searchQueryVolume'),
            imp.get('totalQueryImpressionCount'),
            imp.get('asinImpressionCount'),
            imp.get('asinImpressionShare'),
            clk.get('totalClickCount'),
            clk.get('asinClickCount'),
            clk.get('asinClickShare'),
            cart.get('totalCartAddCount'),
            cart.get('asinCartAddCount'),
            purch.get('totalPurchaseCount'),
            purch.get('asinPurchaseCount'),
            downloaded_at,
        ))
    write_status(label, 'PARSED', {'rows': len(rows)})
    print(f'  [{label}] {len(rows)} rows parsed')
    return rows


def fetch_sqp(client, chunks, asins):
    """Fetch SQP one ASIN at a time, sequentially (API requirement)."""
    total_requests = len(asins) * len(chunks)
    print(f'\n=== sqp_report ({len(asins)} ASINs × {len(chunks)} months = {total_requests} requests) ===')
    print(f'    Single ASIN per request, sequential, 20-min timeout, 5 retries')

    conn = schema.get_conn()
    total = 0
    failed_chunks = []
    INSERT_SQL = '''
        INSERT INTO sqp_report
            (marketplace, asin, start_date, end_date, search_query,
             search_query_score, search_query_volume, total_impressions,
             asin_impressions, asin_impression_share, total_clicks,
             asin_clicks, asin_click_share, total_cart_adds, asin_cart_adds,
             total_purchases, asin_purchases, downloaded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (marketplace, asin, start_date, end_date, search_query)
        DO NOTHING
    '''
    request_num = 0
    try:
        for start, end in chunks:
            month_rows = []
            month_failures = 0
            for a_idx, asin in enumerate(asins, 1):
                request_num += 1
                try:
                    rows = _fetch_sqp_single(
                        client, asin, start, end, a_idx, len(asins)
                    )
                    month_rows.extend(rows)
                    print(f'  Progress: {request_num}/{total_requests}')
                except Exception as e:
                    write_status(f'SQP {asin} {start}', 'FAILED', {'error': str(e)})
                    print(f'  SQP {asin} {start} FAILED: {e}')
                    month_failures += 1

            if month_rows:
                try:
                    batch_insert(conn, INSERT_SQL, month_rows)
                    total += len(month_rows)
                    write_status(f'SQP {start}', 'INSERTED', {'rows': len(month_rows)})
                    print(f'  Month {start}: {len(month_rows)} rows inserted')
                except Exception as e:
                    write_status(f'SQP {start}', 'FAILED', {'error': str(e)})
                    conn = safe_rollback(conn)
                    print(f'  DB insert failed for {start}: {e}')
                    failed_chunks.append((start, end))
            elif month_failures == len(asins):
                failed_chunks.append((start, end))
            else:
                print(f'  Month {start}: 0 rows (no SQP data)')
    finally:
        conn.close()
    print(f'sqp_report done: {total} rows total, {len(failed_chunks)} failed months')
    return failed_chunks


def fetch_search_catalog(client, chunks):
    print(f'\n=== search_catalog_performance ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    failed_chunks = []
    INSERT_SQL = '''
        INSERT INTO search_catalog_performance
            (marketplace, asin, start_date, end_date, impression_count,
             click_count, click_rate, cart_add_count, purchase_count,
             search_traffic_sales, conversion_rate, downloaded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (marketplace, asin, start_date, end_date) DO NOTHING
    '''
    try:
        for start, end in chunks:
            label = f'SCP {start}'
            print(f'\n→ {label}')

            def _run(start=start, end=end):
                resp = create_report_with_retry(
                    client, label=label,
                    reportType='GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
                    marketplaceIds=[MARKETPLACE_ID],
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                return poll_and_download(client, resp.payload['reportId'], label)

            try:
                content = with_retry(_run, label=label)
                data = json.loads(content)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for r in data.get('dataByAsin', []):
                    imp = r.get('impressionData', {})
                    clk = r.get('clickData', {})
                    cart = r.get('cartAddData', {})
                    purch = r.get('purchaseData', {})
                    sts = purch.get('searchTrafficSales', {})
                    rows.append((
                        MARKETPLACE_ID,
                        r.get('asin', ''),
                        r.get('startDate', start.isoformat()),
                        r.get('endDate', end.isoformat()),
                        imp.get('impressionCount'),
                        clk.get('clickCount'),
                        clk.get('clickRate'),
                        cart.get('cartAddCount'),
                        purch.get('purchaseCount'),
                        sts.get('amount') if isinstance(sts, dict) else None,
                        purch.get('conversionRate'),
                        downloaded_at,
                    ))
                batch_insert(conn, INSERT_SQL, rows)
                total += len(rows)
                write_status(label, 'INSERTED', {'rows': len(rows)})
                print(f'  {len(rows)} rows inserted')
            except Exception as e:
                write_status(label, 'FAILED', {'error': str(e)})
                conn = safe_rollback(conn)
                print(f'  FAILED: {e}')
                failed_chunks.append((start, end))
    finally:
        conn.close()
    print(f'search_catalog_performance done: {total} rows total, {len(failed_chunks)} failed months')
    return failed_chunks


def fetch_market_basket(client, chunks):
    print(f'\n=== market_basket ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    failed_chunks = []
    INSERT_SQL = '''
        INSERT INTO market_basket
            (marketplace, asin, start_date, end_date, purchased_with_asin,
             purchased_with_rank, combination_pct, downloaded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (marketplace, asin, start_date, end_date, purchased_with_asin)
        DO NOTHING
    '''
    try:
        for start, end in chunks:
            label = f'MB {start}'
            print(f'\n→ {label}')

            def _run(start=start, end=end):
                resp = create_report_with_retry(
                    client, label=label,
                    reportType='GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
                    marketplaceIds=[MARKETPLACE_ID],
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                return poll_and_download(client, resp.payload['reportId'], label)

            try:
                content = with_retry(_run, label=label)
                data = json.loads(content)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for r in data.get('dataByAsin', []):
                    rows.append((
                        MARKETPLACE_ID,
                        r.get('asin', ''),
                        r.get('startDate', start.isoformat()),
                        r.get('endDate', end.isoformat()),
                        r.get('purchasedWithAsin', ''),
                        r.get('purchasedWithRank'),
                        r.get('combinationPct'),
                        downloaded_at,
                    ))
                batch_insert(conn, INSERT_SQL, rows)
                total += len(rows)
                write_status(label, 'INSERTED', {'rows': len(rows)})
                print(f'  {len(rows)} rows inserted')
            except Exception as e:
                write_status(label, 'FAILED', {'error': str(e)})
                conn = safe_rollback(conn)
                print(f'  FAILED: {e}')
                failed_chunks.append((start, end))
    finally:
        conn.close()
    print(f'market_basket done: {total} rows total, {len(failed_chunks)} failed months')
    return failed_chunks


def fetch_repeat_purchase(client, chunks):
    print(f'\n=== repeat_purchase ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    failed_chunks = []
    INSERT_SQL = '''
        INSERT INTO repeat_purchase
            (marketplace, asin, start_date, end_date, orders, unique_customers,
             repeat_customers_pct, repeat_purchase_revenue,
             repeat_purchase_revenue_pct, currency, downloaded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (marketplace, asin, start_date, end_date) DO NOTHING
    '''
    try:
        for start, end in chunks:
            label = f'RP {start}'
            print(f'\n→ {label}')

            def _run(start=start, end=end):
                resp = create_report_with_retry(
                    client, label=label,
                    reportType='GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
                    marketplaceIds=[MARKETPLACE_ID],
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                return poll_and_download(client, resp.payload['reportId'], label)

            try:
                content = with_retry(_run, label=label)
                data = json.loads(content)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for r in data.get('dataByAsin', []):
                    rev = r.get('repeatPurchaseRevenue', {})
                    rows.append((
                        MARKETPLACE_ID,
                        r.get('asin', ''),
                        r.get('startDate', start.isoformat()),
                        r.get('endDate', end.isoformat()),
                        r.get('orders'),
                        r.get('uniqueCustomers'),
                        r.get('repeatCustomersPctTotal'),
                        rev.get('amount') if isinstance(rev, dict) else None,
                        r.get('repeatPurchaseRevenuePctTotal'),
                        rev.get('currencyCode') if isinstance(rev, dict) else None,
                        downloaded_at,
                    ))
                batch_insert(conn, INSERT_SQL, rows)
                total += len(rows)
                write_status(label, 'INSERTED', {'rows': len(rows)})
                print(f'  {len(rows)} rows inserted')
            except Exception as e:
                write_status(label, 'FAILED', {'error': str(e)})
                conn = safe_rollback(conn)
                print(f'  FAILED: {e}')
                failed_chunks.append((start, end))
    finally:
        conn.close()
    print(f'repeat_purchase done: {total} rows total, {len(failed_chunks)} failed months')
    return failed_chunks


# ── DB readiness check ─────────────────────────────────────────────────────────

def wait_for_db(max_wait_minutes=30, poll_interval=30):
    """Block until Supabase accepts connections."""
    deadline = time.time() + max_wait_minutes * 60
    attempt = 0
    while True:
        try:
            conn = schema.get_conn()
            conn.close()
            print('Database is live.')
            return
        except Exception as e:
            attempt += 1
            remaining = int((deadline - time.time()) / 60)
            if time.time() >= deadline:
                raise RuntimeError(
                    f'Database did not become available within {max_wait_minutes} min. '
                    f'Last error: {e}'
                )
            print(
                f'  [DB] Not reachable (attempt {attempt}): {e}\n'
                f'  Retrying in {poll_interval}s... ({remaining} min remaining)'
            )
            time.sleep(poll_interval)


# ── Row count summary ──────────────────────────────────────────────────────────

def print_row_counts():
    tables = [
        'sales_and_traffic', 'sqp_report', 'search_catalog_performance',
        'market_basket', 'repeat_purchase',
    ]
    conn = schema.get_conn()
    print('\n=== Row counts in Supabase ===')
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f'SELECT COUNT(*) FROM {t}')
            count = cur.fetchone()[0]
            print(f'  {t:<32} {count:>8,}')
    conn.close()


# ── Main ───────────────────────────────────────────────────────────────────────

FETCHERS = {
    'sales_and_traffic': fetch_sales_and_traffic,
    'search_catalog':    fetch_search_catalog,
    'market_basket':     fetch_market_basket,
    'repeat_purchase':   fetch_repeat_purchase,
}


def main():
    parser = argparse.ArgumentParser(description='Amazon SP-API → Supabase overnight backfill')
    parser.add_argument('--test', action='store_true',
                        help='Run Jan 2026 only (quick validation)')
    parser.add_argument('--start', default=None,
                        help='Start month YYYY-MM (default: 24 months ago)')
    parser.add_argument('--end', default=None,
                        help='End month YYYY-MM (default: last complete month)')
    parser.add_argument('--reports', default=None,
                        help=f'Comma-separated subset. Options: {",".join(ALL_REPORTS)}')
    args = parser.parse_args()

    today = date.today()

    if args.test:
        start_ym = (2026, 1)
        end_ym = (2026, 1)
        print('=== TEST MODE: January 2026 only ===')
    else:
        if args.start:
            y, m = map(int, args.start.split('-'))
            start_ym = (y, m)
        else:
            y, m = today.year - 2, today.month
            start_ym = (y, m)

        if args.end:
            y, m = map(int, args.end.split('-'))
            end_ym = (y, m)
        else:
            m = today.month - 1
            y = today.year
            if m == 0:
                m, y = 12, y - 1
            end_ym = (y, m)

        print(f'=== BACKFILL: {date(*start_ym, 1)} → {date(*end_ym, monthrange(*end_ym)[1])} ===')

    enabled = set(args.reports.split(',')) if args.reports else set(ALL_REPORTS)
    all_chunks = month_chunks(start_ym, end_ym)
    print(f'{len(all_chunks)} month chunk(s), reports: {sorted(enabled)}\n')

    wait_for_db()
    schema.init_db()
    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    # Use hardcoded makeup brush ASINs for SQP
    asins = SQP_ASINS if 'sqp' in enabled else None
    if asins:
        print(f'SQP will pull {len(asins)} makeup brush ASINs')

    # Outer retry loop: up to MAX_PASSES passes, retrying only failed months
    # pending[report] = list of (start, end) chunks still to do
    pending = {r: list(all_chunks) for r in enabled}
    grand_total_failed = {}

    for pass_num in range(1, MAX_PASSES + 1):
        print(f'\n{"="*60}')
        print(f'PASS {pass_num} of {MAX_PASSES}')
        print(f'{"="*60}')

        still_failing = {}

        # Run reports in order: fast 4 first, SQP last
        for report in ALL_REPORTS:
            if report not in pending or not pending[report]:
                continue

            chunks_to_run = pending[report]
            print(f'\n  Queuing {len(chunks_to_run)} month(s) for {report}')

            if report == 'sqp':
                failed = fetch_sqp(client, chunks_to_run, asins)
            else:
                failed = FETCHERS[report](client, chunks_to_run)

            if failed:
                still_failing[report] = failed
                grand_total_failed[report] = failed

        # Summary for this pass
        total_failed = sum(len(v) for v in still_failing.values())
        print(f'\n--- Pass {pass_num} complete ---')
        if total_failed == 0:
            print('All reports succeeded. No retries needed.')
            break
        else:
            print(f'{total_failed} month(s) still failed:')
            for r, chunks in still_failing.items():
                dates = [str(c[0]) for c in chunks]
                print(f'  {r}: {dates}')

            if pass_num < MAX_PASSES:
                print(f'\nWaiting {PASS_COOLDOWN // 60} min before pass {pass_num + 1}...')
                time.sleep(PASS_COOLDOWN)
                pending = still_failing
            else:
                print(f'\nMax passes ({MAX_PASSES}) reached. Giving up on remaining months.')

    print_row_counts()
    print('\nBackfill complete.')


if __name__ == '__main__':
    main()
