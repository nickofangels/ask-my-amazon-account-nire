"""
Master backfill — pulls up to 2 years of data from 5 SP-API report types
and stores them in Supabase (PostgreSQL).

Reports:
  sales_and_traffic          GET_SALES_AND_TRAFFIC_REPORT
  sqp                        GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT
  search_catalog             GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT
  market_basket              GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT
  repeat_purchase            GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import httpx
from dotenv import load_dotenv

import schema
from auth import CREDENTIALS, MARKETPLACE, validate
from sp_api.api import Reports

sys.stdout.reconfigure(line_buffering=True)
load_dotenv()
validate()

MARKETPLACE_ID = MARKETPLACE.marketplace_id

ALL_REPORTS = [
    'sales_and_traffic',
    'sqp',
    'search_catalog',
    'market_basket',
    'repeat_purchase',
]


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


# ── Retry helper ───────────────────────────────────────────────────────────────

def with_retry(fn, retries=3, label=''):
    """
    Call fn(). On failure, retry up to `retries` times with exponential backoff
    (30s, 60s, 120s). Raises the last exception if all attempts fail.
    """
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            wait = 30 * (2 ** attempt)
            print(f'  [{label}] Attempt {attempt + 1}/{retries} failed: {e}')
            if attempt < retries - 1:
                print(f'  [{label}] Retrying in {wait}s...')
                time.sleep(wait)
            else:
                print(f'  [{label}] All {retries} attempts exhausted.')
                raise


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
        pass  # never let status writes crash the backfill


# ── Shared downloader ──────────────────────────────────────────────────────────

def poll_and_download(client, report_id, label='Report'):
    """Poll until DONE (20 min timeout), download via S3, return decoded text."""
    timeout = 1200  # 20 minutes — Brand Analytics reports can be slow
    start = time.time()
    write_status(label, 'IN_QUEUE', {'started_at': datetime.now().isoformat()})
    while True:
        if time.time() - start > timeout:
            write_status(label, 'TIMEOUT')
            raise TimeoutError(f'{label} timed out after 20 min')
        resp = client.get_report(reportId=report_id)
        status = resp.payload['processingStatus']
        print(f'  [{label}] {status}')
        write_status(label, status)
        if status == 'DONE':
            document_id = resp.payload['reportDocumentId']
            write_status(label, 'DOWNLOADING')
            break
        if status in ('FATAL', 'CANCELLED'):
            write_status(label, 'FAILED', {'reason': status})
            raise RuntimeError(f'{label} failed with status: {status}')
        time.sleep(30)

    doc = client.get_report_document(reportDocumentId=document_id)
    meta = doc.payload
    raw = httpx.get(meta['url'], timeout=120).content
    if meta.get('compressionAlgorithm') == 'GZIP':
        raw = gzip.decompress(raw)
    return raw.decode('utf-8', errors='replace')


# ── ASIN helpers ───────────────────────────────────────────────────────────────

def fetch_all_asins(client):
    """Return sorted list of all seller ASINs via merchant listings report."""
    import csv
    print('Fetching ASIN list from merchant listings...')

    def _fetch():
        resp = client.create_report(reportType='GET_MERCHANT_LISTINGS_ALL_DATA')
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


def batch_asins(asins, char_limit=200):
    """Split ASIN list into batches that fit within the SP-API 200-char limit."""
    batches, current, current_len = [], [], 0
    for asin in asins:
        addition = len(asin) + (1 if current else 0)
        if current_len + addition > char_limit:
            batches.append(current)
            current, current_len = [asin], len(asin)
        else:
            current.append(asin)
            current_len += addition
    if current:
        batches.append(current)
    return batches


# ── Fetchers ───────────────────────────────────────────────────────────────────

def fetch_sales_and_traffic(client, chunks):
    print(f'\n=== sales_and_traffic ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
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
                resp = client.create_report(
                    reportType='GET_SALES_AND_TRAFFIC_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
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
                print(f'  FAILED after retries: {e}')
    finally:
        conn.close()
    print(f'sales_and_traffic done: {total} rows total')


def _fetch_sqp_batch(client, start, end, batch, b_idx, n_batches):
    """Fetch and parse one SQP ASIN batch. Returns list of row tuples."""
    label = f'SQP {start} b{b_idx}/{n_batches}'
    print(f'\n→ {label}')

    def _run():
        resp = client.create_report(
            reportType='GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT',
            dataStartTime=start.isoformat() + 'T00:00:00Z',
            dataEndTime=end.isoformat() + 'T00:00:00Z',
            reportOptions={'reportPeriod': 'MONTH', 'asin': ' '.join(batch)},
        )
        return poll_and_download(client, resp.payload['reportId'], label)

    content = with_retry(_run, label=label)
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
            r.get('asin'),
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
    batches = batch_asins(asins)
    print(f'\n=== sqp_report ({len(chunks)} months × {len(batches)} ASIN batch(es)) ===')
    conn = schema.get_conn()
    total = 0
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
    try:
        for start, end in chunks:
            # Fetch all ASIN batches for this month in parallel (up to 4 at once)
            all_rows = []
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(
                        _fetch_sqp_batch, client, start, end, batch, b_idx, len(batches)
                    ): b_idx
                    for b_idx, batch in enumerate(batches, 1)
                }
                for future in as_completed(futures):
                    b_idx = futures[future]
                    try:
                        all_rows.extend(future.result())
                    except Exception as e:
                        write_status(f'SQP b{b_idx}', 'FAILED', {'error': str(e)})
                        print(f'  SQP batch {b_idx} FAILED after retries: {e}')

            if all_rows:
                try:
                    batch_insert(conn, INSERT_SQL, all_rows)
                    total += len(all_rows)
                    write_status(f'SQP {start}', 'INSERTED', {'rows': len(all_rows)})
                    print(f'  Month {start}: {len(all_rows)} rows inserted')
                except Exception as e:
                    write_status(f'SQP {start}', 'FAILED', {'error': str(e)})
                    conn = safe_rollback(conn)
                    print(f'  DB insert failed for {start}: {e}')
    finally:
        conn.close()
    print(f'sqp_report done: {total} rows total')


def fetch_search_catalog(client, chunks):
    print(f'\n=== search_catalog_performance ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
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
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
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
                print(f'  FAILED after retries: {e}')
    finally:
        conn.close()
    print(f'search_catalog_performance done: {total} rows total')


def fetch_market_basket(client, chunks):
    print(f'\n=== market_basket ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
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
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
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
                print(f'  FAILED after retries: {e}')
    finally:
        conn.close()
    print(f'market_basket done: {total} rows total')


def fetch_repeat_purchase(client, chunks):
    print(f'\n=== repeat_purchase ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
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
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T00:00:00Z',
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
                print(f'  FAILED after retries: {e}')
    finally:
        conn.close()
    print(f'repeat_purchase done: {total} rows total')


# ── DB readiness check ────────────────────────────────────────────────────────

def wait_for_db(max_wait_minutes=30, poll_interval=30):
    """
    Block until the Supabase database accepts connections.
    Useful when the project is paused and needs time to resume.
    Polls every `poll_interval` seconds for up to `max_wait_minutes`.
    """
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
                f'  [DB] Not reachable yet (attempt {attempt}): {e}\n'
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

def main():
    parser = argparse.ArgumentParser(description='Amazon SP-API → Supabase backfill')
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
    chunks = month_chunks(start_ym, end_ym)
    print(f'{len(chunks)} month chunk(s), reports: {sorted(enabled)}\n')

    wait_for_db()
    schema.init_db()
    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    asins = None
    if 'sqp' in enabled:
        asins = fetch_all_asins(client)
        if not asins:
            print('No ASINs found — skipping sqp.')
            enabled.discard('sqp')

    if 'sales_and_traffic' in enabled:
        fetch_sales_and_traffic(client, chunks)
    if 'sqp' in enabled:
        fetch_sqp(client, chunks, asins)
    if 'search_catalog' in enabled:
        fetch_search_catalog(client, chunks)
    if 'market_basket' in enabled:
        fetch_market_basket(client, chunks)
    if 'repeat_purchase' in enabled:
        fetch_repeat_purchase(client, chunks)

    print_row_counts()
    print('\nBackfill complete.')


if __name__ == '__main__':
    main()
