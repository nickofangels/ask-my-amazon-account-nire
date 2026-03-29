import csv
import gzip
import io
import json
import os
import time
from datetime import datetime

import httpx
from auth import CREDENTIALS, MARKETPLACE, MARKETPLACE_ID, validate
from sp_api.api import Reports

validate()

os.makedirs('data/reports/sqp', exist_ok=True)

MANIFEST_PATH = 'data/manifest.json'
DATA_START = '2026-02-01T00:00:00Z'
DATA_END = '2026-02-28T23:59:59Z'
REPORT_TYPE = 'GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT'


# ---------------------------------------------------------------------------
# Shared: poll a report and return its document content (handles GZIP + S3)
# ---------------------------------------------------------------------------

def poll_and_download(reports_client, report_id, label='Report'):
    timeout = 600
    start = time.time()
    document_id = None

    while True:
        if time.time() - start > timeout:
            raise TimeoutError(f'{label} {report_id} did not complete within 10 minutes')

        # Poll with retry: 3 attempts, 5s apart; treat 3 consecutive failures as UNKNOWN
        status = 'UNKNOWN'
        for _poll_try in range(3):
            try:
                status_resp = reports_client.get_report(reportId=report_id)
                status = status_resp.payload['processingStatus']
                break
            except Exception as poll_exc:
                if _poll_try < 2:
                    print(f'  [{label}] Poll attempt {_poll_try + 1} failed ({poll_exc}), retrying in 5s...')
                    time.sleep(5)
                else:
                    print(f'  [{label}] Poll failed 3 times — treating as UNKNOWN, continuing...')

        print(f'  [{label}] Status: {status}')

        if status == 'DONE':
            document_id = status_resp.payload['reportDocumentId']
            break
        if status in ('FATAL', 'CANCELLED'):
            document_id = status_resp.payload.get('reportDocumentId')
            if document_id:
                print(f'  [{label}] FATAL but document present — downloading for error detail...')
                break
            raise RuntimeError(f'{label} {report_id} failed: {status}')

        time.sleep(30)

    # Download with retry: 3 attempts, re-fetch the pre-signed URL each time
    for _dl_try in range(1, 4):
        try:
            doc_resp = reports_client.get_report_document(reportDocumentId=document_id)
            doc_meta = doc_resp.payload
            if isinstance(doc_meta, dict) and 'url' in doc_meta:
                url = doc_meta['url']
                compression = doc_meta.get('compressionAlgorithm', '')
                raw = httpx.get(url, timeout=120).content
                if compression == 'GZIP':
                    raw = gzip.decompress(raw)
                return raw.decode('utf-8', errors='replace')
            return str(doc_meta)
        except Exception as dl_exc:
            if _dl_try < 3:
                print(f'  [{label}] Download attempt {_dl_try} failed ({dl_exc}), retrying in 15s...')
                time.sleep(15)
            else:
                raise RuntimeError(f'[{label}] Download failed after 3 attempts: {dl_exc}')


# ---------------------------------------------------------------------------
# Step 1: Get all seller ASINs via merchant listings report
# ---------------------------------------------------------------------------

def fetch_all_asins(reports_client):
    print('Requesting merchant listings report to enumerate ASINs...')
    resp = reports_client.create_report(
        reportType='GET_MERCHANT_LISTINGS_ALL_DATA',
        marketplaceIds=[MARKETPLACE_ID],
    )
    report_id = resp.payload['reportId']
    print(f'  Listings report ID: {report_id}')

    content = poll_and_download(reports_client, report_id, label='Listings')

    asins = set()
    reader = csv.DictReader(io.StringIO(content), delimiter='\t')
    for row in reader:
        asin = row.get('asin1') or row.get('ASIN') or row.get('asin')
        if asin:
            asins.add(asin.strip())

    asins = sorted(asins)
    print(f'Found {len(asins)} unique ASIN(s): {asins}')
    return asins


# ---------------------------------------------------------------------------
# Step 2: Batch ASINs to fit within the 200-char API limit
# ---------------------------------------------------------------------------

def batch_asins(asins, char_limit=200):
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
    print(f'Split into {len(batches)} ASIN batch(es)')
    return batches


# ---------------------------------------------------------------------------
# Step 3: Request SQP report for one ASIN batch
# ---------------------------------------------------------------------------

def request_sqp_batch(reports_client, asin_batch, batch_num, total):
    asin_str = ' '.join(asin_batch)
    print(f'\nBatch {batch_num}/{total}: {len(asin_batch)} ASIN(s)')

    create_kwargs = dict(
        reportType=REPORT_TYPE,
        dataStartTime=DATA_START,
        dataEndTime=DATA_END,
        marketplaceIds=[MARKETPLACE_ID],
        reportOptions={
            'reportPeriod': 'MONTH',
            'asin': asin_str,
        },
    )

    for _create_try in range(6):
        try:
            resp = reports_client.create_report(**create_kwargs)
            break
        except Exception as create_exc:
            if 'QuotaExceeded' in str(create_exc):
                wait = 60 * (2 ** _create_try)
                print(f'  QuotaExceeded — waiting {wait}s before retry ({_create_try + 1}/6)...')
                time.sleep(wait)
            else:
                raise
    else:
        raise RuntimeError(f'QuotaExceeded after 6 retries for batch {batch_num}')

    report_id = resp.payload['reportId']
    print(f'  SQP report ID: {report_id}')

    content = poll_and_download(reports_client, report_id, label=f'SQP batch {batch_num}')

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        data = {'raw': content}

    if isinstance(data, dict) and 'errorDetails' in data:
        raise RuntimeError(f'SQP batch {batch_num} error: {data["errorDetails"]}')

    return data


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

DB_PATH = 'data/amazon.db'

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS sqp_report (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    marketplace           TEXT    NOT NULL,
    asin                  TEXT    NOT NULL,
    start_date            TEXT    NOT NULL,
    end_date              TEXT    NOT NULL,
    search_query          TEXT    NOT NULL,
    search_query_score    INTEGER,
    search_query_volume   INTEGER,
    total_impressions     INTEGER,
    asin_impressions      INTEGER,
    asin_impression_share REAL,
    total_clicks          INTEGER,
    asin_clicks           INTEGER,
    asin_click_share      REAL,
    total_cart_adds       INTEGER,
    asin_cart_adds        INTEGER,
    total_purchases       INTEGER,
    asin_purchases        INTEGER,
    downloaded_at         TEXT    NOT NULL,
    UNIQUE (marketplace, asin, start_date, end_date, search_query)
)
'''

def records_to_rows(records, marketplace, downloaded_at):
    rows = []
    for r in records:
        sq = r.get('searchQueryData', {})
        imp = r.get('impressionData', {})
        clk = r.get('clickData', {})
        cart = r.get('cartAddData', {})
        purch = r.get('purchaseData', {})
        rows.append((
            marketplace,
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
    return rows

def save_to_db(records, marketplace):
    import sqlite3
    downloaded_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    rows = records_to_rows(records, marketplace, downloaded_at)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.executemany(
            '''INSERT OR IGNORE INTO sqp_report
               (marketplace, asin, start_date, end_date, search_query,
                search_query_score, search_query_volume,
                total_impressions, asin_impressions, asin_impression_share,
                total_clicks, asin_clicks, asin_click_share,
                total_cart_adds, asin_cart_adds,
                total_purchases, asin_purchases, downloaded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            rows,
        )
        inserted = conn.execute('SELECT changes()').fetchone()[0]

    print(f'Inserted {inserted} new row(s) into {DB_PATH} (skipped duplicates)')
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    reports_client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    asins = fetch_all_asins(reports_client)
    if not asins:
        print('No ASINs found in merchant listings. Exiting.')
        return

    batches = batch_asins(asins)
    all_records = []

    for i, batch in enumerate(batches, start=1):
        data = request_sqp_batch(reports_client, batch, i, len(batches))

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get('dataByAsin', data.get('records', [data]))
        else:
            records = [data]

        all_records.extend(records)
        print(f'  Batch {i}: {len(records)} record(s)')

    marketplace_id = MARKETPLACE.marketplace_id
    save_to_db(all_records, marketplace_id)
    print(f'\nDone. {len(all_records)} record(s) for {marketplace_id} stored in {DB_PATH}')


if __name__ == '__main__':
    main()
