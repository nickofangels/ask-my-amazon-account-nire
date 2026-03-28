"""
Master backfill — pulls up to 2 years of data from 10 SP-API report types
and stores them in Supabase (PostgreSQL).

Usage:
    python backfill.py                              # full 2-year backfill
    python backfill.py --test                       # Jan 2026 only (quick validation)
    python backfill.py --reports sqp,orders         # specific reports only
    python backfill.py --start 2025-01 --end 2025-06  # custom date range
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
from calendar import monthrange
from datetime import date, datetime

sys.stdout.reconfigure(line_buffering=True)

import httpx
from dotenv import load_dotenv

import schema
from auth import CREDENTIALS, MARKETPLACE, validate
from sp_api.api import Reports

load_dotenv()
validate()

MARKETPLACE_ID = MARKETPLACE.marketplace_id

ALL_REPORTS = [
    'sales_and_traffic', 'orders', 'sqp', 'search_catalog',
    'search_terms', 'market_basket', 'repeat_purchase',
    'promotions', 'coupons', 'returns',
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


# ── Shared downloader ──────────────────────────────────────────────────────────

def poll_and_download(client, report_id, label='Report'):
    """Poll until DONE, download content via S3, return decoded string."""
    timeout = 600
    start = time.time()
    while True:
        if time.time() - start > timeout:
            raise TimeoutError(f'{label} timed out after 10 min')
        resp = client.get_report(reportId=report_id)
        status = resp.payload['processingStatus']
        print(f'  [{label}] {status}')
        if status == 'DONE':
            document_id = resp.payload['reportDocumentId']
            break
        if status in ('FATAL', 'CANCELLED'):
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
    print('Fetching ASIN list from merchant listings...')
    resp = client.create_report(reportType='GET_MERCHANT_LISTINGS_ALL_DATA')
    content = poll_and_download(client, resp.payload['reportId'], 'Listings')
    asins = set()
    for row in csv.DictReader(io.StringIO(content), delimiter='\t'):
        asin = row.get('asin1') or row.get('ASIN') or row.get('asin')
        if asin:
            asins.add(asin.strip())
    asins = sorted(asins)
    print(f'  Found {len(asins)} ASIN(s): {asins}')
    return asins


def batch_asins(asins, char_limit=200):
    """Split ASIN list into batches that fit within the SP-API char limit."""
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
    try:
        for start, end in chunks:
            label = f'SAT {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_SALES_AND_TRAFFIC_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    reportOptions={'dateGranularity': 'MONTH', 'asinGranularity': 'CHILD'},
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
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
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO sales_and_traffic
                            (marketplace, asin, parent_asin, sku, start_date, end_date,
                             units_ordered, ordered_product_sales, currency, total_order_items,
                             sessions, page_views, buy_box_percentage, unit_session_percentage,
                             downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, asin, sku, start_date, end_date) DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'sales_and_traffic done: {total} rows processed')


def fetch_orders(client, chunks):
    print(f'\n=== orders ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            label = f'Orders {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for row in csv.DictReader(io.StringIO(content), delimiter='\t'):
                    def g(col):
                        return row.get(col) or None
                    def gf(col):
                        v = (row.get(col) or '').strip()
                        try:
                            return float(v) if v else None
                        except ValueError:
                            return None
                    def gi(col):
                        v = (row.get(col) or '').strip()
                        try:
                            return int(v) if v else None
                        except ValueError:
                            return None
                    rows.append((
                        MARKETPLACE_ID,
                        row.get('amazon-order-id', ''),
                        (row.get('sku') or '').strip(),
                        g('asin'),
                        g('product-name'),
                        g('purchase-date'),
                        g('order-status'),
                        g('fulfillment-channel'),
                        gi('quantity'),
                        gf('item-price'),
                        gf('item-tax'),
                        gf('shipping-price'),
                        gf('item-promotion-discount'),
                        g('currency'),
                        g('ship-city'),
                        g('ship-state'),
                        g('ship-country'),
                        g('is-business-order'),
                        downloaded_at,
                    ))
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO orders
                            (marketplace, order_id, sku, asin, product_name, purchase_date,
                             order_status, fulfillment_channel, quantity, item_price, item_tax,
                             shipping_price, promotion_discount, currency, ship_city, ship_state,
                             ship_country, is_business_order, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, order_id, sku) DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'orders done: {total} rows processed')


def fetch_sqp(client, chunks, asins):
    batches = batch_asins(asins)
    print(f'\n=== sqp_report ({len(chunks)} months × {len(batches)} ASIN batch(es)) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            for b_idx, batch in enumerate(batches, 1):
                label = f'SQP {start} b{b_idx}/{len(batches)}'
                print(f'\n→ {label}')
                try:
                    resp = client.create_report(
                        reportType='GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT',
                        dataStartTime=start.isoformat() + 'T00:00:00Z',
                        dataEndTime=end.isoformat() + 'T23:59:59Z',
                        reportOptions={'reportPeriod': 'MONTH', 'asin': ' '.join(batch)},
                    )
                    content = poll_and_download(client, resp.payload['reportId'], label)
                    data = json.loads(content)
                    records = (
                        data if isinstance(data, list)
                        else data.get('dataByAsin', data.get('records', []))
                    )
                    rows = []
                    downloaded_at = datetime.now().isoformat()
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
                    with conn.cursor() as cur:
                        cur.executemany('''
                            INSERT INTO sqp_report
                                (marketplace, asin, start_date, end_date, search_query,
                                 search_query_score, search_query_volume, total_impressions,
                                 asin_impressions, asin_impression_share, total_clicks,
                                 asin_clicks, asin_click_share, total_cart_adds, asin_cart_adds,
                                 total_purchases, asin_purchases, downloaded_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (marketplace, asin, start_date, end_date, search_query)
                            DO NOTHING
                        ''', rows)
                    conn.commit()
                    total += len(rows)
                    print(f'  Processed {len(rows)} rows')
                except Exception as e:
                    conn.rollback()
                    print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'sqp_report done: {total} rows processed')


def fetch_search_catalog(client, chunks):
    print(f'\n=== search_catalog_performance ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            label = f'SCP {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
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
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO search_catalog_performance
                            (marketplace, asin, start_date, end_date, impression_count,
                             click_count, click_rate, cart_add_count, purchase_count,
                             search_traffic_sales, conversion_rate, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, asin, start_date, end_date) DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'search_catalog_performance done: {total} rows processed')


def fetch_search_terms(client, chunks):
    print(f'\n=== search_terms ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            label = f'ST {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
                data = json.loads(content)
                rows = []
                downloaded_at = datetime.now().isoformat()
                for r in data.get('dataByDepartmentAndSearchTerm', []):
                    rows.append((
                        MARKETPLACE_ID,
                        start.isoformat(),
                        end.isoformat(),
                        r.get('departmentName', ''),
                        r.get('searchTerm', ''),
                        r.get('searchFrequencyRank'),
                        r.get('clickedAsin', ''),
                        r.get('clickShareRank'),
                        r.get('clickShare'),
                        r.get('conversionShare'),
                        downloaded_at,
                    ))
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO search_terms
                            (marketplace, start_date, end_date, department_name, search_term,
                             search_frequency_rank, clicked_asin, click_share_rank,
                             click_share, conversion_share, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, start_date, end_date,
                                     department_name, search_term, clicked_asin)
                        DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'search_terms done: {total} rows processed')


def fetch_market_basket(client, chunks):
    print(f'\n=== market_basket ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            label = f'MB {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
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
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO market_basket
                            (marketplace, asin, start_date, end_date, purchased_with_asin,
                             purchased_with_rank, combination_pct, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, asin, start_date, end_date, purchased_with_asin)
                        DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'market_basket done: {total} rows processed')


def fetch_repeat_purchase(client, chunks):
    print(f'\n=== repeat_purchase ({len(chunks)} months) ===')
    conn = schema.get_conn()
    total = 0
    try:
        for start, end in chunks:
            label = f'RP {start}'
            print(f'\n→ {label}')
            try:
                resp = client.create_report(
                    reportType='GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT',
                    dataStartTime=start.isoformat() + 'T00:00:00Z',
                    dataEndTime=end.isoformat() + 'T23:59:59Z',
                    reportOptions={'reportPeriod': 'MONTH'},
                )
                content = poll_and_download(client, resp.payload['reportId'], label)
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
                with conn.cursor() as cur:
                    cur.executemany('''
                        INSERT INTO repeat_purchase
                            (marketplace, asin, start_date, end_date, orders, unique_customers,
                             repeat_customers_pct, repeat_purchase_revenue,
                             repeat_purchase_revenue_pct, currency, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (marketplace, asin, start_date, end_date) DO NOTHING
                    ''', rows)
                conn.commit()
                total += len(rows)
                print(f'  Processed {len(rows)} rows')
            except Exception as e:
                conn.rollback()
                print(f'  ERROR: {e}')
    finally:
        conn.close()
    print(f'repeat_purchase done: {total} rows processed')


def fetch_promotions(client, backfill_start, backfill_end):
    print(f'\n=== promotions (full range: {backfill_start} → {backfill_end}) ===')
    try:
        resp = client.create_report(
            reportType='GET_PROMOTION_PERFORMANCE_REPORT',
            reportOptions={
                'promotionStartDateFrom': backfill_start.isoformat() + 'T00:00:00Z',
                'promotionStartDateTo': backfill_end.isoformat() + 'T23:59:59Z',
            },
        )
        content = poll_and_download(client, resp.payload['reportId'], 'Promotions')
        data = json.loads(content)
        rows = []
        downloaded_at = datetime.now().isoformat()
        for promo in data.get('promotions', []):
            products = promo.get('includedProducts', [])
            base = (
                MARKETPLACE_ID,
                promo.get('promotionId', ''),
                promo.get('promotionName'),
                promo.get('type'),
                promo.get('status'),
            )
            if products:
                for p in products:
                    rows.append(base + (
                        p.get('asin', ''),
                        p.get('productName'),
                        p.get('productGlanceViews'),
                        p.get('productUnitsSold'),
                        p.get('productRevenue'),
                        p.get('productRevenueCurrencyCode'),
                        promo.get('startDateTime'),
                        promo.get('endDateTime'),
                        downloaded_at,
                    ))
            else:
                rows.append(base + (
                    '',
                    None,
                    promo.get('glanceViews'),
                    promo.get('unitsSold'),
                    promo.get('revenue'),
                    promo.get('revenueCurrencyCode'),
                    promo.get('startDateTime'),
                    promo.get('endDateTime'),
                    downloaded_at,
                ))
        conn = schema.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany('''
                    INSERT INTO promotions
                        (marketplace, promotion_id, promotion_name, type, status, asin,
                         product_name, glance_views, units_sold, revenue, currency,
                         start_date, end_date, downloaded_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (marketplace, promotion_id, asin) DO NOTHING
                ''', rows)
        conn.close()
        print(f'promotions done: {len(rows)} rows processed')
    except Exception as e:
        print(f'promotions ERROR: {e}')


def fetch_coupons(client, backfill_start, backfill_end):
    print(f'\n=== coupons (full range: {backfill_start} → {backfill_end}) ===')
    try:
        resp = client.create_report(
            reportType='GET_COUPON_PERFORMANCE_REPORT',
            reportOptions={
                'campaignStartDateFrom': backfill_start.isoformat() + 'T00:00:00Z',
                'campaignStartDateTo': backfill_end.isoformat() + 'T23:59:59Z',
            },
        )
        content = poll_and_download(client, resp.payload['reportId'], 'Coupons')
        data = json.loads(content)
        rows = []
        downloaded_at = datetime.now().isoformat()
        for campaign in data.get('campaigns', []):
            for coupon in campaign.get('coupons', []):
                rows.append((
                    MARKETPLACE_ID,
                    campaign.get('campaignId', ''),
                    campaign.get('campaignName'),
                    coupon.get('couponId', ''),
                    coupon.get('couponName'),
                    coupon.get('startDate'),
                    coupon.get('endDate'),
                    coupon.get('clips'),
                    coupon.get('redemptions'),
                    coupon.get('revenue'),
                    coupon.get('revenueCurrencyCode'),
                    coupon.get('budget'),
                    downloaded_at,
                ))
        conn = schema.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany('''
                    INSERT INTO coupons
                        (marketplace, campaign_id, campaign_name, coupon_id, coupon_name,
                         start_date, end_date, clips, redemptions, revenue, currency,
                         budget, downloaded_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (marketplace, campaign_id, coupon_id) DO NOTHING
                ''', rows)
        conn.close()
        print(f'coupons done: {len(rows)} rows processed')
    except Exception as e:
        print(f'coupons ERROR: {e}')


def fetch_returns(client, backfill_start, backfill_end):
    print(f'\n=== returns (full range: {backfill_start} → {backfill_end}) ===')
    try:
        resp = client.create_report(
            reportType='GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE',
            dataStartTime=backfill_start.isoformat() + 'T00:00:00Z',
            dataEndTime=backfill_end.isoformat() + 'T23:59:59Z',
        )
        content = poll_and_download(client, resp.payload['reportId'], 'Returns')
        rows = []
        downloaded_at = datetime.now().isoformat()
        for row in csv.DictReader(io.StringIO(content), delimiter='\t'):
            def g(col):
                return row.get(col) or None
            def gi(col):
                v = (row.get(col) or '').strip()
                try:
                    return int(v) if v else None
                except ValueError:
                    return None
            rows.append((
                MARKETPLACE_ID,
                (row.get('Order ID') or row.get('order-id') or '').strip(),
                (row.get('SKU') or row.get('sku') or '').strip(),
                g('ASIN') or g('asin'),
                g('Return date') or g('return-date'),
                gi('Quantity') or gi('quantity'),
                g('Reason') or g('reason'),
                g('Status') or g('status'),
                g('Detailed Disposition') or g('Disposition') or g('disposition'),
                downloaded_at,
            ))
        conn = schema.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany('''
                    INSERT INTO returns
                        (marketplace, order_id, sku, asin, return_date, quantity,
                         reason, status, disposition, downloaded_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (marketplace, order_id, sku, return_date) DO NOTHING
                ''', rows)
        conn.close()
        print(f'returns done: {len(rows)} rows processed')
    except Exception as e:
        print(f'returns ERROR: {e}')


# ── Row count summary ──────────────────────────────────────────────────────────

def print_row_counts():
    tables = [
        'sales_and_traffic', 'orders', 'sqp_report', 'search_catalog_performance',
        'search_terms', 'market_basket', 'repeat_purchase',
        'promotions', 'coupons', 'returns',
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
        backfill_start = date(2026, 1, 1)
        backfill_end = date(2026, 1, 31)
        print('=== TEST MODE: January 2026 only ===')
    else:
        if args.start:
            y, m = map(int, args.start.split('-'))
            start_ym = (y, m)
        else:
            y = today.year - 2
            m = today.month
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

        backfill_start = date(*start_ym, 1)
        backfill_end = date(*end_ym, monthrange(*end_ym)[1])
        print(f'=== BACKFILL: {backfill_start} → {backfill_end} ===')

    enabled = set(args.reports.split(',')) if args.reports else set(ALL_REPORTS)
    chunks = month_chunks(start_ym, end_ym)
    print(f'{len(chunks)} month chunk(s), reports: {sorted(enabled)}\n')

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
    if 'orders' in enabled:
        fetch_orders(client, chunks)
    if 'sqp' in enabled:
        fetch_sqp(client, chunks, asins)
    if 'search_catalog' in enabled:
        fetch_search_catalog(client, chunks)
    if 'search_terms' in enabled:
        fetch_search_terms(client, chunks)
    if 'market_basket' in enabled:
        fetch_market_basket(client, chunks)
    if 'repeat_purchase' in enabled:
        fetch_repeat_purchase(client, chunks)
    if 'promotions' in enabled:
        fetch_promotions(client, backfill_start, backfill_end)
    if 'coupons' in enabled:
        fetch_coupons(client, backfill_start, backfill_end)
    if 'returns' in enabled:
        fetch_returns(client, backfill_start, backfill_end)

    print_row_counts()
    print('\nBackfill complete.')


if __name__ == '__main__':
    main()
