"""
Database schema for the Amazon business database (Supabase / PostgreSQL).

Call init_db() at the start of any script to ensure all tables exist.
Call get_conn() anywhere to get a live psycopg2 connection.
"""

import os
from urllib.parse import urlparse, unquote

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    """Return a psycopg2 connection to the Supabase PostgreSQL database."""
    url = urlparse(os.getenv('DATABASE_URL'))
    return psycopg2.connect(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip('/'),
        user=url.username,
        password=unquote(url.password),
        sslmode='require',
    )


TABLES = [
    # ── Sales & Traffic ────────────────────────────────────────────────────────
    # Source: GET_SALES_AND_TRAFFIC_REPORT (Seller only)
    # Canonical for: revenue, units, page views, sessions, conversion, buy box %
    """
    CREATE TABLE IF NOT EXISTS sales_and_traffic (
        id                      SERIAL PRIMARY KEY,
        marketplace             TEXT NOT NULL,
        asin                    TEXT NOT NULL DEFAULT '',
        parent_asin             TEXT NOT NULL DEFAULT '',
        sku                     TEXT NOT NULL DEFAULT '',
        start_date              TEXT NOT NULL,
        end_date                TEXT NOT NULL,
        units_ordered           INTEGER,
        ordered_product_sales   DOUBLE PRECISION,
        currency                TEXT,
        total_order_items       INTEGER,
        sessions                INTEGER,
        page_views              INTEGER,
        buy_box_percentage      DOUBLE PRECISION,
        unit_session_percentage DOUBLE PRECISION,
        downloaded_at           TEXT NOT NULL,
        UNIQUE (marketplace, asin, sku, start_date, end_date)
    )
    """,

    # ── Orders ─────────────────────────────────────────────────────────────────
    # Source: GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL
    # Canonical for: individual order-level detail, geo, fulfillment channel
    """
    CREATE TABLE IF NOT EXISTS orders (
        id                   SERIAL PRIMARY KEY,
        marketplace          TEXT NOT NULL,
        order_id             TEXT NOT NULL,
        sku                  TEXT NOT NULL DEFAULT '',
        asin                 TEXT,
        product_name         TEXT,
        purchase_date        TEXT,
        order_status         TEXT,
        fulfillment_channel  TEXT,
        quantity             INTEGER,
        item_price           DOUBLE PRECISION,
        item_tax             DOUBLE PRECISION,
        shipping_price       DOUBLE PRECISION,
        promotion_discount   DOUBLE PRECISION,
        currency             TEXT,
        ship_city            TEXT,
        ship_state           TEXT,
        ship_country         TEXT,
        is_business_order    TEXT,
        downloaded_at        TEXT NOT NULL,
        UNIQUE (marketplace, order_id, sku)
    )
    """,

    # ── Search Query Performance ───────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT
    # Canonical for: per-ASIN per-query impressions, clicks, cart adds, purchases
    """
    CREATE TABLE IF NOT EXISTS sqp_report (
        id                    SERIAL PRIMARY KEY,
        marketplace           TEXT NOT NULL,
        asin                  TEXT NOT NULL,
        start_date            TEXT NOT NULL,
        end_date              TEXT NOT NULL,
        search_query          TEXT NOT NULL,
        search_query_score    INTEGER,
        search_query_volume   INTEGER,
        total_impressions     INTEGER,
        asin_impressions      INTEGER,
        asin_impression_share DOUBLE PRECISION,
        total_clicks          INTEGER,
        asin_clicks           INTEGER,
        asin_click_share      DOUBLE PRECISION,
        total_cart_adds       INTEGER,
        asin_cart_adds        INTEGER,
        total_purchases       INTEGER,
        asin_purchases        INTEGER,
        downloaded_at         TEXT NOT NULL,
        UNIQUE (marketplace, asin, start_date, end_date, search_query)
    )
    """,

    # ── Search Catalog Performance ─────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT
    # Canonical for: catalog-wide search funnel per ASIN (no query breakdown)
    """
    CREATE TABLE IF NOT EXISTS search_catalog_performance (
        id                   SERIAL PRIMARY KEY,
        marketplace          TEXT NOT NULL,
        asin                 TEXT NOT NULL,
        start_date           TEXT NOT NULL,
        end_date             TEXT NOT NULL,
        impression_count     INTEGER,
        click_count          INTEGER,
        click_rate           DOUBLE PRECISION,
        cart_add_count       INTEGER,
        purchase_count       INTEGER,
        search_traffic_sales DOUBLE PRECISION,
        conversion_rate      DOUBLE PRECISION,
        downloaded_at        TEXT NOT NULL,
        UNIQUE (marketplace, asin, start_date, end_date)
    )
    """,

    # ── Search Terms (marketplace-wide) ───────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT
    # Canonical for: marketplace-wide keyword intelligence, top ASINs per term
    """
    CREATE TABLE IF NOT EXISTS search_terms (
        id                    SERIAL PRIMARY KEY,
        marketplace           TEXT NOT NULL,
        start_date            TEXT NOT NULL,
        end_date              TEXT NOT NULL,
        department_name       TEXT NOT NULL,
        search_term           TEXT NOT NULL,
        search_frequency_rank INTEGER,
        clicked_asin          TEXT NOT NULL DEFAULT '',
        click_share_rank      INTEGER,
        click_share           DOUBLE PRECISION,
        conversion_share      DOUBLE PRECISION,
        downloaded_at         TEXT NOT NULL,
        UNIQUE (marketplace, start_date, end_date, department_name, search_term, clicked_asin)
    )
    """,

    # ── Market Basket ─────────────────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT
    # Canonical for: cross-sell signals, bundle opportunities
    """
    CREATE TABLE IF NOT EXISTS market_basket (
        id                  SERIAL PRIMARY KEY,
        marketplace         TEXT NOT NULL,
        asin                TEXT NOT NULL,
        start_date          TEXT NOT NULL,
        end_date            TEXT NOT NULL,
        purchased_with_asin TEXT NOT NULL,
        purchased_with_rank INTEGER,
        combination_pct     DOUBLE PRECISION,
        downloaded_at       TEXT NOT NULL,
        UNIQUE (marketplace, asin, start_date, end_date, purchased_with_asin)
    )
    """,

    # ── Repeat Purchase ───────────────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT
    # Canonical for: LTV / retention metrics per ASIN
    """
    CREATE TABLE IF NOT EXISTS repeat_purchase (
        id                          SERIAL PRIMARY KEY,
        marketplace                 TEXT NOT NULL,
        asin                        TEXT NOT NULL,
        start_date                  TEXT NOT NULL,
        end_date                    TEXT NOT NULL,
        orders                      INTEGER,
        unique_customers            INTEGER,
        repeat_customers_pct        DOUBLE PRECISION,
        repeat_purchase_revenue     DOUBLE PRECISION,
        repeat_purchase_revenue_pct DOUBLE PRECISION,
        currency                    TEXT,
        downloaded_at               TEXT NOT NULL,
        UNIQUE (marketplace, asin, start_date, end_date)
    )
    """,

    # ── Promotions ────────────────────────────────────────────────────────────
    # Source: GET_PROMOTION_PERFORMANCE_REPORT
    # Canonical for: promotion-level revenue lift, units sold, discount type
    # One row per (promotion, ASIN) pair
    """
    CREATE TABLE IF NOT EXISTS promotions (
        id             SERIAL PRIMARY KEY,
        marketplace    TEXT NOT NULL,
        promotion_id   TEXT NOT NULL,
        promotion_name TEXT,
        type           TEXT,
        status         TEXT,
        asin           TEXT NOT NULL DEFAULT '',
        product_name   TEXT,
        glance_views   INTEGER,
        units_sold     INTEGER,
        revenue        DOUBLE PRECISION,
        currency       TEXT,
        start_date     TEXT,
        end_date       TEXT,
        downloaded_at  TEXT NOT NULL,
        UNIQUE (marketplace, promotion_id, asin)
    )
    """,

    # ── Coupons ───────────────────────────────────────────────────────────────
    # Source: GET_COUPON_PERFORMANCE_REPORT
    # Canonical for: coupon clip/redemption rate, revenue per coupon
    """
    CREATE TABLE IF NOT EXISTS coupons (
        id            SERIAL PRIMARY KEY,
        marketplace   TEXT NOT NULL,
        campaign_id   TEXT NOT NULL,
        campaign_name TEXT,
        coupon_id     TEXT NOT NULL,
        coupon_name   TEXT,
        start_date    TEXT,
        end_date      TEXT,
        clips         INTEGER,
        redemptions   INTEGER,
        revenue       DOUBLE PRECISION,
        currency      TEXT,
        budget        DOUBLE PRECISION,
        downloaded_at TEXT NOT NULL,
        UNIQUE (marketplace, campaign_id, coupon_id)
    )
    """,

    # ── Returns ───────────────────────────────────────────────────────────────
    # Source: GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE
    # Canonical for: return reason, quantity, date per ASIN
    """
    CREATE TABLE IF NOT EXISTS returns (
        id            SERIAL PRIMARY KEY,
        marketplace   TEXT NOT NULL,
        order_id      TEXT NOT NULL,
        sku           TEXT NOT NULL DEFAULT '',
        asin          TEXT,
        return_date   TEXT,
        quantity      INTEGER,
        reason        TEXT,
        status        TEXT,
        disposition   TEXT,
        downloaded_at TEXT NOT NULL,
        UNIQUE (marketplace, order_id, sku, return_date)
    )
    """,
]


def init_db():
    """Create all tables if they don't exist. Safe to call repeatedly."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in TABLES:
                cur.execute(sql)
        conn.commit()
    print('Database schema initialized (10 tables).')
