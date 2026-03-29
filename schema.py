"""
Database schema for the Amazon business database (Supabase / PostgreSQL).

5 tables:
  sales_and_traffic          — revenue, units, sessions, buy box, conversion per ASIN
  sqp_report                 — per-ASIN per-query search funnel
  search_catalog_performance — catalog-wide search funnel per ASIN
  market_basket              — cross-sell / co-purchase signals
  repeat_purchase            — LTV / retention per ASIN

Call init_db() at startup to ensure all tables exist.
Call get_conn() to get a live psycopg2 connection.
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
    # Source: GET_SALES_AND_TRAFFIC_REPORT
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
]


def init_db():
    """Create all tables if they don't exist. Safe to call repeatedly."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in TABLES:
                cur.execute(sql)
        conn.commit()
    print('Database schema initialized (5 tables).')
