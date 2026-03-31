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
    # ══════════════════════════════════════════════════════════════════════════
    # Derived / Dashboard tables (created by transform.py and build scripts)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Listings ──────────────────────────────────────────────────────────────
    # Source: GET_MERCHANT_LISTINGS_ALL_DATA
    """
    CREATE TABLE IF NOT EXISTS listings (
        asin                TEXT NOT NULL,
        sku                 TEXT NOT NULL DEFAULT '',
        product_name        TEXT,
        price               TEXT,
        quantity            TEXT,
        open_date           TEXT,
        status              TEXT,
        fulfillment         TEXT,
        pulled_at           TEXT NOT NULL,
        PRIMARY KEY (asin, sku)
    )
    """,

    # ── Sales & Traffic — Daily brand totals ──────────────────────────────────
    # Source: GET_SALES_AND_TRAFFIC_REPORT (daily granularity)
    """
    CREATE TABLE IF NOT EXISTS sales_traffic_daily (
        date                TEXT NOT NULL PRIMARY KEY,
        period              TEXT,
        revenue             DOUBLE PRECISION,
        units               INTEGER,
        total_order_items   INTEGER,
        units_refunded      INTEGER,
        sessions            INTEGER,
        page_views          INTEGER,
        buy_box_pct         DOUBLE PRECISION,
        conversion_rate     DOUBLE PRECISION,
        pulled_at           TEXT NOT NULL
    )
    """,

    # ── Sales & Traffic — Monthly per-ASIN (derived from sales_and_traffic) ──
    """
    CREATE TABLE IF NOT EXISTS sales_traffic_asin (
        month               TEXT NOT NULL,
        asin                TEXT NOT NULL,
        parent_asin         TEXT,
        period              TEXT,
        units               INTEGER,
        revenue             DOUBLE PRECISION,
        sessions            INTEGER,
        page_views          INTEGER,
        conversion_rate     DOUBLE PRECISION,
        buy_box_pct         DOUBLE PRECISION,
        pulled_at           TEXT NOT NULL,
        PRIMARY KEY (month, asin)
    )
    """,

    # ── Search Terms (Brand Analytics) ────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT
    """
    CREATE TABLE IF NOT EXISTS search_terms (
        month               TEXT NOT NULL,
        search_term         TEXT NOT NULL,
        search_freq_rank    INTEGER,
        clicked_asin        TEXT NOT NULL,
        product_title       TEXT,
        click_share_rank    INTEGER,
        click_share         DOUBLE PRECISION,
        conversion_share    DOUBLE PRECISION,
        period              TEXT,
        pulled_at           TEXT NOT NULL,
        PRIMARY KEY (month, search_term, clicked_asin)
    )
    """,

    # ── Search Query Performance (derived from sqp_report) ────────────────────
    """
    CREATE TABLE IF NOT EXISTS search_query_performance (
        month                   TEXT NOT NULL,
        asin                    TEXT NOT NULL,
        search_query            TEXT NOT NULL,
        search_query_score      INTEGER,
        search_query_volume     INTEGER,
        total_impressions       INTEGER,
        asin_impressions        INTEGER,
        asin_impression_share   DOUBLE PRECISION,
        total_clicks            INTEGER,
        total_click_rate        DOUBLE PRECISION,
        asin_clicks             INTEGER,
        asin_click_share        DOUBLE PRECISION,
        total_cart_adds         INTEGER,
        asin_cart_adds          INTEGER,
        asin_cart_add_share     DOUBLE PRECISION,
        total_purchases         INTEGER,
        asin_purchases          INTEGER,
        asin_purchase_share     DOUBLE PRECISION,
        period                  TEXT,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, asin, search_query)
    )
    """,

    # ── Catalog Performance (derived from search_catalog_performance) ─────────
    """
    CREATE TABLE IF NOT EXISTS catalog_performance (
        month                   TEXT NOT NULL,
        asin                    TEXT NOT NULL,
        impressions             INTEGER,
        clicks                  INTEGER,
        click_rate              DOUBLE PRECISION,
        cart_adds               INTEGER,
        purchases               INTEGER,
        conversion_rate         DOUBLE PRECISION,
        search_traffic_sales    DOUBLE PRECISION,
        period                  TEXT,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, asin)
    )
    """,

    # ── Keyword Targets (built by build_keywords.py) ──────────────────────────
    # brand_* columns = Nire Beauty metrics (dwc_* in DWC reference project)
    """
    CREATE TABLE IF NOT EXISTS keyword_targets (
        search_query        TEXT NOT NULL PRIMARY KEY,
        volume              INTEGER,
        vol_tier            TEXT,
        brand_clicks        INTEGER,
        brand_purchases     INTEGER,
        brand_click_share   DOUBLE PRECISION,
        brand_purchase_share DOUBLE PRECISION,
        mkt_clicks          INTEGER,
        mkt_purchases       INTEGER,
        mkt_cvr             DOUBLE PRECISION,
        brand_cvr           DOUBLE PRECISION,
        cvr_index           DOUBLE PRECISION,
        brand_impressions   INTEGER,
        mkt_impressions     INTEGER,
        brand_ctr           DOUBLE PRECISION,
        mkt_ctr             DOUBLE PRECISION,
        ctr_index           DOUBLE PRECISION,
        hero_asin           TEXT,
        hero_cvr            DOUBLE PRECISION,
        hero_aov            DOUBLE PRECISION,
        hero_revenue_score  DOUBLE PRECISION,
        strategy            TEXT,
        keyword_type        TEXT,
        asin_count          INTEGER,
        cannibalization_flag INTEGER,
        share_trend         DOUBLE PRECISION,
        months_of_data      INTEGER,
        built_at            TEXT NOT NULL
    )
    """,

    # ── ASIN-Keyword Scores (built by build_asin_keywords.py) ─────────────────
    """
    CREATE TABLE IF NOT EXISTS asin_keyword_scores (
        asin                    TEXT NOT NULL,
        search_query            TEXT NOT NULL,

        asin_impressions        INTEGER,
        asin_clicks             INTEGER,
        asin_cart_adds          INTEGER,
        asin_purchases          INTEGER,
        mkt_clicks              INTEGER,
        mkt_purchases           INTEGER,
        search_volume           INTEGER,

        asin_cvr                DOUBLE PRECISION,
        adjusted_cvr            DOUBLE PRECISION,
        mkt_cvr                 DOUBLE PRECISION,
        cvr_index               DOUBLE PRECISION,
        click_share             DOUBLE PRECISION,
        purchase_share          DOUBLE PRECISION,
        aov                     DOUBLE PRECISION,
        revenue_score           DOUBLE PRECISION,

        within_asin_traffic_pct DOUBLE PRECISION,
        within_asin_revenue_pct DOUBLE PRECISION,

        within_kw_dominance_pct DOUBLE PRECISION,
        within_kw_cvr_pct       DOUBLE PRECISION,
        within_kw_aov_pct       DOUBLE PRECISION,

        volume_pct              DOUBLE PRECISION,
        cvr_advantage_pct       DOUBLE PRECISION,
        headroom_pct            DOUBLE PRECISION,
        momentum_pct            DOUBLE PRECISION,

        keyword_relevance       DOUBLE PRECISION,
        asin_priority           DOUBLE PRECISION,

        keyword_type            TEXT,
        keyword_role            TEXT,

        share_trend             DOUBLE PRECISION,

        built_at                TEXT NOT NULL,
        PRIMARY KEY (asin, search_query)
    )
    """,

    # ── Keyword Goals (user-set targets via dashboard UI) ─────────────────────
    """
    CREATE TABLE IF NOT EXISTS keyword_goals (
        search_query            TEXT NOT NULL PRIMARY KEY,
        target_purchase_share   DOUBLE PRECISION,
        priority                TEXT,
        notes                   TEXT,
        updated_at              TEXT NOT NULL
    )
    """,

    # ── Period Metadata ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS period_meta (
        period      TEXT PRIMARY KEY,
        start_date  TEXT NOT NULL,
        end_date    TEXT NOT NULL,
        label       TEXT NOT NULL
    )
    """,

    # ── Pull Log (audit trail) ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS pull_log (
        id          SERIAL PRIMARY KEY,
        report_type TEXT NOT NULL,
        month       TEXT,
        source_file TEXT,
        row_count   INTEGER,
        pulled_at   TEXT NOT NULL
    )
    """,

    # ── Data Coverage ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS data_coverage (
        data_type    TEXT NOT NULL,
        month        TEXT NOT NULL,
        period       TEXT NOT NULL,
        row_count    INTEGER NOT NULL DEFAULT 0,
        is_complete  BOOLEAN NOT NULL DEFAULT FALSE,
        days_covered INTEGER,
        PRIMARY KEY (data_type, month)
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
    print(f'Database schema initialized ({len(TABLES)} tables).')
