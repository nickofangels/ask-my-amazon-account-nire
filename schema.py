"""
Database schema for the Amazon business database (Supabase / PostgreSQL).

Tables are organized into tiers:

  Tier 1 — Derived (loaded from raw JSON files by db/load.py):
    sales_traffic_asin          — monthly per-ASIN sales & traffic
    search_query_performance    — per-ASIN per-query search funnel
    catalog_performance         — catalog-wide search funnel per ASIN
    repeat_purchase             — LTV / retention per ASIN
    market_basket               — cross-sell / co-purchase signals
    search_terms                — marketplace-wide search terms (filtered to brand ASINs)
    listings                    — current ASIN/SKU catalog snapshot

  Tier 2 — Scored (built by db/build_*.py):
    asin_keyword_scores         — full ASIN-keyword scoring matrix
    keyword_targets             — keyword-level aggregation & strategy

  Tier 3 — Meta:
    keyword_goals               — user-set keyword targets (dashboard-edited)
    period_meta                 — L52/P52 period boundaries
    pull_log                    — audit trail of all data loads
    data_coverage               — row counts per table/month

Call init_db() at startup to ensure all tables + indexes exist.
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


# ══════════════════════════════════════════════════════════════════════════════
# Tier 1 — Derived tables (loaded from raw JSON files by db/load.py)
# ══════════════════════════════════════════════════════════════════════════════

TABLES = [
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

    # ── Sales & Traffic — Monthly per-ASIN ────────────────────────────────────
    # Source: GET_SALES_AND_TRAFFIC_REPORT (salesAndTrafficByAsin)
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

    # ── Search Query Performance ──────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT
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

    # ── Catalog Performance ───────────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT
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

    # ── Repeat Purchase ───────────────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT
    """
    CREATE TABLE IF NOT EXISTS repeat_purchase (
        month                       TEXT NOT NULL,
        asin                        TEXT NOT NULL,
        period                      TEXT,
        orders                      INTEGER,
        unique_customers            INTEGER,
        repeat_customers_pct        DOUBLE PRECISION,
        repeat_purchase_revenue     DOUBLE PRECISION,
        repeat_purchase_revenue_pct DOUBLE PRECISION,
        pulled_at                   TEXT NOT NULL,
        PRIMARY KEY (month, asin)
    )
    """,

    # ── Market Basket ─────────────────────────────────────────────────────────
    # Source: GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT
    """
    CREATE TABLE IF NOT EXISTS market_basket (
        month               TEXT NOT NULL,
        asin                TEXT NOT NULL,
        purchased_with_asin TEXT NOT NULL,
        purchased_with_rank INTEGER,
        combination_pct     DOUBLE PRECISION,
        period              TEXT,
        pulled_at           TEXT NOT NULL,
        PRIMARY KEY (month, asin, purchased_with_asin)
    )
    """,

    # ══════════════════════════════════════════════════════════════════════════
    # Tier 2 — Scored tables (built by db/build_*.py)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Keyword Targets (built by build_keywords.py) ──────────────────────────
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

    # ══════════════════════════════════════════════════════════════════════════
    # Tier 3 — Meta tables
    # ══════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# Indexes — following DWC's pattern: index every column used in
# WHERE, JOIN, GROUP BY across dashboard.py, build_*.py, and load.py.
# ══════════════════════════════════════════════════════════════════════════════

INDEXES = [
    # Listings
    "CREATE INDEX IF NOT EXISTS idx_listings_asin ON listings (asin)",

    # Sales Traffic ASIN
    "CREATE INDEX IF NOT EXISTS idx_sta_asin   ON sales_traffic_asin (asin)",
    "CREATE INDEX IF NOT EXISTS idx_sta_period ON sales_traffic_asin (period)",
    "CREATE INDEX IF NOT EXISTS idx_sta_month  ON sales_traffic_asin (month)",

    # Search Terms
    "CREATE INDEX IF NOT EXISTS idx_st_asin   ON search_terms (clicked_asin)",
    "CREATE INDEX IF NOT EXISTS idx_st_term   ON search_terms (search_term)",
    "CREATE INDEX IF NOT EXISTS idx_st_period ON search_terms (period)",
    "CREATE INDEX IF NOT EXISTS idx_st_month  ON search_terms (month)",

    # Search Query Performance
    "CREATE INDEX IF NOT EXISTS idx_sqp_asin   ON search_query_performance (asin)",
    "CREATE INDEX IF NOT EXISTS idx_sqp_query  ON search_query_performance (search_query)",
    "CREATE INDEX IF NOT EXISTS idx_sqp_period ON search_query_performance (period)",
    "CREATE INDEX IF NOT EXISTS idx_sqp_month  ON search_query_performance (month)",

    # Catalog Performance
    "CREATE INDEX IF NOT EXISTS idx_cp_asin   ON catalog_performance (asin)",
    "CREATE INDEX IF NOT EXISTS idx_cp_period ON catalog_performance (period)",
    "CREATE INDEX IF NOT EXISTS idx_cp_month  ON catalog_performance (month)",

    # Repeat Purchase
    "CREATE INDEX IF NOT EXISTS idx_rp_asin   ON repeat_purchase (asin)",
    "CREATE INDEX IF NOT EXISTS idx_rp_period ON repeat_purchase (period)",
    "CREATE INDEX IF NOT EXISTS idx_rp_month  ON repeat_purchase (month)",

    # Market Basket
    "CREATE INDEX IF NOT EXISTS idx_mb_asin   ON market_basket (asin)",
    "CREATE INDEX IF NOT EXISTS idx_mb_period ON market_basket (period)",
    "CREATE INDEX IF NOT EXISTS idx_mb_month  ON market_basket (month)",

    # ASIN Keyword Scores
    "CREATE INDEX IF NOT EXISTS idx_aks_asin     ON asin_keyword_scores (asin)",
    "CREATE INDEX IF NOT EXISTS idx_aks_query    ON asin_keyword_scores (search_query)",
    "CREATE INDEX IF NOT EXISTS idx_aks_role     ON asin_keyword_scores (keyword_role)",
    "CREATE INDEX IF NOT EXISTS idx_aks_type     ON asin_keyword_scores (keyword_type)",
    "CREATE INDEX IF NOT EXISTS idx_aks_kw_rel   ON asin_keyword_scores (keyword_relevance)",
    "CREATE INDEX IF NOT EXISTS idx_aks_asin_pri ON asin_keyword_scores (asin_priority)",

    # Keyword Targets
    "CREATE INDEX IF NOT EXISTS idx_kt_strategy ON keyword_targets (strategy)",
    "CREATE INDEX IF NOT EXISTS idx_kt_volume   ON keyword_targets (volume)",
    "CREATE INDEX IF NOT EXISTS idx_kt_cvr_idx  ON keyword_targets (cvr_index)",
]


def init_db():
    """Create all tables and indexes if they don't exist. Safe to call repeatedly."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in TABLES:
                cur.execute(sql)
            for sql in INDEXES:
                cur.execute(sql)
        conn.commit()
    print(f'Database schema initialized ({len(TABLES)} tables, {len(INDEXES)} indexes).')
