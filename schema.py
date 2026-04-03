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

    # ══════════════════════════════════════════════════════════════════════════
    # Tier 4 — Advertising tables (loaded from Excel/CSV by db/load_ads.py)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Ad Campaigns (SP Campaign + SP Budget + SB Campaign + SD Campaign) ───
    """
    CREATE TABLE IF NOT EXISTS ads_campaigns (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        attribution_window      TEXT,
        status                  TEXT,
        portfolio_name          TEXT,
        budget                  DOUBLE PRECISION,
        spend                   DOUBLE PRECISION,
        impressions             INTEGER,
        clicks                  INTEGER,
        ctr                     DOUBLE PRECISION,
        cpc                     DOUBLE PRECISION,
        orders                  INTEGER,
        units                   INTEGER,
        sales                   DOUBLE PRECISION,
        acos                    DOUBLE PRECISION,
        roas                    DOUBLE PRECISION,
        cvr                     DOUBLE PRECISION,
        bidding_strategy        TEXT,
        targeting_type          TEXT,
        recommended_budget      DOUBLE PRECISION,
        avg_time_in_budget      DOUBLE PRECISION,
        est_missed_imp_lower    DOUBLE PRECISION,
        est_missed_imp_upper    DOUBLE PRECISION,
        est_missed_clicks_lower DOUBLE PRECISION,
        est_missed_clicks_upper DOUBLE PRECISION,
        est_missed_sales_lower  DOUBLE PRECISION,
        est_missed_sales_upper  DOUBLE PRECISION,
        ntb_orders              INTEGER,
        ntb_sales               DOUBLE PRECISION,
        ntb_units               INTEGER,
        ntb_order_pct           DOUBLE PRECISION,
        ntb_sales_pct           DOUBLE PRECISION,
        branded_searches        INTEGER,
        dpv                     INTEGER,
        video_complete_views    INTEGER,
        video_completion_rate   DOUBLE PRECISION,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name)
    )
    """,

    # ── Ad Search Terms (SP + SB search term + impression share merged) ──────
    """
    CREATE TABLE IF NOT EXISTS ads_search_terms (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        ad_group_name           TEXT NOT NULL DEFAULT '',
        targeting_text          TEXT NOT NULL DEFAULT '',
        match_type              TEXT,
        customer_search_term    TEXT NOT NULL,
        attribution_window      TEXT,
        impressions             INTEGER,
        clicks                  INTEGER,
        ctr                     DOUBLE PRECISION,
        cpc                     DOUBLE PRECISION,
        spend                   DOUBLE PRECISION,
        orders                  INTEGER,
        units                   INTEGER,
        sales                   DOUBLE PRECISION,
        acos                    DOUBLE PRECISION,
        roas                    DOUBLE PRECISION,
        cvr                     DOUBLE PRECISION,
        impression_rank         INTEGER,
        impression_share        DOUBLE PRECISION,
        own_sku_units           INTEGER,
        own_sku_sales           DOUBLE PRECISION,
        other_sku_units         INTEGER,
        other_sku_sales         DOUBLE PRECISION,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name, ad_group_name,
                     targeting_text, customer_search_term)
    )
    """,

    # ── Ad Targeting (SP Targeting + SB Keyword + SD Targeting + SP Audience) ─
    """
    CREATE TABLE IF NOT EXISTS ads_targeting (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        ad_group_name           TEXT NOT NULL DEFAULT '',
        targeting_text          TEXT NOT NULL,
        match_type              TEXT NOT NULL DEFAULT '',
        attribution_window      TEXT,
        targeting_type          TEXT,
        impressions             INTEGER,
        clicks                  INTEGER,
        ctr                     DOUBLE PRECISION,
        cpc                     DOUBLE PRECISION,
        spend                   DOUBLE PRECISION,
        orders                  INTEGER,
        units                   INTEGER,
        sales                   DOUBLE PRECISION,
        acos                    DOUBLE PRECISION,
        roas                    DOUBLE PRECISION,
        cvr                     DOUBLE PRECISION,
        top_of_search_imp_share DOUBLE PRECISION,
        own_sku_units           INTEGER,
        own_sku_sales           DOUBLE PRECISION,
        other_sku_units         INTEGER,
        other_sku_sales         DOUBLE PRECISION,
        ntb_orders              INTEGER,
        ntb_sales               DOUBLE PRECISION,
        branded_searches        INTEGER,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name, ad_group_name,
                     targeting_text, match_type)
    )
    """,

    # ── Ad Products (SP + SD Advertised Product) ─────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ads_products (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        ad_group_name           TEXT NOT NULL DEFAULT '',
        asin                    TEXT NOT NULL,
        sku                     TEXT,
        attribution_window      TEXT,
        impressions             INTEGER,
        clicks                  INTEGER,
        ctr                     DOUBLE PRECISION,
        cpc                     DOUBLE PRECISION,
        spend                   DOUBLE PRECISION,
        orders                  INTEGER,
        units                   INTEGER,
        sales                   DOUBLE PRECISION,
        acos                    DOUBLE PRECISION,
        roas                    DOUBLE PRECISION,
        cvr                     DOUBLE PRECISION,
        own_sku_units           INTEGER,
        own_sku_sales           DOUBLE PRECISION,
        other_sku_units         INTEGER,
        other_sku_sales         DOUBLE PRECISION,
        ntb_orders              INTEGER,
        ntb_sales               DOUBLE PRECISION,
        ntb_units               INTEGER,
        dpv                     INTEGER,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name, ad_group_name, asin)
    )
    """,

    # ── Ad Placements (SP Placement + SB Campaign/Keyword Placement) ─────────
    """
    CREATE TABLE IF NOT EXISTS ads_placements (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        placement               TEXT NOT NULL,
        targeting_text          TEXT NOT NULL DEFAULT '',
        match_type              TEXT NOT NULL DEFAULT '',
        attribution_window      TEXT,
        impressions             INTEGER,
        clicks                  INTEGER,
        ctr                     DOUBLE PRECISION,
        cpc                     DOUBLE PRECISION,
        spend                   DOUBLE PRECISION,
        orders                  INTEGER,
        units                   INTEGER,
        sales                   DOUBLE PRECISION,
        acos                    DOUBLE PRECISION,
        roas                    DOUBLE PRECISION,
        ntb_orders              INTEGER,
        ntb_sales               DOUBLE PRECISION,
        branded_searches        INTEGER,
        video_complete_views    INTEGER,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name, placement,
                     targeting_text, match_type)
    )
    """,

    # ── Ad Purchased Products (SP halo/cross-sell) ───────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ads_purchased_products (
        month                   TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        ad_group_name           TEXT NOT NULL DEFAULT '',
        advertised_asin         TEXT NOT NULL,
        purchased_asin          TEXT NOT NULL,
        targeting_text          TEXT NOT NULL DEFAULT '',
        match_type              TEXT,
        other_sku_units         INTEGER,
        other_sku_orders        INTEGER,
        other_sku_sales         DOUBLE PRECISION,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, campaign_name, advertised_asin,
                     purchased_asin, targeting_text)
    )
    """,

    # ── Ad Benchmarks (cross-channel + SB category benchmark) ────────────────
    """
    CREATE TABLE IF NOT EXISTS ads_benchmarks (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        category                TEXT NOT NULL DEFAULT '',
        brand                   TEXT NOT NULL DEFAULT '',
        metric_name             TEXT NOT NULL,
        your_value              DOUBLE PRECISION,
        p25                     DOUBLE PRECISION,
        p50                     DOUBLE PRECISION,
        p75                     DOUBLE PRECISION,
        peer_set_size           INTEGER,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, category, brand, metric_name)
    )
    """,

    # ── Ad Invalid Traffic (SP + SB + SD gross/invalid traffic) ──────────────
    """
    CREATE TABLE IF NOT EXISTS ads_invalid_traffic (
        month                   TEXT NOT NULL,
        ad_type                 TEXT NOT NULL,
        campaign_name           TEXT NOT NULL,
        gross_impressions       INTEGER,
        impressions             INTEGER,
        invalid_impressions     INTEGER,
        invalid_impression_rate DOUBLE PRECISION,
        gross_clicks            INTEGER,
        clicks                  INTEGER,
        invalid_clicks          INTEGER,
        invalid_click_rate      DOUBLE PRECISION,
        pulled_at               TEXT NOT NULL,
        PRIMARY KEY (month, ad_type, campaign_name)
    )
    """,

    # ══════════════════════════════════════════════════════════════════════════
    # Tier 5 — Content optimization (built by db/build_content_briefs.py
    #          and db/build_listing_recommendations.py)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Content Briefs (tiered keywords per ASIN) ────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS content_briefs (
        asin                TEXT NOT NULL,
        search_query        TEXT NOT NULL,
        content_brief_score DOUBLE PRECISION,
        content_tier        TEXT NOT NULL,
        tier_rank           INTEGER NOT NULL,
        search_volume       INTEGER,
        keyword_relevance   DOUBLE PRECISION,
        keyword_role        TEXT,
        keyword_type        TEXT,
        strategy            TEXT,
        cvr_index           DOUBLE PRECISION,
        click_share         DOUBLE PRECISION,
        purchase_share      DOUBLE PRECISION,
        revenue_score       DOUBLE PRECISION,
        headroom_pct        DOUBLE PRECISION,
        momentum_pct        DOUBLE PRECISION,
        share_trend         DOUBLE PRECISION,
        built_at            TEXT NOT NULL,
        PRIMARY KEY (asin, search_query)
    )
    """,

    # ── Listing Recommendations (generated copy per ASIN) ────────────────────
    """
    CREATE TABLE IF NOT EXISTS listing_recommendations (
        asin                    TEXT NOT NULL PRIMARY KEY,
        rec_title               TEXT,
        rec_title_chars         INTEGER,
        rec_bullets             TEXT,
        rec_description         TEXT,
        rec_description_chars   INTEGER,
        rec_backend_terms       TEXT,
        rec_qa_seeds            TEXT,
        title_keywords_used     INTEGER,
        title_keywords_total    INTEGER,
        bullet_keywords_used    INTEGER,
        bullet_keywords_total   INTEGER,
        total_volume_covered    INTEGER,
        total_volume_available  INTEGER,
        current_title           TEXT,
        built_at                TEXT NOT NULL
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

    # Ads — Search Terms (main join key to organic data)
    "CREATE INDEX IF NOT EXISTS idx_ast_term    ON ads_search_terms (customer_search_term)",
    "CREATE INDEX IF NOT EXISTS idx_ast_month   ON ads_search_terms (month)",
    "CREATE INDEX IF NOT EXISTS idx_ast_adtype  ON ads_search_terms (ad_type)",
    "CREATE INDEX IF NOT EXISTS idx_ast_camp    ON ads_search_terms (campaign_name)",

    # Ads — Campaigns
    "CREATE INDEX IF NOT EXISTS idx_ac_month    ON ads_campaigns (month)",
    "CREATE INDEX IF NOT EXISTS idx_ac_adtype   ON ads_campaigns (ad_type)",

    # Ads — Targeting
    "CREATE INDEX IF NOT EXISTS idx_at_month    ON ads_targeting (month)",
    "CREATE INDEX IF NOT EXISTS idx_at_target   ON ads_targeting (targeting_text)",

    # Ads — Products
    "CREATE INDEX IF NOT EXISTS idx_ap_month    ON ads_products (month)",
    "CREATE INDEX IF NOT EXISTS idx_ap_asin     ON ads_products (asin)",

    # Ads — Placements
    "CREATE INDEX IF NOT EXISTS idx_apl_month   ON ads_placements (month)",
    "CREATE INDEX IF NOT EXISTS idx_apl_place   ON ads_placements (placement)",

    # Ads — Purchased Products
    "CREATE INDEX IF NOT EXISTS idx_app_advasin ON ads_purchased_products (advertised_asin)",
    "CREATE INDEX IF NOT EXISTS idx_app_purasin ON ads_purchased_products (purchased_asin)",

    # Content Briefs
    "CREATE INDEX IF NOT EXISTS idx_cb_asin     ON content_briefs (asin)",
    "CREATE INDEX IF NOT EXISTS idx_cb_tier     ON content_briefs (content_tier)",
    "CREATE INDEX IF NOT EXISTS idx_cb_query    ON content_briefs (search_query)",

    # Listing Recommendations
    "CREATE INDEX IF NOT EXISTS idx_lr_asin     ON listing_recommendations (asin)",
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
