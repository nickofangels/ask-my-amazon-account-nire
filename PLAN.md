# Nire Beauty Dashboard — Implementation Plan

## Context

Erin's project (`ask-my-amazon-account-nire`) has a working SP-API backfill pipeline that loads 5 report types into Supabase PostgreSQL, but no analytics dashboard. The goal is to port the DWC Big Analysis dashboard (`dwc-big-analysis/dashboard.py`) nearly 1-for-1, adapting it for:

- **PostgreSQL/Supabase** instead of SQLite
- **Nire Beauty makeup brushes** instead of Death Wish Coffee
- **No Subscribe & Save** (not applicable to this brand)
- **No Coffee Market tab** (domain-specific to DWC)

The DWC dashboard is a 4099-line Flask app with 8 tabs, 25+ API endpoints, Chart.js visualizations, and a keyword intelligence system. We're porting 6 of the 8 tabs.

**Reference project:** `/Users/nickdeangelo/Documents/GitHub/dwc-big-analysis/`

---

## Known Risks & Gotchas (from audit)

### 1. SQP API Access — START THIS FIRST
Erin's SQP reports keep erroring out. This is the #1 blocker because:
- SQP data feeds the entire Keywords tab (the most complex part of the dashboard)
- SQP requires Brand Registry + Brand Analytics enrollment + possibly Role ARN
- Debugging API access is trial-and-error and can take hours/days
- **Action:** Kick off SQP debugging in Phase 0 so it runs in parallel with code work

### 2. DWC's `load.py` Does NOT Port to Nire
DWC's load.py reads raw JSON files from disk → SQLite. Nire has no JSON files — `backfill.py` streams API data directly into Supabase tables. **Do not port load.py.** Instead, write a lightweight `db/transform.py` that:
- Reads from the 5 existing raw Supabase tables
- Derives `month` from `start_date` (Nire stores date ranges, not month labels)
- Assigns `period` tags (L52/P52) using config.py
- Writes to dashboard-ready derived tables

### 3. `month` Column Doesn't Exist in Nire
The DWC dashboard queries `WHERE month = ...` and `GROUP BY month` everywhere. Nire's tables use `start_date`/`end_date` instead. Every dashboard query needs `TO_CHAR(start_date::DATE, 'YYYY-MM') AS month` or the transform layer must add a `month` column to derived tables. **The transform layer approach is cleaner** — derive it once, query it simply.

### 4. `period` Column Doesn't Exist in Nire
Same issue. DWC pre-tags every row with `period IN ('L52','P52')`. Nire's raw tables have no period column. The transform layer must compute this from date ranges using `config.period_label()`.

### 5. Column Name Mismatches (Will Cause Silent Bugs)
These will bite you if you forget any:

| DWC column | Nire raw column | Tables affected |
|---|---|---|
| `revenue` | `ordered_product_sales` | sales_and_traffic |
| `units` | `units_ordered` | sales_and_traffic |
| `buy_box_pct` | `buy_box_percentage` | sales_and_traffic |
| `conversion_rate` | `unit_session_percentage` | sales_and_traffic |
| `impressions` | `impression_count` | search_catalog_performance |
| `clicks` | `click_count` | search_catalog_performance |
| `cart_adds` | `cart_add_count` | search_catalog_performance |
| `purchases` | `purchase_count` | search_catalog_performance |
| `repeat_revenue` | `repeat_purchase_revenue` | repeat_purchase |
| `repeat_revenue_pct` | `repeat_purchase_revenue_pct` | repeat_purchase |

**Decision:** The transform layer should rename columns to match DWC names in derived tables, so dashboard SQL can be ported with minimal changes.

### 6. Dashboard Queries 13+ Tables — Only 5 Exist
The dashboard references tables that must be BUILT, not just pulled:
- `keyword_targets` — built by `build_keywords.py` from SQP data
- `asin_keyword_scores` — built by `build_asin_keywords.py` from SQP + sales data
- `keyword_goals` — empty table, populated by user via dashboard UI
- `listings` — needs a new pull script
- `sales_traffic_daily` — needs daily-granularity pull (existing backfill is monthly only)
- `sales_traffic_asin` — derived from raw `sales_and_traffic` with period tags + column renames
- `search_terms` — needs a new pull script (Brand Analytics Search Terms report)
- `search_query_performance` — derived from raw `sqp_report` with period tags
- `catalog_perf` / `catalog_performance` — derived from raw `search_catalog_performance`
- `period_meta`, `data_coverage`, `pull_log` — metadata tables

### 7. `build_asin_keywords.py` Depends on AOV from `sales_traffic_asin`
The keyword matrix computes `revenue_score = asin_purchases * aov`. AOV comes from `sales_traffic_asin` (revenue/units per ASIN). If that table is empty or has NULL revenue, the entire percentile ranking cascade breaks. **sales_traffic_asin must be populated before keyword builds.**

### 8. SQLite → PostgreSQL Translation (Mechanical but Pervasive)
Apply everywhere in dashboard.py and db/ scripts:
- `sqlite3.connect(DB_PATH)` → `schema.get_conn()`
- `conn.row_factory = sqlite3.Row` → `psycopg2.extras.RealDictCursor`
- `?` → `%s` (parameter placeholders)
- `INSERT OR REPLACE` → `INSERT ... ON CONFLICT ... DO UPDATE`
- `CAST(x AS REAL)` → `x::FLOAT`
- `GROUP_CONCAT(...)` → `STRING_AGG(..., ',')`
- `IFNULL(x, 0)` → `COALESCE(x, 0)`
- `PRAGMA table_info()` → `information_schema.columns`

### 9. Frontend Has ~50+ "DWC"/"dwc" References
The embedded HTML/JS/CSS references `dwc_click_share`, `dwc_purchases`, DWC branding text, etc. All must become `brand_*` / "Nire Beauty". Miss one and the JS silently gets `undefined` for that field.

### 10. Removing Coffee + Subscriptions Tabs Needs Care
The tab system uses index-based arrays. Removing tabs 7 and 8 requires updating the `TABS` array AND any initialization code that references tab indices. The CSS/JS for those tabs can be deleted but check for shared class names.

---

## Phase 0: API Access & SQP Debugging (DO FIRST — runs in parallel)

**This is the long pole.** Start it immediately so it runs while you build code.

### 0a. Set up `.env` file
```
SP_API_CLIENT_ID=...
SP_API_CLIENT_SECRET=...
SP_API_REFRESH_TOKEN=...
SP_API_ROLE_ARN=...          # likely required for Brand Analytics
MARKETPLACE_ID=ATVPDKIKX0DER
DATABASE_URL=postgresql://...  # Supabase connection string
```

### 0b. Validate basic API access
```bash
python test_auth.py           # basic orders API test
python probe_reports.py       # test all 13 report types — note which are ACCESSIBLE vs NO ACCESS
```

### 0c. Debug SQP access specifically
SQP (`GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT`) is the most fragile report type. Common failure modes:
1. **Missing Brand Registry** — Nire Beauty must be enrolled in Amazon Brand Registry
2. **Missing Role ARN** — Brand Analytics often requires IAM role-based auth
3. **Wrong marketplace** — Verify `ATVPDKIKX0DER` is correct for Erin's account
4. **Quota exceeded** — SQP has aggressive rate limits; single-ASIN requests required
5. **ASIN not in brand** — The ASINs in `SQP_ASINS` must belong to the registered brand

**Test script to create:** `test_sqp.py`
- Attempt a single-ASIN SQP request for 1 recent month
- Log the full error response (not just status code)
- Try with and without Role ARN
- Try different ASINs to isolate which ones work
- This is trial and error — expect multiple iterations

### 0d. Pull listings to discover real ASINs
```bash
python scripts/pull_listings.py  # or fetch_sqp_report.py's fetch_all_asins()
```
- Replace hardcoded `SQP_ASINS` in `backfill.py` with actual Nire Beauty ASINs
- Some ASINs may be inactive or not brand-registered — SQP will fail on those

### 0e. Test other Brand Analytics reports
While SQP is being debugged, test these (they may also fail):
- `GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT` — needed for Search Terms tab
- `GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT` — already in backfill
- `GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT` — already in backfill
- `GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT` — already in backfill

**Success criteria:** `probe_reports.py` shows ACCESSIBLE for all 5 Brand Analytics report types.

---

## Phase 1: Foundation — Config & Period System

### Create `config.py`
- **Reference:** `dwc-big-analysis/config.py`
- Port: L52/P52 rolling period logic, `_monthly_intervals`, `month_to_period`, `period_label`, `PERIOD_META`
- Remove: all DWC paths, SQLite refs, `is_month_complete()` (reads JSON files — not applicable)
- Replace `get_active_asins()`: query `listings` table via `schema.get_conn()` instead of sqlite3
- Rename env var: `NIRE_AS_OF_DATE` (from `DWC_AS_OF_DATE`)

### Verify
```bash
python config.py  # should print L52 and P52 month ranges
```

---

## Phase 2: Schema Extension

Add derived/dashboard tables to `schema.py` while keeping existing 5 raw tables for `backfill.py`.

### Add to `schema.py` TABLES list:

| New Table | Purpose | Key columns to get right |
|---|---|---|
| `listings` | Product catalog | `asin, sku, product_name, price, status, fulfillment` |
| `sales_traffic_daily` | Daily brand totals | `date, period, revenue, units, sessions, page_views, buy_box_pct, conversion_rate` |
| `sales_traffic_asin` | Monthly per-ASIN (derived) | `month, asin, period, units, revenue, sessions, page_views, conversion_rate, buy_box_pct` |
| `search_terms` | Brand Analytics | `month, search_term, clicked_asin, click_share_rank, click_share, conversion_share, period` |
| `search_query_performance` | SQP with period tags (derived) | Same as `sqp_report` + `month, period` columns |
| `catalog_performance` | Catalog perf (derived) | `month, asin, period, impressions, clicks, cart_adds, purchases, conversion_rate, search_traffic_sales` |
| `keyword_targets` | Keyword strategy | Use `brand_*` not `dwc_*` — see `dwc-big-analysis/db/schema.sql` lines 343-376 |
| `asin_keyword_scores` | ASIN-keyword matrix | See `dwc-big-analysis/db/schema.sql` lines 278-336 |
| `keyword_goals` | User targets | `search_query, target_purchase_share, priority, notes` |
| `period_meta` | Period boundaries | `period, start_date, end_date, label` |
| `pull_log` | Audit trail | `report_type, month, row_count, pulled_at` — use `SERIAL` not `AUTOINCREMENT` |
| `data_coverage` | Completeness | `data_type, month, period, row_count, is_complete` — use `BOOLEAN DEFAULT FALSE` not `0` |

**Critical:** Derived tables (`sales_traffic_asin`, `search_query_performance`, `catalog_performance`) use DWC-style column names (not Nire raw names) so dashboard SQL ports cleanly.

### Verify
```bash
python -c "from schema import init_db; init_db()"
# Should print "Database schema initialized (17 tables)."
```

---

## Phase 3: Data Pull Scripts + Transform Layer

### 3a. Create `scripts/api_client.py`
- **Reference:** `dwc-big-analysis/scripts/api_client.py`
- Thin wrapper importing from `auth.py`

### 3b. Create `scripts/pull_listings.py`
- **Reference:** `dwc-big-analysis/scripts/pull_listings.py`
- `GET_MERCHANT_LISTINGS_ALL_DATA` → parse TSV → insert `listings`

### 3c. Create `scripts/pull_search_terms.py`
- **Reference:** `dwc-big-analysis/scripts/pull_search_terms.py`
- `GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT` → insert `search_terms`
- **Depends on:** Brand Analytics API access (tested in Phase 0)

### 3d. Create `scripts/pull_sales_traffic.py`
- **Reference:** `dwc-big-analysis/scripts/pull_sales_traffic.py`
- `GET_SALES_AND_TRAFFIC_REPORT` with **daily granularity** → insert `sales_traffic_daily`
- Note: existing backfill pulls monthly ASIN-level into `sales_and_traffic` — this adds daily brand-level

### 3e. Create `db/transform.py` (NOT a port of load.py)
This is new code. DWC's `load.py` reads JSON files from disk — we don't have those. Instead, `transform.py` reads from Nire's 5 raw Supabase tables and writes to derived dashboard tables:

**Transforms:**
1. `sales_and_traffic` → `sales_traffic_asin`: rename columns (`ordered_product_sales`→`revenue`, `units_ordered`→`units`, etc.), derive `month` from `start_date`, assign `period` via `config.month_to_period()`
2. `sqp_report` → `search_query_performance`: add `month` and `period` columns
3. `search_catalog_performance` → `catalog_performance`: rename columns (`impression_count`→`impressions`, etc.), add `month` and `period`
4. `repeat_purchase` → add `month` and `period` columns (can update in-place or create derived view)
5. `market_basket` → add `month` and `period` columns
6. Populate `period_meta` from `config.PERIOD_META`
7. Populate `data_coverage` by counting rows per table/month

**Key pattern for month derivation:**
```sql
INSERT INTO sales_traffic_asin (month, asin, period, units, revenue, ...)
SELECT TO_CHAR(start_date::DATE, 'YYYY-MM') AS month,
       asin,
       %s AS period,  -- computed in Python via config.month_to_period()
       units_ordered AS units,
       ordered_product_sales AS revenue,
       ...
FROM sales_and_traffic
WHERE start_date >= %s AND end_date <= %s
ON CONFLICT (month, asin) DO UPDATE SET ...
```

### 3f. Create `scripts/pull_all.py`
- Orchestrator: pull_listings → pull_sales_traffic → pull_search_terms → backfill (if needed) → transform → build_keywords

### Verify
```bash
python db/transform.py  # should populate derived tables
# Check row counts: sales_traffic_asin, search_query_performance, catalog_performance
```

---

## Phase 4: Keyword Intelligence

**Depends on:** Phase 3 complete (sales_traffic_asin + search_query_performance populated)

### 4a. Create `db/utils.py`
- **Reference:** `dwc-big-analysis/db/utils.py`
- Port `percentile_ranks()` — pure Python, copies directly
- Port `safe_pct()` — copies directly
- Port `_add_months()` — copies directly
- Port `trend_windows()` — change `sqlite3.Connection` to psycopg2 connection, query `search_query_performance` table
- Rewrite `keyword_type()` for Nire Beauty:
  - **Branded:** `nire`, `nire beauty`
  - **Competitor:** `real techniques`, `sigma`, `morphe`, `bh cosmetics`, `jessup`, `bs-mall`, `docolor`, `bestope`
  - **Category:** everything else

### 4b. Create `db/build_asin_keywords.py`
- **Reference:** `dwc-big-analysis/db/build_asin_keywords.py`
- **Must run AFTER** `sales_traffic_asin` is populated (AOV dependency)
- SQL changes: `?`→`%s`, `INSERT OR REPLACE`→`ON CONFLICT DO UPDATE`, `PRAGMA`→`information_schema`
- Column renames: all `dwc_*` → `brand_*`
- Python logic (Bayesian CVR, percentile ranks, composite scores, role classification) is 100% portable — no changes needed

### 4c. Create `db/build_keywords.py`
- **Reference:** `dwc-big-analysis/db/build_keywords.py`
- **Must run AFTER** `build_asin_keywords.py`
- Same SQL translation patterns
- Column renames: `dwc_clicks`→`brand_clicks`, `dwc_purchases`→`brand_purchases`, `dwc_click_share`→`brand_click_share`, `dwc_purchase_share`→`brand_purchase_share`, `dwc_cvr`→`brand_cvr`, `dwc_impressions`→`brand_impressions`, `dwc_ctr`→`brand_ctr`
- `hero_asin` query uses `ROW_NUMBER() OVER` — works in PostgreSQL as-is

### Verify
```bash
python db/build_asin_keywords.py  # builds matrix
python db/build_keywords.py       # builds targets
# Check: SELECT COUNT(*) FROM keyword_targets; SELECT COUNT(*) FROM asin_keyword_scores;
```

---

## Phase 5: Dashboard

The biggest single file. ~3400 lines after removing coffee + subscriptions.

### Create `dashboard.py`
- **Reference:** `dwc-big-analysis/dashboard.py` (entire file)

#### Backend — systematic changes:

**Connection pattern (every endpoint):**
```python
# DWC:
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
conn.close()

# Nire:
from schema import get_conn
import psycopg2.extras
conn = get_conn()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute(sql, params)
rows = [dict(r) for r in cur.fetchall()]
cur.close()
conn.close()
```

**Endpoints to port (22):** All from the DWC table above except `/api/coffee-market` and `/api/subscriptions/*`

**Endpoints with hardest SQL (focus attention here):**
- `/api/keywords` (lines 469-519) — joins `keyword_targets` + `keyword_goals`, uses NULLS LAST
- `/api/keywords/monthly` (lines 521-558) — complex CTE with market dedup
- `/api/highlights` (lines 794-962) — 6 separate queries, joins daily + repeat + SQP data
- `/api/asin/<asin>` (lines 1020-1192) — 8 separate queries across 6 tables

#### Frontend — systematic changes:
- Delete Coffee Market tab HTML/CSS/JS section
- Delete Subscribe & Save tab HTML/CSS/JS section
- Update `TABS` array (remove those 2 entries)
- Find/replace `dwc_` → `brand_` in all JS fetch response field access
- Find/replace "DWC" / "Death Wish" → "Nire" / "Nire Beauty" in display text
- Verify tab switching still works after removing 2 tabs (check for index-based logic)

#### Port on `http://localhost:5050` (same as DWC, different from monitor.py's 5001)

### Verify
```bash
python dashboard.py
# Open http://localhost:5050
# Test each tab:
#   Overview — KPI cards load, trend charts render
#   Highlights — YoY metrics, breakout ASINs
#   Products — sortable ASIN table
#   Search Funnel — funnel chart + top queries
#   Search Terms — term table with click/conversion share
#   ASIN Explorer — autocomplete works, drill-down charts render
#   Keywords — filters work, strategy chips colored, share tracker chart renders
```

---

## Phase 6: End-to-End Pipeline

```bash
# 1. Schema
python -c "from schema import init_db; init_db()"

# 2. API pulls (requires working credentials from Phase 0)
python scripts/pull_listings.py
python scripts/pull_search_terms.py
python scripts/pull_sales_traffic.py

# 3. Backfill (if not already done)
python backfill.py --test

# 4. Transform raw → derived
python db/transform.py

# 5. Build keyword intelligence
python db/build_asin_keywords.py
python db/build_keywords.py

# 6. Launch
python dashboard.py  # http://localhost:5050
```

---

## Execution Order & Parallelism

```
START
  │
  ├──► Phase 0: SQP debugging (LONG — runs in background)
  │      test_auth.py → probe_reports.py → test_sqp.py → iterate
  │
  ├──► Phase 1: config.py (quick, no dependencies)
  │
  ├──► Phase 2: schema.py extension (quick, no dependencies)
  │
  │    (Phase 1+2 done)
  │         │
  │         ▼
  │    Phase 3: Pull scripts + transform.py
  │         │
  │         ▼
  │    Phase 4: Keyword intelligence (needs transform data)
  │         │
  │         ▼
  │    Phase 5: Dashboard (needs all tables populated)
  │
  └──► Phase 0 results feed into Phase 3 (can't pull data without working creds)
```

**Phase 0 is the critical path.** Everything else is code work that can proceed in parallel, but nothing runs end-to-end until API access works.

---

## Files Summary

### New files to create:
```
config.py                      # Period system
dashboard.py                   # Analytics dashboard (port 5050)
test_sqp.py                    # SQP debugging script
db/__init__.py
db/transform.py                # Raw → derived table ETL (NOT a port of load.py)
db/utils.py                    # Percentile, keyword classification
db/build_keywords.py           # Keyword targets builder
db/build_asin_keywords.py      # ASIN-keyword matrix builder
scripts/__init__.py
scripts/api_client.py          # SP-API wrapper
scripts/pull_all.py            # Orchestrator
scripts/pull_listings.py       # Listings pull
scripts/pull_search_terms.py   # Search terms pull
scripts/pull_sales_traffic.py  # Daily sales/traffic pull
```

### Existing files to modify:
```
schema.py                      # Add 12 new tables
backfill.py                    # Update SQP_ASINS (after ASIN discovery)
requirements.txt               # Add psycopg2.extras if not present
```

### Existing files that stay untouched:
```
auth.py, monitor.py, fetch_sqp_report.py, probe_reports.py, test_auth.py, .cursor/rules/*
```
