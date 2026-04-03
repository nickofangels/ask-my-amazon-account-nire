"""
db/build_content_briefs.py — Build per-ASIN content briefs with tiered keywords.

Reads from asin_keyword_scores + keyword_targets to produce a content_briefs
table that assigns each keyword to a content tier:
  title        — top 5 keywords (must appear in product title)
  bullet       — next 20 keywords (should appear in bullet points)
  nice_to_have — next 30 keywords (description/backend search terms)
  branded      — branded keywords (separate section, brand already in title)

Adapted from the DWC project for Niré Beauty (PostgreSQL / Supabase).

Usage:
    python -m db.build_content_briefs
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
from schema import get_conn

# =====================================================================
# CONTENT BRIEF WEIGHTS — All weights must sum to 1.0.
# =====================================================================

CONTENT_BRIEF_WEIGHTS = {
    "volume":        0.25,   # high volume = more searches to capture via title
    "revenue":       0.20,   # proven revenue generator for this ASIN
    "cvr_advantage": 0.125,  # we convert well = content-keyword fit validated
    "headroom":      0.125,  # room to grow with better content
    "momentum":      0.075,  # rising trends worth chasing
    "strategy":      0.225,  # strategic classification boost
}

STRATEGY_BOOST = {
    "Branded":      1.00,
    "Defend":       0.95,
    "Grow":         0.90,
    "Watch":        0.55,
    "Deprioritize": 0.15,
}

POWER_STRETCH = 0.6

TIER_CONFIG = {
    "title":        {"max_count": 5,  "score_floor": 0.70, "min_floor": 0.55},
    "bullet":       {"max_count": 20, "score_floor": 0.45, "min_floor": 0.35},
    "nice_to_have": {"max_count": 30, "score_floor": 0.25, "min_floor": 0.15},
}

TITLE_MIN_COUNT = 3
TITLE_CHAR_LIMIT = 200
TITLE_BRAND_RESERVE = 20       # "Niré Beauty " ~13 chars + buffer
TITLE_DESCRIPTOR_RESERVE = 35
TITLE_CHAR_BUDGET = TITLE_CHAR_LIMIT - TITLE_BRAND_RESERVE - TITLE_DESCRIPTOR_RESERVE
TITLE_STOP_WORDS = {"a", "an", "the", "and", "or", "for", "of", "in", "to", "with", "by"}


# =====================================================================
# Main build function
# =====================================================================

def build_content_briefs(conn) -> int:
    """Rebuild content_briefs from asin_keyword_scores + keyword_targets."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("  [build_content_briefs] Loading asin_keyword_scores...", flush=True)

    cur.execute("""
        SELECT
            s.asin,
            s.search_query,
            s.search_volume,
            s.keyword_relevance,
            s.keyword_role,
            s.keyword_type,
            s.cvr_index,
            s.click_share,
            s.purchase_share,
            s.revenue_score,
            s.headroom_pct,
            s.momentum_pct,
            s.share_trend,
            s.volume_pct,
            s.within_asin_revenue_pct,
            s.cvr_advantage_pct,
            COALESCE(kt.strategy, 'Deprioritize') AS strategy
        FROM asin_keyword_scores s
        LEFT JOIN keyword_targets kt ON s.search_query = kt.search_query
    """)
    rows = cur.fetchall()
    print(f"  [build_content_briefs] {len(rows):,} (asin, keyword) pairs loaded", flush=True)

    # Group by ASIN
    asin_keywords: dict[str, list[dict]] = {}
    for r in rows:
        rec = dict(r)
        asin_keywords.setdefault(rec["asin"], []).append(rec)

    print(f"  [build_content_briefs] {len(asin_keywords):,} ASINs to process", flush=True)

    W = CONTENT_BRIEF_WEIGHTS
    _P = POWER_STRETCH
    output_rows: list[tuple] = []
    tier_totals = {"title": 0, "bullet": 0, "nice_to_have": 0, "branded": 0}

    def _pct(val):
        return (val or 0.0)

    for asin, kw_list in asin_keywords.items():
        for rec in kw_list:
            strategy_val = STRATEGY_BOOST.get(rec["strategy"], 0.30)
            rec["content_brief_score"] = (
                W["volume"]        * _pct(rec["volume_pct"]) ** _P
              + W["revenue"]       * _pct(rec["within_asin_revenue_pct"]) ** _P
              + W["cvr_advantage"] * _pct(rec["cvr_advantage_pct"]) ** _P
              + W["headroom"]      * _pct(rec["headroom_pct"]) ** _P
              + W["momentum"]      * _pct(rec["momentum_pct"]) ** _P
              + W["strategy"]      * strategy_val ** _P
            )

        branded = [r for r in kw_list if r["keyword_type"] == "branded"]
        category = [r for r in kw_list if r["keyword_type"] == "category"]
        category.sort(key=lambda r: r["content_brief_score"], reverse=True)

        # ── Title tier with character budget ──
        title_cfg = TIER_CONFIG["title"]
        title_floor = title_cfg["score_floor"]
        title_max = title_cfg["max_count"]
        title_min_floor = title_cfg["min_floor"]

        while title_floor > title_min_floor:
            count_above = sum(
                1 for r in category[:title_max]
                if r["content_brief_score"] >= title_floor
            )
            if count_above >= TITLE_MIN_COUNT:
                break
            title_floor -= 0.05

        title_unique_words: set[str] = set()
        title_rank = 0
        title_overflow: list[dict] = []

        for r in category[:title_max]:
            if r["content_brief_score"] < title_floor:
                break
            kw_words = {
                w for w in r["search_query"].lower().split()
                if len(w) > 2 and w not in TITLE_STOP_WORDS
            }
            projected_words = title_unique_words | kw_words
            projected_chars = sum(len(w) for w in projected_words) + max(0, len(projected_words) - 1)

            if projected_chars <= TITLE_CHAR_BUDGET:
                title_rank += 1
                r["content_tier"] = "title"
                r["tier_rank"] = title_rank
                title_unique_words = projected_words
            else:
                title_overflow.append(r)

        title_assigned = title_rank + len(title_overflow)
        tier_totals["title"] += title_rank

        # ── Bullet tier ──
        bullet_cfg = TIER_CONFIG["bullet"]
        bullet_floor = bullet_cfg["score_floor"]
        bullet_max = bullet_cfg["max_count"]

        bullet_rank = 0
        for r in title_overflow:
            bullet_rank += 1
            r["content_tier"] = "bullet"
            r["tier_rank"] = bullet_rank

        remaining_budget = bullet_max - bullet_rank
        for r in category[title_assigned:title_assigned + remaining_budget]:
            if r["content_brief_score"] < bullet_floor:
                break
            bullet_rank += 1
            r["content_tier"] = "bullet"
            r["tier_rank"] = bullet_rank

        bullet_assigned = title_assigned + (bullet_rank - len(title_overflow))
        tier_totals["bullet"] += bullet_rank

        # ── Nice-to-have tier ──
        nth_cfg = TIER_CONFIG["nice_to_have"]
        nth_floor = nth_cfg["score_floor"]
        nth_max = nth_cfg["max_count"]
        nth_rank = 0

        for r in category[bullet_assigned:bullet_assigned + nth_max]:
            if r["content_brief_score"] < nth_floor:
                break
            nth_rank += 1
            r["content_tier"] = "nice_to_have"
            r["tier_rank"] = nth_rank

        assigned = bullet_assigned + nth_rank
        tier_totals["nice_to_have"] += nth_rank

        # Branded keywords
        branded.sort(key=lambda r: r["content_brief_score"], reverse=True)
        for i, r in enumerate(branded, 1):
            r["content_tier"] = "branded"
            r["tier_rank"] = i
            tier_totals["branded"] += 1

        for r in category[:assigned] + branded:
            if "content_tier" not in r:
                continue

            def _rd(v, d=6):
                return round(v, d) if v is not None else None

            output_rows.append((
                r["asin"],
                r["search_query"],
                _rd(r["content_brief_score"]),
                r["content_tier"],
                r["tier_rank"],
                r["search_volume"],
                _rd(r["keyword_relevance"]),
                r["keyword_role"],
                r["keyword_type"],
                r["strategy"],
                _rd(r["cvr_index"]),
                _rd(r["click_share"]),
                _rd(r["purchase_share"]),
                _rd(r["revenue_score"], 2),
                _rd(r["headroom_pct"]),
                _rd(r["momentum_pct"]),
                _rd(r["share_trend"]),
                now,
            ))

    # Write to content_briefs
    print("  [build_content_briefs] Writing content_briefs...", flush=True)

    with conn.cursor() as w_cur:
        w_cur.execute("DELETE FROM content_briefs")
        for row in output_rows:
            w_cur.execute(
                """INSERT INTO content_briefs
                   (asin, search_query, content_brief_score, content_tier, tier_rank,
                    search_volume, keyword_relevance, keyword_role, keyword_type, strategy,
                    cvr_index, click_share, purchase_share, revenue_score,
                    headroom_pct, momentum_pct, share_trend, built_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                row,
            )
    conn.commit()

    print(f"  [build_content_briefs] Done — {len(output_rows):,} rows written")
    print("  [build_content_briefs] Tier breakdown:")
    for tier, cnt in sorted(tier_totals.items(), key=lambda x: -x[1]):
        print(f"    {tier:<15} {cnt:>6,}")

    return len(output_rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from schema import init_db
    init_db()

    conn = get_conn()
    n = build_content_briefs(conn)
    conn.close()
    print(f"\nDone. {n:,} content_briefs rows written.")
