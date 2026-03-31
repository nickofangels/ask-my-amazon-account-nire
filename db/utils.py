"""
db/utils.py — Shared utilities for the build pipeline.

Imported by build_asin_keywords.py and build_keywords.py to ensure a
single source of truth for keyword classification, percentile ranking,
and trend-window computation.
"""
from __future__ import annotations

from datetime import date


# ---------------------------------------------------------------------------
# Keyword type constants and classifier — Nire Beauty (makeup brushes)
# ---------------------------------------------------------------------------

BRANDED_TERMS = [
    "nire beauty", "nire brush", "nire makeup",
]
BRANDED_EXACT = {"nire"}

BRANDED_COMBOS: list[tuple[str, ...]] = [
    ("nire", "brush"),
]

COMPETITOR_TERMS = [
    "real techniques", "sigma", "morphe", "bh cosmetics",
    "jessup", "bs-mall", "bs mall", "docolor", "bestope",
    "lamora", "eigshow", "ducare", "anne's giverny",
]


def keyword_type(query: str) -> str:
    q = query.lower()
    if q in BRANDED_EXACT or any(t in q for t in BRANDED_TERMS):
        return "branded"
    if any(all(t in q for t in combo) for combo in BRANDED_COMBOS):
        return "branded"
    if any(t in q for t in COMPETITOR_TERMS):
        return "competitor"
    return "category"


# ---------------------------------------------------------------------------
# Percentile ranking
# ---------------------------------------------------------------------------

def percentile_ranks(values: list[float | None]) -> list[float | None]:
    """
    Return a parallel list of percentile ranks (0.0-1.0) for each value.

    - None inputs -> None outputs.
    - Ties receive the average rank.
    - n=1: the sole non-None value ranks 1.0 (best/only = top).
    """
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    if not indexed:
        return [None] * len(values)

    indexed.sort(key=lambda x: x[0])
    n = len(indexed)

    ranks: list[float] = [0.0] * len(values)
    i = 0
    while i < n:
        j = i
        while j < n - 1 and indexed[j + 1][0] == indexed[i][0]:
            j += 1
        avg_rank = (i + j) / 2.0 / (n - 1) if n > 1 else 1.0
        for k in range(i, j + 1):
            ranks[indexed[k][1]] = avg_rank
        i = j + 1

    return [ranks[i] if values[i] is not None else None for i in range(len(values))]


def safe_pct(ranks: list[float | None], i: int, fallback: float = 0.0) -> float:
    v = ranks[i]
    return v if v is not None else fallback


# ---------------------------------------------------------------------------
# Trend window helper
# ---------------------------------------------------------------------------

def _add_months(ym: str, n: int) -> str:
    """Add n months to a 'YYYY-MM' string (n may be negative)."""
    y, m = int(ym[:4]), int(ym[5:7])
    m += n
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def trend_windows(conn) -> tuple[str, str, str, str]:
    """
    Derive the recent and prior 4-month trend windows from the data.

    Returns (recent_start, recent_end, prior_start, prior_end) as
    'YYYY-MM' strings.  Windows are non-overlapping and sequential.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(month) FROM search_query_performance WHERE period = 'L52'"
    )
    row = cur.fetchone()
    max_month: str = row[0] if row and row[0] else date.today().strftime("%Y-%m")
    cur.close()

    recent_end   = max_month
    recent_start = _add_months(recent_end, -3)
    prior_end    = _add_months(recent_start, -1)
    prior_start  = _add_months(prior_end, -3)

    return recent_start, recent_end, prior_start, prior_end
