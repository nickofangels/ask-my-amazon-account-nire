"""
config.py — Date ranges and shared constants for Nire Beauty Y/Y analysis.

L52 = Last 52 weeks  (current period)
P52 = Prior 52 weeks (year-ago period)

Monthly intervals are used so each SP-API report request stays within Amazon's
supported date range window and data can be processed incrementally.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Marketplace
# ---------------------------------------------------------------------------

from auth import MARKETPLACE_ID

# ---------------------------------------------------------------------------
# Period definitions (rolling 52-week windows computed from "today")
# ---------------------------------------------------------------------------
# Override "today" for reproducible analysis:
#   NIRE_AS_OF_DATE=2026-03-05 python config.py

_as_of = os.environ.get("NIRE_AS_OF_DATE")
TODAY = date.fromisoformat(_as_of) if _as_of else date.today()

L52_END   = TODAY
L52_START = TODAY - timedelta(days=364)          # 52 weeks back
P52_END   = L52_START - timedelta(days=1)
P52_START = P52_END - timedelta(days=364)        # 52 weeks back


def _monthly_intervals(start: date, end: date) -> list[tuple[date, date, str]]:
    """
    Split [start, end] into calendar-month-aligned chunks.

    Returns a list of (chunk_start, chunk_end, label) tuples where label
    is 'YYYY-MM' (based on chunk_start).  The first and last chunks are
    clipped to the period boundaries.
    """
    intervals: list[tuple[date, date, str]] = []
    cursor = start

    while cursor <= end:
        if cursor.month == 12:
            next_month_start = date(cursor.year + 1, 1, 1)
        else:
            next_month_start = date(cursor.year, cursor.month + 1, 1)

        chunk_end = min(next_month_start - timedelta(days=1), end)
        label = cursor.strftime("%Y-%m")
        intervals.append((cursor, chunk_end, label))
        cursor = next_month_start

    return intervals


L52_MONTHS = _monthly_intervals(L52_START, L52_END)
P52_MONTHS = _monthly_intervals(P52_START, P52_END)

ALL_MONTHS = P52_MONTHS + L52_MONTHS  # chronological order

# Deduplicated month labels for pull scripts — each label appears once.
_seen_labels: set[str] = set()
PULL_MONTHS: list[tuple[date, date, str]] = []
for _s, _e, _lbl in ALL_MONTHS:
    if _lbl not in _seen_labels:
        _seen_labels.add(_lbl)
        PULL_MONTHS.append((_s, _e, _lbl))
del _seen_labels


# ---------------------------------------------------------------------------
# Canonical period lookups (single source of truth)
# ---------------------------------------------------------------------------

# Month-label -> period mapping.  L52 overwrites P52 for shared boundary labels.
_MONTH_TO_PERIOD: dict[str, str] = {lbl: "P52" for _, _, lbl in P52_MONTHS}
_MONTH_TO_PERIOD.update({lbl: "L52" for _, _, lbl in L52_MONTHS})


def month_to_period(month: str) -> str:
    """Canonical month-label -> period lookup."""
    return _MONTH_TO_PERIOD.get(month, "other")


def period_label(d: date) -> str:
    """Return 'L52' if d falls in the current period, 'P52' if prior, else 'other'."""
    if L52_START <= d <= L52_END:
        return "L52"
    if P52_START <= d <= P52_END:
        return "P52"
    return "other"


def _period_display_label(start: date, end: date) -> str:
    return f"{start.strftime('%b %Y')} \u2013 {end.strftime('%b %Y')}"


PERIOD_META = {
    "L52": {"start_date": str(L52_START), "end_date": str(L52_END),
            "label": _period_display_label(L52_START, L52_END)},
    "P52": {"start_date": str(P52_START), "end_date": str(P52_END),
            "label": _period_display_label(P52_START, P52_END)},
}


def full_month_bounds(label: str) -> tuple[date, date]:
    """Return (first_of_month, last_of_month) for a YYYY-MM label."""
    import calendar as _cal
    year, month = int(label[:4]), int(label[5:7])
    first = date(year, month, 1)
    last = date(year, month, _cal.monthrange(year, month)[1])
    return first, last


def get_active_asins() -> list[str]:
    """Return sorted list of Active ASINs from the listings table.

    Falls back to SQP_ASINS from backfill.py if the listings table
    doesn't exist yet (it's created in Phase 2).
    """
    try:
        from schema import get_conn
        import psycopg2.extras
        conn = get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT DISTINCT asin FROM listings "
                "WHERE LOWER(status) = 'active' ORDER BY asin"
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if rows:
                return [r["asin"] for r in rows]
        except Exception:
            conn.rollback()
            conn.close()
    except Exception:
        pass

    # Fallback: use hardcoded ASINs from backfill.py
    from backfill import SQP_ASINS
    return list(SQP_ASINS)


if __name__ == "__main__":
    print(f"L52: {L52_START} \u2192 {L52_END}  ({len(L52_MONTHS)} months)")
    for s, e, lbl in L52_MONTHS:
        print(f"  {lbl}: {s} \u2192 {e}")
    print(f"\nP52: {P52_START} \u2192 {P52_END}  ({len(P52_MONTHS)} months)")
    for s, e, lbl in P52_MONTHS:
        print(f"  {lbl}: {s} \u2192 {e}")
    print(f"\nPERIOD_META:")
    for k, v in PERIOD_META.items():
        print(f"  {k}: {v['label']}  ({v['start_date']} to {v['end_date']})")
    print(f"\nActive ASINs: {get_active_asins()}")
