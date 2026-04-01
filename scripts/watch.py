#!/usr/bin/env python3
"""Live dashboard for report pull progress. Refreshes every 60s.
Polls SQP queue via API every 2 min (1 call, cached to avoid quota fights).
Tracks SQP processing times to improve ETA accuracy.
Usage: .venv/bin/python scripts/watch.py
"""
import json, sys, time
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

MANIFEST = ROOT / "raw" / "_manifest.json"
LOG = ROOT / "raw" / "_pull_all.log"
TIMING_FILE = ROOT / "raw" / "_sqp_timing.json"

# ANSI
CLEAR  = "\033[2J\033[H"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"

# ── SQP timing tracker ──────────────────────────────────────────────────────
# Persists to disk so we keep history across watcher restarts

def _load_timing():
    if TIMING_FILE.exists():
        return json.loads(TIMING_FILE.read_text())
    return {"completed": [], "current_id": None, "current_start": None}

def _save_timing(t):
    TIMING_FILE.parent.mkdir(parents=True, exist_ok=True)
    TIMING_FILE.write_text(json.dumps(t, indent=2))

def _parse_iso(s):
    """Parse ISO timestamp from API, return datetime with tzinfo."""
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def update_timing(sqp_reports):
    """Track SQP processing times from API data."""
    t = _load_timing()
    known_ids = {e["id"] for e in t["completed"]}

    for r in sqp_reports:
        rid = r.get("reportId")
        status = r.get("processingStatus")
        start = r.get("processingStartTime")
        end = r.get("processingEndTime")

        # Track newly completed reports
        if status == "DONE" and rid not in known_ids and start and end:
            started = _parse_iso(start)
            ended = _parse_iso(end)
            if started and ended:
                mins = (ended - started).total_seconds() / 60
                t["completed"].append({
                    "id": rid,
                    "started": start,
                    "ended": end,
                    "minutes": round(mins, 1),
                })
                known_ids.add(rid)

        # Track current in-progress
        if status == "IN_PROGRESS":
            t["current_id"] = rid
            t["current_start"] = start

    # Clear current if it's now done
    if t["current_id"] and t["current_id"] in known_ids:
        t["current_id"] = None
        t["current_start"] = None

    _save_timing(t)
    return t


# ── API cache ────────────────────────────────────────────────────────────────
_api_cache = {"data": None, "time": 0, "error": None}
API_REFRESH_SECS = 300  # 5 min — generous to avoid competing with download phase

def fetch_sqp_queue():
    """Single API call for SQP queue status. Cached 2 min."""
    now = time.time()
    if _api_cache["data"] is not None and (now - _api_cache["time"]) < API_REFRESH_SECS:
        return _api_cache["data"]
    try:
        from sp_api.api import Reports
        from auth import CREDENTIALS, MARKETPLACE
        client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)
        res = client.get_reports(
            reportTypes=["GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"],
            pageSize=50,
        )
        payload = res.payload
        reports = payload.get("reports", []) if isinstance(payload, dict) else payload
        result = reports if isinstance(reports, list) else []
        _api_cache.update(data=result, time=now, error=None)
        return result
    except Exception as e:
        _api_cache["error"] = str(e)
        return _api_cache["data"]  # stale cache or None


def load_manifest():
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {"reports": {}}


def tail_log(n=6):
    if not LOG.exists():
        return ["(no log yet)"]
    lines = LOG.read_text().strip().split("\n")
    return lines[-n:] if lines else ["(empty)"]


def bar(statuses, width=40):
    total = len(statuses)
    if not total:
        return DIM + "." * width + RESET
    c = Counter(statuses)
    out = ""
    used = 0
    for s, ch, color in [
        ("DOWNLOADED", "\u2588", GREEN), ("READY", "\u2593", CYAN),
        ("IN_PROGRESS", "\u25b6", YELLOW), ("WAITING", "\u2591", DIM),
        ("FATAL", "X", RED), ("CANCELLED", "X", RED),
    ]:
        n = c.get(s, 0)
        if n:
            w = max(1, round(n / total * width))
            out += f"{color}{ch * w}{RESET}"
            used += w
    out += " " * max(0, width - used)
    return out


def fmt_mins(m):
    if m < 60:
        return f"{int(m)}m"
    h, m = divmod(int(m), 60)
    return f"{h}h{m:02d}m"


# ── Render ───────────────────────────────────────────────────────────────────

def render():
    manifest = load_manifest()
    reports = manifest.get("reports", {})
    our_ids = {v["report_id"] for v in reports.values()}

    now = datetime.now(timezone.utc)
    now_local = datetime.now()

    lines = [
        f"{BOLD}{'─' * 65}{RESET}",
        f"{BOLD}  REPORT PULL DASHBOARD{RESET}              {DIM}{now_local.strftime('%H:%M:%S')}{RESET}",
        f"{BOLD}{'─' * 65}{RESET}",
        "",
    ]

    # ── SQP Queue ────────────────────────────────────────────────────
    sqp_api = fetch_sqp_queue()

    cache_age = int(time.time() - _api_cache["time"]) if _api_cache["time"] else 0
    cache_tag = f"{DIM}updated {cache_age}s ago{RESET}"
    if _api_cache["error"]:
        cache_tag = f"{YELLOW}cached (quota){RESET}"

    lines.append(f"  {BOLD}SQP Queue{RESET}  {cache_tag}")
    lines.append(f"  {DIM}{'─' * 55}{RESET}")

    timing = {"completed": [], "current_id": None, "current_start": None}
    if sqp_api is None:
        lines.append(f"  {DIM}Waiting for first API response...{RESET}")
    else:
        timing = update_timing(sqp_api)

        # Split: ours vs stale
        stale_pending = [r for r in sqp_api
                         if r.get("reportId") not in our_ids
                         and r.get("processingStatus") in ("IN_QUEUE", "IN_PROGRESS")]
        ours = [r for r in sqp_api if r.get("reportId") in our_ids]
        our_c = Counter(r.get("processingStatus", "?") for r in ours)

        processing = [r for r in sqp_api if r.get("processingStatus") == "IN_PROGRESS"]

        # Stale blockers
        if stale_pending:
            lines.append(f"  {RED}\u26a0 {len(stale_pending)} stale reports blocking queue{RESET}  "
                         f"{DIM}(run --cancel-stale){RESET}")

        # Currently processing
        if processing:
            r = processing[0]
            who = "ours" if r["reportId"] in our_ids else "stale"
            elapsed_str = ""
            start_time = _parse_iso(r.get("processingStartTime"))
            if start_time:
                elapsed_min = (now - start_time).total_seconds() / 60
                elapsed_str = f" {YELLOW}{fmt_mins(elapsed_min)} elapsed{RESET}"
            lines.append(f"  {YELLOW}\u25b6 Processing:{RESET} {r['reportId'][:12]}... ({who}){elapsed_str}")
        else:
            lines.append(f"  {DIM}No report currently processing{RESET}")

        # Our SQP counts
        done = our_c.get("DONE", 0)
        prog = our_c.get("IN_PROGRESS", 0)
        queue = our_c.get("IN_QUEUE", 0)
        total_ours = len(ours)

        if total_ours:
            lines.append(f"  Ours:  {GREEN}\u2713{done} done{RESET}  "
                         f"{YELLOW}\u25b6{prog} active{RESET}  "
                         f"{DIM}\u00b7{queue} queued{RESET}  "
                         f"({total_ours} total)")

            # ETA based on observed avg or 45 min default
            completed_times = [e["minutes"] for e in timing["completed"]]
            if completed_times:
                avg_min = sum(completed_times) / len(completed_times)
            else:
                avg_min = 45  # default estimate
            remaining = len(stale_pending) + queue + prog
            if remaining:
                eta = remaining * avg_min
                lines.append(f"  {DIM}ETA: ~{fmt_mins(eta)} ({remaining} remaining \u00d7 ~{fmt_mins(avg_min)} avg){RESET}")
        else:
            lines.append(f"  {DIM}No SQP reports created yet{RESET}")

    lines.append("")

    # ── SQP Timing History ───────────────────────────────────────────
    completed = timing.get("completed", [])
    if completed:
        lines.append(f"  {BOLD}SQP Timing{RESET}  {DIM}({len(completed)} completed){RESET}")
        lines.append(f"  {DIM}{'─' * 55}{RESET}")

        times = [e["minutes"] for e in completed]
        avg = sum(times) / len(times)
        lo = min(times)
        hi = max(times)
        lines.append(f"  Avg: {CYAN}{fmt_mins(avg)}{RESET}  "
                     f"Min: {GREEN}{fmt_mins(lo)}{RESET}  "
                     f"Max: {RED}{fmt_mins(hi)}{RESET}")

        # Show last 5 completed
        recent = completed[-5:]
        for e in recent:
            rid = e["id"][:12]
            mins = e["minutes"]
            color = GREEN if mins < avg else (YELLOW if mins < hi else RED)
            lines.append(f"  {DIM}{rid}...{RESET}  {color}{fmt_mins(mins)}{RESET}")
        lines.append("")

    # ── Pipeline ─────────────────────────────────────────────────────
    api_done = set()
    api_prog = set()
    if sqp_api:
        for r in sqp_api:
            rid = r.get("reportId")
            st = r.get("processingStatus")
            if st == "DONE":
                api_done.add(rid)
            elif st == "IN_PROGRESS":
                api_prog.add(rid)

    groups = {}
    for key, entry in sorted(reports.items()):
        folder = key.split("/")[0]
        if folder not in groups:
            groups[folder] = []

        fpath = ROOT / "raw" / entry["file"]
        rid = entry.get("report_id", "")

        if fpath.exists() and fpath.stat().st_size > 0:
            status = "DOWNLOADED"
        elif rid in api_done:
            status = "READY"
        elif rid in api_prog:
            status = "IN_PROGRESS"
        elif folder != "sqp":
            try:
                created = datetime.strptime(entry.get("created_at", ""), "%Y-%m-%d %H:%M:%S")
                age = (now_local - created).total_seconds() / 60
                status = "READY" if age > 5 else "WAITING"
            except Exception:
                status = "WAITING"
        else:
            status = "WAITING"

        groups[folder].append((key, status, entry.get("label", key)))

    total = len(reports)
    all_st = [s for entries in groups.values() for _, s, _ in entries]
    c = Counter(all_st)
    dl = c.get("DOWNLOADED", 0)
    rd = c.get("READY", 0)
    wt = c.get("WAITING", 0) + c.get("IN_PROGRESS", 0)
    pct = int(dl / total * 100) if total else 0

    lines.append(f"  {BOLD}Pipeline{RESET}  {GREEN}\u2588 saved{RESET}  {CYAN}\u2593 ready{RESET}  {YELLOW}\u25b6 active{RESET}  {DIM}\u2591 waiting{RESET}")
    lines.append(f"  {DIM}{'─' * 55}{RESET}")
    lines.append(f"  [{bar(all_st, 50)}] {pct}%")
    lines.append(f"  {GREEN}{dl} saved{RESET}  {CYAN}{rd} ready{RESET}  {DIM}{wt} waiting{RESET}  ({total} total)")
    lines.append("")

    type_order = ["listings", "sales_traffic", "search_terms",
                  "search_catalog_performance", "market_basket",
                  "repeat_purchase", "sqp"]
    for folder in type_order:
        if folder not in groups:
            continue
        entries = groups[folder]
        statuses = [s for _, s, _ in entries]
        fc = Counter(statuses)
        label = folder.replace("_", " ").title()

        parts = []
        if fc.get("DOWNLOADED", 0):
            parts.append(f"{GREEN}\u2713{fc['DOWNLOADED']}{RESET}")
        if fc.get("READY", 0):
            parts.append(f"{CYAN}\u2b07{fc['READY']}{RESET}")
        if fc.get("IN_PROGRESS", 0):
            parts.append(f"{YELLOW}\u25b6{fc['IN_PROGRESS']}{RESET}")
        if fc.get("WAITING", 0):
            parts.append(f"{DIM}\u00b7{fc['WAITING']}{RESET}")

        lines.append(f"  {BOLD}{label:30s}{RESET} [{bar(statuses, 25)}] {' '.join(parts)}")

    lines.append("")

    # ── Disk stats ───────────────────────────────────────────────────
    raw_dir = ROOT / "raw"
    file_count = 0
    total_kb = 0
    for p in raw_dir.rglob("*"):
        if p.is_file() and p.name not in ("_manifest.json", "_pull_all.log", "_sqp_timing.json"):
            file_count += 1
            total_kb += p.stat().st_size // 1024

    lines.append(f"  {BOLD}Disk:{RESET} {file_count} files, {total_kb:,} KB")
    lines.append("")

    # ── Log tail ─────────────────────────────────────────────────────
    lines.append(f"  {BOLD}Log:{RESET}")
    for l in tail_log(6):
        lines.append(f"  {DIM}{l}{RESET}")

    lines.append("")
    lines.append(f"  {DIM}Refreshes every 60s \u00b7 API every {API_REFRESH_SECS}s \u00b7 Ctrl+C to stop{RESET}")

    return "\n".join(lines)


if __name__ == "__main__":
    try:
        while True:
            try:
                print(CLEAR + render(), flush=True)
            except Exception as e:
                print(CLEAR + f"{RED}Error: {e}{RESET}\nRetrying...", flush=True)
            time.sleep(60)
    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped.{RESET}")
