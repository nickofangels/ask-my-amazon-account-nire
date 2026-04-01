#!/usr/bin/env python3
"""Live watcher for sales_traffic CHILD repull progress. Refreshes every 10s."""
import json, os, sys, time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw" / "sales_traffic"

# ANSI
CLEAR  = "\033[2J\033[H"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"


def check_files():
    """Scan raw/sales_traffic/*.json and classify by granularity."""
    child = []
    parent = []
    missing = []

    # Expected months from the file names on disk
    files = sorted(RAW_DIR.glob("*.json"))
    for f in files:
        month = f.stem
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            asin_rows = data.get("salesAndTrafficByAsin", [])
            if asin_rows and "childAsin" in asin_rows[0]:
                child.append(month)
            else:
                parent.append(month)
        except Exception:
            missing.append(month)

    return child, parent, missing


def render():
    child, parent, missing = check_files()
    total = len(child) + len(parent) + len(missing)
    done = len(child)

    bar_len = 30
    filled = int(bar_len * done / total) if total else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    pct = done / total * 100 if total else 0

    lines = [
        f"{BOLD}{'─' * 60}{RESET}",
        f"{BOLD}  SALES TRAFFIC REPULL{RESET}  (PARENT → CHILD)",
        f"{BOLD}{'─' * 60}{RESET}",
        "",
        f"  Progress: [{GREEN}{bar}{RESET}] {done}/{total} ({pct:.0f}%)",
        "",
    ]

    if child:
        # Wrap at ~8 months per line
        chunks = [child[i:i+8] for i in range(0, len(child), 8)]
        lines.append(f"  {GREEN}✓ CHILD:{RESET}  {', '.join(chunks[0])}")
        for chunk in chunks[1:]:
            lines.append(f"            {', '.join(chunk)}")

    if parent:
        chunks = [parent[i:i+8] for i in range(0, len(parent), 8)]
        lines.append(f"  {YELLOW}⏳ PARENT:{RESET} {', '.join(chunks[0])}")
        for chunk in chunks[1:]:
            lines.append(f"            {', '.join(chunk)}")

    if missing:
        lines.append(f"  {RED}✗ ERROR:{RESET}  {', '.join(missing)}")

    lines.append("")

    if done == total:
        lines.append(f"  {GREEN}{BOLD}All files have CHILD granularity! Ready to load.{RESET}")
        lines.append(f"  Run: {CYAN}.venv/bin/python -m db.load --only sales{RESET}")
    elif done > 0:
        # Estimate: check timestamps on child files for avg time
        child_files = [RAW_DIR / f"{m}.json" for m in child]
        if len(child_files) >= 2:
            times = sorted(f.stat().st_mtime for f in child_files if f.exists())
            if len(times) >= 2:
                avg_secs = (times[-1] - times[0]) / (len(times) - 1)
                remaining = len(parent) + len(missing)
                eta_min = remaining * avg_secs / 60
                lines.append(f"  {DIM}ETA: ~{int(eta_min)} min ({remaining} remaining × ~{int(avg_secs)}s avg){RESET}")

    lines.append("")
    lines.append(f"  {DIM}Last checked: {datetime.now().strftime('%I:%M:%S %p')}  ·  Refreshes every 10s  ·  Ctrl+C to stop{RESET}")

    return "\n".join(lines)


if __name__ == "__main__":
    try:
        while True:
            print(CLEAR + render(), flush=True)
            time.sleep(10)
    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped.{RESET}")
