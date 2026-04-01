#!/usr/bin/env python3
"""Read-only SQP progress watcher. Updates every 3 min."""
import json, os, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from auth import CREDENTIALS, MARKETPLACE
from sp_api.api import Reports

MANIFEST = Path(__file__).resolve().parent.parent / "raw" / "_manifest.json"
TOTAL_VALID = 16  # 23 total minus 3 FATAL minus 4 CANCELLED

def get_sqp_entries():
    m = json.loads(MANIFEST.read_text())
    return {k: v for k, v in sorted(m["reports"].items()) if k.startswith("sqp/")}

def check():
    entries = get_sqp_entries()
    client = Reports(credentials=CREDENTIALS, marketplace=MARKETPLACE)

    downloaded = []
    pending = []
    terminal = []
    current = None

    for k, v in entries.items():
        month = k.split("/")[1]
        status = v["status"]
        if status == "DOWNLOADED" or Path(f"raw/{v['file']}").exists():
            downloaded.append(month)
        elif status in ("FATAL", "CANCELLED"):
            terminal.append((month, status))
        else:
            # Check live status
            try:
                res = client.get_report(v["report_id"])
                live = res.payload.get("processingStatus", "?")
                if live == "DONE":
                    downloaded.append(month + " (ready!)")
                elif live == "IN_PROGRESS":
                    started = res.payload.get("processingStartTime", "")
                    current = (month, started)
                    pending.append(month)
                else:
                    pending.append(month)
                time.sleep(0.3)
            except:
                pending.append(month)

    return downloaded, pending, terminal, current

def display(downloaded, pending, terminal, current):
    os.system("clear")
    done = len(downloaded)
    bar_len = 30
    filled = int(bar_len * done / TOTAL_VALID)
    bar = "█" * filled + "░" * (bar_len - filled)
    pct = done / TOTAL_VALID * 100

    print(f"  SQP Progress: [{bar}] {done}/{TOTAL_VALID} ({pct:.0f}%)")
    print()

    if current:
        month, started = current
        if started:
            from datetime import datetime, timezone
            st = datetime.fromisoformat(started.replace("Z", "+00:00"))
            elapsed = (datetime.now(timezone.utc) - st).total_seconds() / 60
            print(f"  Processing:   {month} ({elapsed:.0f} min so far)")
        else:
            print(f"  Processing:   {month}")
    else:
        print(f"  Processing:   (none active)")

    print()
    print(f"  Downloaded:   {', '.join(downloaded)}")
    print(f"  In queue:     {', '.join(pending) or '(none)'}")
    print(f"  Terminal:     {', '.join(f'{m} ({s})' for m, s in terminal)}")
    print()
    print(f"  Last checked: {time.strftime('%I:%M:%S %p')}")
    print(f"  Next check:   3 min  |  Ctrl+C to stop")

if __name__ == "__main__":
    while True:
        try:
            downloaded, pending, terminal, current = check()
            display(downloaded, pending, terminal, current)
            time.sleep(180)
        except KeyboardInterrupt:
            print("\nStopped.")
            break
