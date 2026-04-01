#!/bin/bash
# pull_all.sh — Fire-and-forget: create ALL reports, then poll-download until done.
#
# Lessons learned:
#   - SQP processes ONE AT A TIME (~30-60 min each). Create SQP FIRST.
#   - Fast reports use separate queues, finish in 1-2 min.
#   - createReport burst limit: 15, then ~1/min sustained.
#   - Reports stay downloadable for 72 hours.
#   - Cancel stale (old test) reports before starting to clear the SQP queue.
#
# Usage: run from project root.  Logs to raw/_pull_all.log

set -e
cd "$(dirname "$0")/.."
PY=.venv/bin/python
LOG=raw/_pull_all.log

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

mkdir -p raw

# Phase 0: Clear any stale/test reports blocking the SQP queue
log "=== Phase 0: Clearing stale SQP queue ==="
$PY scripts/pull_raw.py --cancel-stale 2>&1 | tee -a "$LOG"

# Phase 1: Create SQP first (slow processor — start these ASAP)
log "=== Phase 1a: Queueing SQP reports (slow, ~30-60 min each) ==="
$PY scripts/pull_raw.py --create --only sqp  2>&1 | tee -a "$LOG"

# Phase 1b: Create fast reports (separate queues, finish in 1-2 min)
log "=== Phase 1b: Queueing fast reports ==="
$PY scripts/pull_raw.py --create --only fast 2>&1 | tee -a "$LOG"

log "All creates submitted."

# Phase 2: Download everything in a loop until nothing is pending
# Fast reports will be ready almost immediately; SQP will trickle in over hours.
log "=== Phase 2: Downloading all reports ==="
for i in $(seq 1 120); do
    log "Download pass $i..."
    $PY scripts/pull_raw.py --download 2>&1 | tee -a "$LOG"

    # Count pending by type: SQP vs fast (they have very different timelines)
    read PENDING_FAST PENDING_SQP < <($PY -c "
import json
from pathlib import Path
m = json.load(open('raw/_manifest.json'))
fast = sqp = 0
for k, e in m['reports'].items():
    if e.get('status') in ('DOWNLOADED', 'FATAL', 'CANCELLED'):
        continue
    if Path('raw/' + e['file']).exists():
        continue
    if k.startswith('sqp/'):
        sqp += 1
    else:
        fast += 1
print(f'{fast} {sqp}')
" 2>/dev/null)

    PENDING=$((PENDING_FAST + PENDING_SQP))

    if [ "$PENDING" = "0" ]; then
        log "All reports downloaded!"
        break
    fi

    # Adaptive wait based on what's actually pending:
    # - Fast reports: ready in 1-2 min, poll frequently
    # - SQP only: ~30-60 min each, don't hammer the API
    if [ "$PENDING_FAST" -gt 0 ]; then
        if [ "$PENDING_FAST" -gt 10 ]; then
            WAIT=180  # 3 min — many fast still processing
        else
            WAIT=60   # 1 min — few fast remaining
        fi
    else
        WAIT=600  # 10 min — only SQP left, no point polling frequently
    fi

    log "$PENDING pending (fast=$PENDING_FAST, sqp=$PENDING_SQP), waiting ${WAIT}s..."
    sleep $WAIT
done

log "=== Done! ==="
$PY scripts/pull_raw.py --status 2>&1 | tee -a "$LOG"
