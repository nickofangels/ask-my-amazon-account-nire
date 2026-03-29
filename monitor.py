"""
Backfill progress monitor — http://localhost:5001

Run alongside backfill.py:
    Terminal 1:  python3 backfill.py 2>&1 | tee /tmp/backfill.log
    Terminal 2:  python3 monitor.py [--log /tmp/backfill.log]
"""

import argparse
import json
import os
import time
from collections import deque
from datetime import datetime

import schema
from flask import Flask, Response, render_template_string

app = Flask(__name__)

STATUS_FILE = '/tmp/backfill_status.json'
DEFAULT_LOG = '/tmp/backfill.log'
LOG_LINES = 80

TABLES = [
    'sales_and_traffic',
    'sqp_report',
    'search_catalog_performance',
    'market_basket',
    'repeat_purchase',
]

# Expected full-backfill row estimates (24 months) — used for progress bars
EXPECTED_ROWS = {
    'sales_and_traffic': 800,
    'sqp_report': 50000,
    'search_catalog_performance': 800,
    'market_basket': 2000,
    'repeat_purchase': 800,
}

STATUS_COLORS = {
    'IN_QUEUE':    '#f59e0b',
    'IN_PROGRESS': '#3b82f6',
    'DOWNLOADING': '#8b5cf6',
    'PARSED':      '#06b6d4',
    'INSERTED':    '#10b981',
    'DONE':        '#10b981',
    'FAILED':      '#ef4444',
    'TIMEOUT':     '#ef4444',
    'CANCELLED':   '#ef4444',
    'FATAL':       '#ef4444',
}

LOG_PATH = DEFAULT_LOG  # overridden by --log arg


def get_row_counts():
    try:
        conn = schema.get_conn()
        counts = {}
        with conn.cursor() as cur:
            for table in TABLES:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM {table}')
                    counts[table] = cur.fetchone()[0]
                except Exception:
                    counts[table] = None
        conn.close()
        return counts
    except Exception as e:
        return {t: None for t in TABLES}


def get_api_status():
    try:
        if not os.path.exists(STATUS_FILE):
            return {}
        with open(STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def tail_log(n=LOG_LINES):
    try:
        if not os.path.exists(LOG_PATH):
            return [f'Log file not found: {LOG_PATH}']
        with open(LOG_PATH) as f:
            return list(deque(f, n))
    except Exception as e:
        return [f'Error reading log: {e}']


def is_backfill_running():
    try:
        import subprocess
        result = subprocess.run(
            ['pgrep', '-f', 'backfill.py'],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except Exception:
        return None


DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Backfill Monitor</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0f172a; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", monospace; font-size: 14px; }
  header { background: #1e293b; padding: 16px 24px; border-bottom: 1px solid #334155; display: flex; align-items: center; gap: 16px; }
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: .5px; }
  .pill { padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .pill.running  { background: #065f46; color: #6ee7b7; }
  .pill.idle     { background: #1e293b; color: #94a3b8; border: 1px solid #334155; }
  .updated { margin-left: auto; color: #64748b; font-size: 12px; }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; padding: 20px 24px; }
  @media (max-width: 900px) { main { grid-template-columns: 1fr; } }
  section { background: #1e293b; border-radius: 10px; padding: 18px; border: 1px solid #334155; }
  section h2 { font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: #94a3b8; margin-bottom: 14px; }
  .table-row { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
  .table-name { width: 200px; flex-shrink: 0; font-size: 13px; color: #cbd5e1; }
  .bar-wrap { flex: 1; background: #0f172a; border-radius: 4px; height: 8px; overflow: hidden; }
  .bar { height: 100%; border-radius: 4px; background: #3b82f6; transition: width .5s; }
  .bar.full { background: #10b981; }
  .count { width: 70px; text-align: right; font-size: 13px; color: #94a3b8; }
  .api-grid { display: flex; flex-direction: column; gap: 8px; max-height: 420px; overflow-y: auto; }
  .api-row { display: flex; align-items: center; gap: 10px; padding: 8px 10px; background: #0f172a; border-radius: 6px; border: 1px solid #1e293b; }
  .api-label { flex: 1; font-size: 12px; color: #cbd5e1; word-break: break-all; }
  .api-badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; color: #fff; flex-shrink: 0; }
  .api-meta { font-size: 11px; color: #64748b; flex-shrink: 0; }
  .log-box { background: #020617; border-radius: 6px; padding: 12px; font-family: monospace; font-size: 12px; line-height: 1.6; max-height: 460px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; color: #94a3b8; }
  .log-box span.ok   { color: #6ee7b7; }
  .log-box span.err  { color: #fca5a5; }
  .log-box span.info { color: #93c5fd; }
  .log-box span.warn { color: #fcd34d; }
  .empty { color: #475569; font-style: italic; text-align: center; padding: 20px 0; }
  .full-width { grid-column: 1 / -1; }
</style>
</head>
<body>
<header>
  <h1>Backfill Monitor</h1>
  <span id="proc-pill" class="pill idle">checking...</span>
  <span class="updated">Updated: <span id="updated-at">—</span></span>
</header>
<main>
  <section>
    <h2>Row Counts (Supabase)</h2>
    <div id="counts-panel"></div>
  </section>
  <section>
    <h2>Amazon API Status</h2>
    <div id="api-panel" class="api-grid"><div class="empty">No reports requested yet</div></div>
  </section>
  <section class="full-width">
    <h2>Live Log</h2>
    <div id="log-box" class="log-box">Waiting for log...</div>
  </section>
</main>

<script>
const EXPECTED = ''' + json.dumps(EXPECTED_ROWS) + ''';
const STATUS_COLORS = ''' + json.dumps(STATUS_COLORS) + ''';

function fmtCount(n) {
  if (n === null || n === undefined) return '—';
  return n.toLocaleString();
}

function timeSince(iso) {
  if (!iso) return '';
  const diff = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (diff < 60) return diff + 's ago';
  if (diff < 3600) return Math.floor(diff/60) + 'm ago';
  return Math.floor(diff/3600) + 'h ago';
}

function colorLine(line) {
  if (/DONE|inserted|rows|Connected|live|Found/.test(line)) return `<span class="ok">${line}</span>`;
  if (/FAIL|ERROR|error|Traceback|Exception|timed out/.test(line)) return `<span class="err">${line}</span>`;
  if (/IN_PROGRESS|IN_QUEUE|DOWNLOADING|Retrying/.test(line)) return `<span class="warn">${line}</span>`;
  if (/===|→/.test(line)) return `<span class="info">${line}</span>`;
  return line;
}

function renderCounts(counts) {
  const el = document.getElementById('counts-panel');
  el.innerHTML = Object.entries(counts).map(([table, count]) => {
    const expected = EXPECTED[table] || 1000;
    const pct = count === null ? 0 : Math.min(100, Math.round(count / expected * 100));
    const full = pct >= 100 ? ' full' : '';
    return `<div class="table-row">
      <div class="table-name">${table}</div>
      <div class="bar-wrap"><div class="bar${full}" style="width:${pct}%"></div></div>
      <div class="count">${fmtCount(count)}</div>
    </div>`;
  }).join('');
}

function renderApi(statuses) {
  const el = document.getElementById('api-panel');
  const entries = Object.entries(statuses);
  if (!entries.length) {
    el.innerHTML = '<div class="empty">No reports requested yet</div>';
    return;
  }
  // Sort: in-flight first, then by updated_at desc
  entries.sort((a, b) => {
    const order = ['IN_QUEUE','IN_PROGRESS','DOWNLOADING','PARSED','INSERTED','DONE','FAILED','TIMEOUT'];
    const ai = order.indexOf(a[1].status), bi = order.indexOf(b[1].status);
    if (ai !== bi) return ai - bi;
    return (b[1].updated_at || '').localeCompare(a[1].updated_at || '');
  });
  el.innerHTML = entries.map(([label, info]) => {
    const color = STATUS_COLORS[info.status] || '#64748b';
    const extra = info.rows ? ` · ${info.rows.toLocaleString()} rows` : (info.error ? ` · ${info.error.substring(0,60)}` : '');
    return `<div class="api-row">
      <div class="api-label">${label}</div>
      <div class="api-meta">${timeSince(info.updated_at)}${extra}</div>
      <div class="api-badge" style="background:${color}">${info.status}</div>
    </div>`;
  }).join('');
}

function renderLog(lines) {
  const box = document.getElementById('log-box');
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  box.innerHTML = lines.map(l => colorLine(l.replace(/</g,'&lt;').replace(/>/g,'&gt;').trimEnd())).join('\\n');
  if (atBottom) box.scrollTop = box.scrollHeight;
}

const es = new EventSource('/stream');
es.onmessage = e => {
  const data = JSON.parse(e.data);
  document.getElementById('updated-at').textContent = new Date().toLocaleTimeString();
  const pill = document.getElementById('proc-pill');
  if (data.running === true)       { pill.textContent = 'Running'; pill.className = 'pill running'; }
  else if (data.running === false) { pill.textContent = 'Idle';    pill.className = 'pill idle'; }
  renderCounts(data.counts);
  renderApi(data.api_status);
  renderLog(data.log_lines);
};
es.onerror = () => {
  document.getElementById('updated-at').textContent = 'connection lost — retrying...';
};
</script>
</body>
</html>'''


@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route('/stream')
def stream():
    def event_generator():
        while True:
            payload = {
                'counts': get_row_counts(),
                'api_status': get_api_status(),
                'log_lines': tail_log(),
                'running': is_backfill_running(),
            }
            yield f'data: {json.dumps(payload)}\n\n'
            time.sleep(5)

    return Response(event_generator(), mimetype='text/event-stream')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', default=DEFAULT_LOG,
                        help=f'Path to backfill log file (default: {DEFAULT_LOG})')
    parser.add_argument('--port', type=int, default=5001)
    args = parser.parse_args()
    LOG_PATH = args.log
    print(f'Monitor running at http://localhost:{args.port}')
    print(f'Reading log from: {LOG_PATH}')
    app.run(host='0.0.0.0', port=args.port, threaded=True)
