"""
content_viewer.py — Standalone viewer for content briefs & COSMO relation coverage.

Shows tiered keywords in order of importance + COSMO framework per ASIN.
Modeled after the DWC big analysis content brief viewer.

Usage:
    python3 content_viewer.py
"""
import json
import os
from flask import Flask, jsonify, request as req
import psycopg2
import psycopg2.extras
from schema import get_conn

app = Flask(__name__)

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Niré Beauty — Content Briefs</title>
<style>
  :root { --bg:#f8f9fa; --card:#fff; --accent:#6c5ce7; --accent2:#a29bfe;
          --text:#2d3436; --muted:#636e72; --border:#dfe6e9; --green:#00b894;
          --yellow:#fdcb6e; --red:#e17055; --blue:#0984e3; --lightblue:#74b9ff; }
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:var(--bg);color:var(--text);line-height:1.5;font-size:13px}
  .header{background:linear-gradient(135deg,var(--accent),var(--accent2));
          color:#fff;padding:20px 28px}
  .header h1{font-size:20px;font-weight:600}
  .header p{opacity:.85;font-size:12px;margin-top:3px}
  .container{max-width:1200px;margin:0 auto;padding:20px}

  /* ASIN Tabs */
  .tabs{display:flex;gap:0;border-bottom:2px solid var(--border);margin-bottom:20px;overflow-x:auto}
  .tab{padding:8px 16px;cursor:pointer;font-size:12px;font-weight:500;color:var(--muted);
       border-bottom:2px solid transparent;margin-bottom:-2px;white-space:nowrap;transition:.2s}
  .tab:hover{color:var(--accent)}
  .tab.active{color:var(--accent);border-bottom-color:var(--accent)}
  .tab .sub{font-size:10px;color:var(--muted);margin-left:4px}

  /* KPI Grid */
  .kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px}
  .kpi{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px;text-align:center}
  .kpi .val{font-size:20px;font-weight:700}
  .kpi .lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.3px}
  .kpi .sub{font-size:10px;color:var(--muted)}

  /* Cards */
  .card{background:var(--card);border:1px solid var(--border);border-radius:8px;
        padding:16px;margin-bottom:16px}
  .card h2{font-size:14px;font-weight:600;margin-bottom:10px;color:var(--accent)}

  /* Gap analysis */
  .gap-warn{border-left:4px solid var(--yellow);padding:10px 14px;margin-bottom:14px;
            background:#ffeaa7;border-radius:0 8px 8px 0}
  .gap-ok{border-left:4px solid var(--green);padding:10px 14px;margin-bottom:14px;
          background:#55efc4;border-radius:0 8px 8px 0}
  .badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600;margin:2px}

  /* Keyword tables */
  details{margin-bottom:10px}
  summary{cursor:pointer;font-weight:600;font-size:13px;margin-bottom:6px;user-select:none}
  .tbl-wrap{overflow-x:auto}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{text-align:left;padding:6px 8px;background:var(--bg);font-size:10px;
     text-transform:uppercase;color:var(--muted);position:sticky;top:0}
  td{padding:5px 8px;border-top:1px solid var(--border)}
  .num{text-align:right;font-variant-numeric:tabular-nums}
  .kw-cell{max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .score-bar{display:flex;align-items:center;gap:4px}
  .score-bar .track{width:40px;height:5px;background:var(--border);border-radius:3px}
  .score-bar .fill{height:100%;border-radius:3px}
  .score-bar .val{font-size:11px}

  /* Strategy badges */
  .strat{display:inline-block;padding:1px 8px;border-radius:10px;font-size:10px;font-weight:600}
  .strat-Branded{background:var(--yellow);color:#2d3436}
  .strat-Defend{background:var(--green);color:#fff}
  .strat-Grow{background:var(--blue);color:#fff}
  .strat-Watch{background:#a29bfe;color:#fff}
  .strat-Deprioritize{background:var(--border);color:var(--muted)}

  /* Trend arrows */
  .trend-up{color:var(--green)}
  .trend-down{color:var(--red)}
  .trend-flat{color:var(--muted)}

  /* COSMO framework */
  .cosmo-group-label{font-size:11px;color:var(--muted);text-transform:uppercase;
                     letter-spacing:.5px;margin-bottom:6px;border-bottom:1px solid var(--border);
                     padding-bottom:4px;margin-top:14px}
  .cosmo-row{display:flex;align-items:flex-start;gap:8px;padding:4px 0;font-size:12px}
  .cosmo-badge{flex-shrink:0;padding:2px 8px;border-radius:8px;font-size:10px;
               min-width:115px;text-align:center;margin-top:2px;border:1px solid}
  .cosmo-def{font-size:11px;color:var(--muted)}
  .cosmo-where{font-size:10px;color:var(--muted);opacity:.7}
  .cosmo-input{width:100%;padding:4px 8px;border:1px solid var(--border);border-radius:4px;
               background:var(--bg);color:var(--text);font-size:12px;outline:none;margin-top:3px}
  .cosmo-input:focus{border-color:var(--accent)}

  /* Buttons */
  .btn{padding:4px 12px;border:1px solid var(--border);border-radius:4px;
       background:var(--card);color:var(--text);cursor:pointer;font-size:12px}
  .btn:hover{background:var(--bg)}
  .btn-accent{border-color:var(--accent);color:var(--accent)}

  .no-data{text-align:center;padding:40px;color:var(--muted)}

  @media(max-width:768px){.kpi-grid{grid-template-columns:repeat(3,1fr)}}
</style>
</head>
<body>
<div class="header">
  <h1>Niré Beauty — Content Briefs</h1>
  <p>Tiered keyword priorities + COSMO relation coverage per ASIN</p>
</div>
<div class="container">
  <div class="tabs" id="tabs"></div>
  <div id="panels"></div>
</div>

<script>
const COSMO_DEFS = {
  Used_For_Func:    {def:'The specific function it performs',where:'Title, Bullets',group:'Functional',color:'#e74c3c'},
  Used_To:          {def:'The task it accomplishes',where:'Bullets, Description',group:'Functional',color:'#e74c3c'},
  Capable_Of:       {def:'What it can do / performance claims',where:'Title, Bullets',group:'Functional',color:'#e74c3c'},
  Used_For_Audience:{def:'The target user role',where:'Bullets, Description',group:'Audience',color:'#3498db'},
  Used_By:          {def:'Who typically uses it',where:'Description, Q&A',group:'Audience',color:'#3498db'},
  xIs_A:            {def:'Identity of the typical buyer',where:'Q&A, Description',group:'Audience',color:'#3498db'},
  Used_For_Event:   {def:'The occasion or activity',where:'Description, Q&A',group:'Context',color:'#2ecc71'},
  Used_On:          {def:'Temporal context (when used)',where:'Q&A, Description',group:'Context',color:'#2ecc71'},
  Used_In_Location: {def:'Where it\'s used',where:'Description, Bullets',group:'Context',color:'#2ecc71'},
  Used_In_Body:     {def:'Body area relevance',where:'Q&A, Bullets',group:'Context',color:'#2ecc71'},
  Used_As:          {def:'Functional classification',where:'Title, Bullets',group:'Classification',color:'#9b59b6'},
  Is_A:             {def:'Category identity',where:'Title',group:'Classification',color:'#9b59b6'},
  Used_With:        {def:'Complementary products / compatibility',where:'Bullets, Q&A',group:'Complementary',color:'#f39c12'},
  xInterested_In:   {def:'Shopper interest signals',where:'Description, A+ Content',group:'Complementary',color:'#f39c12'},
  xWant:            {def:'Desired activity or outcome',where:'Bullets, Description',group:'Complementary',color:'#f39c12'},
};

let DATA = [];

// COSMO localStorage persistence
function getCosmo(asin){ try{return JSON.parse(localStorage.getItem('cosmo_nire_'+asin)||'{}')}catch(e){return{}} }
function saveCosmo(asin,key,val){ const e=getCosmo(asin); if(val.trim())e[key]=val.trim(); else delete e[key]; localStorage.setItem('cosmo_nire_'+asin,JSON.stringify(e)); updateCosmoCount(asin); }
function updateCosmoCount(asin){ const n=Object.values(getCosmo(asin)).filter(v=>v).length; const el=document.getElementById('cosmo-count'); if(el){el.textContent=n+' / 15 filled'; el.style.color=n>=12?'var(--green)':n>=6?'var(--yellow)':'var(--muted)';} }
function clearCosmo(asin){ localStorage.removeItem('cosmo_nire_'+asin); showTab(_currentIdx); }

async function init(){
  const res = await fetch('/api/briefs');
  DATA = await res.json();
  renderTabs();
  if(DATA.length) showTab(0);
}

function renderTabs(){
  document.getElementById('tabs').innerHTML = DATA.map((d,i)=>
    `<div class="tab" onclick="showTab(${i})" id="tab-${i}">
      ${d.product_name_short||d.asin}
      <span class="sub">${d.asin}</span>
    </div>`
  ).join('');
}

let _currentIdx = 0;
function showTab(idx){
  _currentIdx = idx;
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('tab-'+idx).classList.add('active');
  renderPanel(DATA[idx]);
}

function fmtN(n){ return n!=null ? Number(n).toLocaleString() : '—'; }
function fmtPct(v){ return v!=null ? (v*100).toFixed(1)+'%' : '—'; }
function stratBadge(s){ return `<span class="strat strat-${s||'Deprioritize'}">${s||'—'}</span>`; }
function trendArrow(v){
  if(v==null) return '<span class="trend-flat">—</span>';
  if(v>0.05) return `<span class="trend-up">▲ ${(v*100).toFixed(0)}%</span>`;
  if(v<-0.05) return `<span class="trend-down">▼ ${(v*100).toFixed(0)}%</span>`;
  return `<span class="trend-flat">— ${(v*100).toFixed(0)}%</span>`;
}
function cvrFmt(v){
  if(v==null) return '—';
  const c = v>=1.2?'var(--green)':v>=0.8?'var(--yellow)':'var(--red)';
  return `<span style="color:${c}">${v.toFixed(2)}x</span>`;
}

function renderPanel(d){
  const p = document.getElementById('panels');
  if(!d.tiers || !Object.keys(d.tiers).length){
    p.innerHTML=`<div class="card no-data"><h2>${d.asin} — ${d.current_title||'Unknown'}</h2>
      <p>No keyword data available. This ASIN may not be enrolled in Brand Analytics SQP.</p></div>`;
    return;
  }

  const s = d.summary||{};
  const gap = d.gap_analysis||{};
  const tierColors = {title:'var(--green)',bullet:'var(--blue)',nice_to_have:'var(--muted)',branded:'var(--yellow)'};
  const tierLabels = {title:'Title',bullet:'Bullet',nice_to_have:'Nice-to-Have',branded:'Branded'};

  const charPct = s.title_char_budget ? Math.round(s.title_chars_used/s.title_char_budget*100) : 0;
  const charColor = charPct>100?'var(--red)':charPct>90?'var(--yellow)':'var(--green)';
  const covPct = s.coverage_pct||0;
  const covColor = covPct>=70?'var(--green)':'var(--yellow)';

  let html = '';

  // Product name
  html += `<div style="font-size:14px;font-weight:600;margin-bottom:14px;color:var(--text)">${d.current_title||d.asin}</div>`;

  // KPI Grid
  html += `<div class="kpi-grid">
    <div class="kpi"><div class="val" style="color:var(--green)">${s.title_keywords||0}</div><div class="lbl">Title Keywords</div><div class="sub">vol: ${fmtN(s.title_volume)}</div></div>
    <div class="kpi"><div class="val" style="color:${charColor}">${s.title_chars_used||0} / ${s.title_char_budget||145}</div><div class="lbl">Title Chars</div><div class="sub">${s.title_chars_remaining||0} remaining</div></div>
    <div class="kpi"><div class="val" style="color:var(--blue)">${s.bullet_keywords||0}</div><div class="lbl">Bullet Keywords</div><div class="sub">vol: ${fmtN(s.bullet_volume)}</div></div>
    <div class="kpi"><div class="val" style="color:var(--muted)">${s.nice_to_have_keywords||0}</div><div class="lbl">Nice-to-Have</div></div>
    <div class="kpi"><div class="val" style="color:var(--yellow)">${s.branded_keywords||0}</div><div class="lbl">Branded</div></div>
    <div class="kpi"><div class="val" style="color:${covColor}">${covPct}%</div><div class="lbl">Volume Coverage</div><div class="sub">title + bullets</div></div>
  </div>`;

  // Gap Analysis
  if(gap.missing_from_title && gap.missing_from_title.length){
    html += `<div class="gap-warn">
      <strong style="color:#d35400">⚠ ${gap.missing_from_title.length} title keyword${gap.missing_from_title.length>1?'s':''} missing from current title:</strong>
      <div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:4px">
        ${gap.missing_from_title.map(kw=>`<span class="badge" style="background:var(--yellow);color:#000">${kw}</span>`).join('')}
      </div>
      ${gap.in_title&&gap.in_title.length?`<div style="margin-top:6px;font-size:11px;color:var(--muted)">Already in title: ${gap.in_title.join(', ')}</div>`:''}
    </div>`;
  } else if(gap.in_title&&gap.in_title.length){
    html += `<div class="gap-ok"><strong style="color:#00855a">✓ All title-tier keywords present in current title</strong></div>`;
  }

  // Title unique words
  if(s.title_unique_words&&s.title_unique_words.length){
    html += `<div style="margin-bottom:12px;font-size:12px;color:var(--muted)">
      <strong>Title unique words:</strong> ${s.title_unique_words.join(', ')}
    </div>`;
  }

  // Action buttons
  html += `<div style="display:flex;gap:8px;margin-bottom:14px">
    <button class="btn" onclick="copyBrief('${d.asin}')">Copy Brief</button>
    <button class="btn" onclick="exportCSV('${d.asin}')">Export CSV</button>
  </div>`;

  // Tier keyword tables
  for(const tier of ['title','bullet','nice_to_have','branded']){
    const rows = d.tiers[tier]||[];
    if(!rows.length) continue;
    html += `<details ${tier==='title'?'open':''} style="margin-bottom:10px">
      <summary style="color:${tierColors[tier]}">
        ${tierLabels[tier]} <span style="font-weight:400;color:var(--muted)">(${rows.length} keywords)</span>
      </summary>
      <div class="tbl-wrap"><table>
      <thead><tr>
        <th class="num">#</th><th>Keyword</th><th class="num">Score</th><th>Strategy</th>
        <th class="num">Volume</th><th class="num">CVR Index</th>
        <th class="num">Click Share</th><th class="num">Purch Share</th>
        <th class="num">Revenue</th><th class="num">Trend</th>
      </tr></thead><tbody>
      ${rows.map(r=>`<tr>
        <td class="num" style="color:var(--muted)">${r.tier_rank}</td>
        <td class="kw-cell" title="${r.search_query}">${r.search_query}</td>
        <td class="num"><div class="score-bar">
          <div class="track"><div class="fill" style="width:${Math.round((r.content_brief_score||0)*100)}%;background:${tierColors[tier]}"></div></div>
          <span class="val" style="color:${tierColors[tier]}">${(+(r.content_brief_score||0)).toFixed(3)}</span>
        </div></td>
        <td>${stratBadge(r.strategy)}</td>
        <td class="num">${r.search_volume?fmtN(r.search_volume):'—'}</td>
        <td class="num">${cvrFmt(r.cvr_index)}</td>
        <td class="num">${fmtPct(r.click_share)}</td>
        <td class="num">${fmtPct(r.purchase_share)}</td>
        <td class="num">${r.revenue_score?'$'+fmtN(Math.round(r.revenue_score)):'—'}</td>
        <td class="num">${trendArrow(r.share_trend)}</td>
      </tr>`).join('')}
      </tbody></table></div>
    </details>`;
  }

  // COSMO Relation Framework
  html += renderCosmo(d.asin);

  p.innerHTML = html;
  updateCosmoCount(d.asin);
}

function renderCosmo(asin){
  const entries = getCosmo(asin);
  const filled = Object.values(entries).filter(v=>v).length;

  // Group relations
  const groups = {};
  Object.entries(COSMO_DEFS).forEach(([key,val])=>{
    if(!groups[val.group]) groups[val.group]=[];
    groups[val.group].push({key,...val});
  });

  let html = `<div class="card" style="border-left:4px solid var(--blue);margin-top:20px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
      <h2 style="margin:0">COSMO Relation Framework</h2>
      <div style="display:flex;align-items:center;gap:10px">
        <span id="cosmo-count" style="font-size:13px;font-weight:700">${filled} / 15 filled</span>
        <button class="btn" style="font-size:11px" onclick="clearCosmo('${asin}')">Clear All</button>
      </div>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">
      Fill in how your product covers each of Amazon's 15 COSMO semantic relations. Entries are saved per ASIN in your browser.
    </div>
    <div style="display:flex;gap:8px;margin-bottom:12px">
      <button class="btn btn-accent" style="font-size:11px" onclick="copyCosmoPrompt('${asin}')">Copy Research Prompt</button>
      <button class="btn" style="font-size:11px" onclick="toggleImport('${asin}')">Paste LLM Response</button>
    </div>
    <div id="cosmo-import" style="display:none;margin-bottom:12px">
      <textarea id="cosmo-import-text" style="width:100%;height:100px;padding:8px;border:1px solid var(--border);border-radius:4px;background:var(--bg);font-size:11px;font-family:monospace;resize:vertical" placeholder="Used_For_Func: apply makeup flawlessly\nUsed_To: create professional looks..."></textarea>
      <div style="display:flex;gap:8px;margin-top:6px">
        <button class="btn" style="background:var(--green);color:#fff;border-color:var(--green);font-size:11px" onclick="importCosmo('${asin}')">Import</button>
        <button class="btn" style="font-size:11px" onclick="document.getElementById('cosmo-import').style.display='none'">Cancel</button>
      </div>
    </div>`;

  Object.entries(groups).forEach(([groupName,rels])=>{
    html += `<div class="cosmo-group-label">${groupName}</div>`;
    rels.forEach(r=>{
      const val = entries[r.key]||'';
      const isFilled = !!val;
      html += `<div class="cosmo-row">
        <span class="cosmo-badge" style="background:${r.color}${isFilled?'22':'0a'};color:${isFilled?r.color:'var(--muted)'};border-color:${r.color}${isFilled?'44':'22'}">
          ${isFilled?'✓ ':''}${r.key.replace(/_/g,' ')}
        </span>
        <div style="flex:1;min-width:0">
          <div style="display:flex;justify-content:space-between;gap:8px">
            <span class="cosmo-def">${r.def}</span>
            <span class="cosmo-where">${r.where}</span>
          </div>
          <input class="cosmo-input" type="text" value="${(val||'').replace(/"/g,'&quot;')}"
            placeholder="How does your product cover this?"
            onchange="saveCosmo('${asin}','${r.key}',this.value)"
            style="${isFilled?'border-color:'+r.color+'44;background:'+r.color+'08':''}">
        </div>
      </div>`;
    });
  });

  html += '</div>';
  return html;
}

function toggleImport(){ document.getElementById('cosmo-import').style.display = document.getElementById('cosmo-import').style.display==='block'?'none':'block'; }
function importCosmo(asin){
  const text = document.getElementById('cosmo-import-text').value;
  if(!text.trim()) return;
  const entries = getCosmo(asin);
  const validKeys = Object.keys(COSMO_DEFS);
  text.split('\n').forEach(line=>{
    const m = line.match(/^\s*([A-Za-z_]+)\s*:\s*(.+)$/);
    if(m && validKeys.includes(m[1]) && m[2].trim().toLowerCase()!=='n/a'){
      entries[m[1]] = m[2].trim();
    }
  });
  localStorage.setItem('cosmo_nire_'+asin, JSON.stringify(entries));
  document.getElementById('cosmo-import').style.display='none';
  showTab(_currentIdx);
}
function copyCosmoPrompt(asin){
  const d = DATA.find(x=>x.asin===asin);
  const title = d?d.current_title:asin;
  const defs = Object.entries(COSMO_DEFS).map(([k,v])=>k+': '+v.def+' (belongs in: '+v.where+')').join('\n');
  const prompt = `I need you to research this Amazon product and draft how it maps to each of Amazon's 15 COSMO (commonsense knowledge) relations. This will be used to optimize the listing for COSMO and Rufus.\n\nProduct: ${title}\nASIN: ${asin}\n\nFor each COSMO relation below, write a short phrase (5-15 words) describing how this specific product covers that relation. If a relation genuinely doesn't apply, write "N/A".\n\nThe 15 COSMO relations:\n${defs}\n\nReturn your answer as a simple list:\nUsed_For_Func: [your answer]\nUsed_To: [your answer]\nCapable_Of: [your answer]\nUsed_For_Audience: [your answer]\nUsed_By: [your answer]\nxIs_A: [your answer]\nUsed_For_Event: [your answer]\nUsed_On: [your answer]\nUsed_In_Location: [your answer]\nUsed_In_Body: [your answer]\nUsed_As: [your answer]\nIs_A: [your answer]\nUsed_With: [your answer]\nxInterested_In: [your answer]\nxWant: [your answer]`;
  navigator.clipboard.writeText(prompt);
  event.target.textContent='Copied!'; setTimeout(()=>event.target.textContent='Copy Research Prompt',1500);
}

// Copy / Export
function copyBrief(asin){
  const d = DATA.find(x=>x.asin===asin);
  if(!d) return;
  let text = `Content Brief: ${d.current_title}\nASIN: ${asin}\n\n`;
  for(const tier of ['title','bullet','nice_to_have','branded']){
    const rows = (d.tiers||{})[tier]||[];
    if(!rows.length) continue;
    text += `=== ${tier.toUpperCase()} (${rows.length} keywords) ===\n`;
    rows.forEach(r=>{ text += `  ${r.tier_rank}. ${r.search_query} | score:${(r.content_brief_score||0).toFixed(3)} | vol:${r.search_volume||0} | strategy:${r.strategy||''}\n`; });
    text += '\n';
  }
  navigator.clipboard.writeText(text);
}
function exportCSV(asin){
  const d = DATA.find(x=>x.asin===asin);
  if(!d) return;
  let csv = 'tier,rank,keyword,score,strategy,volume,cvr_index,click_share,purchase_share,revenue_score,trend\n';
  for(const tier of ['title','bullet','nice_to_have','branded']){
    ((d.tiers||{})[tier]||[]).forEach(r=>{
      csv += [tier,r.tier_rank,'"'+r.search_query+'"',(r.content_brief_score||0).toFixed(4),r.strategy||'',r.search_volume||'',r.cvr_index||'',r.click_share||'',r.purchase_share||'',r.revenue_score||'',r.share_trend||''].join(',')+'\n';
    });
  }
  const blob = new Blob([csv],{type:'text/csv'});
  const a = document.createElement('a');
  a.href=URL.createObjectURL(blob); a.download=`content_brief_${asin}.csv`; a.click();
}

init();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return HTML


@app.route("/api/briefs")
def api_briefs():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get all content briefs with full keyword data
    cur.execute("""
        SELECT asin, search_query, content_brief_score, content_tier, tier_rank,
               search_volume, keyword_relevance, keyword_role, keyword_type, strategy,
               cvr_index, click_share, purchase_share, revenue_score,
               headroom_pct, momentum_pct, share_trend
        FROM content_briefs
        ORDER BY asin, content_tier, tier_rank
    """)
    asin_tiers: dict = {}
    for r in cur.fetchall():
        row = dict(r)
        asin = row["asin"]
        tier = row["content_tier"]
        asin_tiers.setdefault(asin, {}).setdefault(tier, []).append(row)

    # Get listings for product names + current titles
    cur.execute("SELECT asin, product_name FROM listings")
    listings = {r["asin"]: r["product_name"] for r in cur.fetchall()}

    # Build results
    target_asins = ['B01FQZNFYG', 'B08GM17CZF', 'B0B63QMTBQ',
                    'B0CHMQGG2F', 'B08B9124NB', 'B089MFSYWT']

    # Include all ASINs that have briefs + target ASINs
    all_asins = set(asin_tiers.keys()) | set(target_asins)
    results = []

    for asin in all_asins:
        tiers = asin_tiers.get(asin, {})
        current_title = listings.get(asin, asin)

        # Build summary
        title_kws = tiers.get("title", [])
        bullet_kws = tiers.get("bullet", [])
        nth_kws = tiers.get("nice_to_have", [])
        branded_kws = tiers.get("branded", [])

        title_volume = sum(k.get("search_volume") or 0 for k in title_kws)
        bullet_volume = sum(k.get("search_volume") or 0 for k in bullet_kws)
        total_volume = title_volume + bullet_volume + sum(k.get("search_volume") or 0 for k in nth_kws)

        # Title character analysis
        title_unique_words = set()
        for kw in title_kws:
            for w in kw["search_query"].lower().split():
                if len(w) > 2 and w not in {"a","an","the","and","or","for","of","in","to","with","by"}:
                    title_unique_words.add(w)
        title_chars_used = sum(len(w) for w in title_unique_words) + max(0, len(title_unique_words)-1)
        title_char_budget = 145  # 200 - brand - descriptor

        # Gap analysis: which title keywords are in current title?
        current_lower = (current_title or "").lower()
        in_title = []
        missing = []
        for kw in title_kws:
            q = kw["search_query"].lower()
            # Check if key concept words are in the current title
            concept_words = [w for w in q.split() if len(w) > 2
                           and w not in {"a","an","the","and","or","for","of","in","to","with","by",
                                         "set","brush","makeup","brushes"}]
            if all(w in current_lower for w in concept_words):
                in_title.append(kw["search_query"])
            else:
                missing.append(kw["search_query"])

        coverage_pct = round((title_volume + bullet_volume) / total_volume * 100) if total_volume else 0

        # Short name for tab
        short = current_title[:40] + "..." if current_title and len(current_title) > 40 else current_title

        results.append({
            "asin": asin,
            "current_title": current_title,
            "product_name_short": short,
            "tiers": tiers,
            "summary": {
                "title_keywords": len(title_kws),
                "bullet_keywords": len(bullet_kws),
                "nice_to_have_keywords": len(nth_kws),
                "branded_keywords": len(branded_kws),
                "title_volume": title_volume,
                "bullet_volume": bullet_volume,
                "coverage_pct": coverage_pct,
                "title_unique_words": sorted(title_unique_words),
                "title_chars_used": title_chars_used,
                "title_char_budget": title_char_budget,
                "title_chars_remaining": max(0, title_char_budget - title_chars_used),
            },
            "gap_analysis": {
                "in_title": in_title,
                "missing_from_title": missing,
            },
        })

    # Sort by total volume desc, no-data ASINs last
    results.sort(key=lambda r: sum(
        sum(k.get("search_volume") or 0 for k in kws)
        for kws in r["tiers"].values()
    ), reverse=True)

    conn.close()
    return jsonify(results)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5053))
    print(f"Content Brief Viewer -> http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)
