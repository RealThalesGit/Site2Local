"""Site2Local: CDN viewer HTML page (self-contained, 50 ms live refresh).

The template below is kept byte-identical to the monolith's inline string
(s2l.py, inside _s2l_viewer_endpoint) so the served page matches exactly —
match-based modularization: no byte of HTML/CSS/JS was touched.
"""

_CDN_VIEWER_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CDN Viewer</title>
<style>
  :root{
    --bg:#000000; --pnl:#000000; --pnl2:#141414; --bd:#ffffff; --bd2:rgba(255,255,255,.18);
    --fg:#ffffff; --fg2:#ffffff; --dim:#9d9d9d; --acc:#ffffff; --grn:#4ade80;
    --ylw:#fbbf24; --red:#f87171; --mag:#c084fc; --cy:#7dd3fc;
    --ok:#4ade80; --warn:#fbbf24; --bad:#f87171;
  }
  [data-theme="light"]{
    --bg:#ffffff; --pnl:#ffffff; --pnl2:#f0f0f0; --bd:#000000; --bd2:rgba(0,0,0,.18);
    --fg:#000000; --fg2:#000000; --dim:#595959; --acc:#000000; --grn:#16a34a;
    --ylw:#d97706; --red:#dc2626; --mag:#9333ea; --cy:#0284c7;
    --ok:#16a34a; --warn:#d97706; --bad:#dc2626;
  }
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:var(--bg);color:var(--fg);
    font-family:"SF Mono",Menlo,Consolas,"Liberation Mono",monospace;font-size:13px}
  body{display:flex;flex-direction:column;min-height:100vh}
  ::selection{background:var(--acc);color:var(--bg)}

  ::-webkit-scrollbar{width:10px;height:10px}
  ::-webkit-scrollbar-track{background:var(--bg)}
  ::-webkit-scrollbar-thumb{background:var(--bd);border-radius:6px;border:2px solid var(--bg)}
  ::-webkit-scrollbar-thumb:hover{background:var(--dim)}
  ::-webkit-scrollbar-corner{background:var(--bg)}

  /* ── Header ───────────────────────────────────────────────────────── */
  header{padding:14px 20px;border-bottom:1px solid var(--bd);background:var(--pnl);
    display:flex;align-items:center;gap:14px;flex-wrap:wrap}
  .pulse{width:11px;height:11px;border-radius:50%;background:var(--grn);
    box-shadow:0 0 0 0 rgba(63,185,80,.5);animation:pulse 1.6s infinite;display:inline-block;flex-shrink:0}
  .pulse.stale{background:var(--ylw);animation:none}
  .pulse.dead{background:var(--red);animation:none}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(63,185,80,.45)}70%{box-shadow:0 0 0 9px rgba(63,185,80,0)}100%{box-shadow:0 0 0 0 rgba(63,185,80,0)}}
  h1{font-size:15px;margin:0;letter-spacing:.5px;color:var(--fg2);font-weight:700;display:flex;align-items:center;gap:8px}
  h1 .v{color:var(--mag);font-weight:700}
  .target{color:var(--grn);font-size:12px;font-weight:600;display:inline-flex;align-items:center;gap:6px}
  .proxy{color:var(--dim);font-size:12px}
  .uptime{color:var(--dim);font-size:11px;padding:2px 7px;border:1px solid var(--bd);border-radius:4px}
  .stats{margin-left:auto;display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
  .stat{position:relative;display:flex;flex-direction:column;align-items:center;line-height:1.15;
    padding:6px 12px;border-radius:6px;background:var(--pnl2);min-width:62px;border:1px solid var(--bd2);cursor:default;
    transition:transform .12s, border-color .12s}
  .stat:hover{transform:translateY(-1px);border-color:var(--acc)}
  .stat .n{font-size:16px;font-weight:700;color:var(--fg2);font-variant-numeric:tabular-nums}
  .stat .l{font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:.7px;margin-top:1px}
  .stat[data-tip]:hover::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 6px);left:50%;
    transform:translateX(-50%);background:var(--pnl2);color:var(--fg2);border:1px solid var(--bd);
    padding:4px 8px;border-radius:4px;font-size:10px;white-space:nowrap;z-index:50;pointer-events:none}
  /* Broken stat: red-tinted when >0 (clickable shortcut to the Broken filter).
     Selected stat: accent-tinted, only visible while a selection exists. */
  .stat-broken{cursor:pointer}
  .stat-broken .n{color:var(--dim)}
  .stat-broken.has-broken .n{color:var(--bad)}
  .stat-broken.has-broken:hover{border-color:var(--bad)}
  .stat-broken.has-broken.active{background:rgba(248,81,73,.15);border-color:var(--bad)}
  .stat-selected .n{color:var(--acc)}
  .stat-selected:hover{border-color:var(--acc)}


  /* ── Toolbar ──────────────────────────────────────────────────────── */
  .toolbar{display:flex;gap:10px;align-items:center;margin:14px 20px 0;flex-wrap:wrap}
  /* Toolbar group dividers — thin vertical separators that break the toolbar
     into logical groups (refresh | filter | pause | bulk-actions | selection |
     utilities) so 10+ controls don't read as an undifferentiated band. Faint
     by default, invisible when the selection group is hidden. */
  .tb-sep{width:1px;height:22px;background:var(--bd2);flex-shrink:0;opacity:.6}
  .bar-group{display:flex;align-items:center;gap:8px;color:var(--dim);font-size:12px;
    padding:5px 10px;background:var(--pnl);border:1px solid var(--bd);border-radius:6px}
  .bar-group .rate{color:var(--acc);font-weight:700}
  .bar-group .tag{display:inline-block;padding:1px 7px;border-radius:4px;background:var(--pnl2);
    color:var(--dim);font-size:10px;border:1px solid var(--bd2)}
  .bar-group .tag.ok{color:var(--grn);border-color:var(--grn)}
  .bar-group .tag.err{color:var(--red);border-color:var(--red)}
  .search{flex:1;min-width:180px;display:flex;align-items:center;gap:6px;
    padding:6px 10px;background:var(--pnl);border:1px solid var(--bd);border-radius:6px}
  .search:focus-within{border-color:var(--acc);box-shadow:0 0 0 3px rgba(255,255,255,.15)}
  .search svg{width:14px;height:14px;color:var(--dim);flex-shrink:0}
  .search input{flex:1;background:transparent;border:0;outline:0;color:var(--fg);font:inherit;font-size:12px}
  .search input::placeholder{color:var(--dim)}
  .search .count{color:var(--dim);font-size:10px;padding-left:6px;border-left:1px solid var(--bd2);margin-left:4px}
  .btn{display:inline-flex;align-items:center;gap:5px;padding:6px 12px;background:var(--pnl);
    border:1px solid var(--bd);border-radius:6px;color:var(--fg);font:inherit;font-size:11px;font-weight:600;
    cursor:pointer;transition:background .12s,border-color .12s,transform .08s;white-space:nowrap}
  .btn:hover{background:var(--pnl2);border-color:var(--acc)}
  .btn:active{transform:translateY(1px)}
  .btn.active{background:var(--acc);color:var(--bg);border-color:var(--acc)}
  .btn svg{width:13px;height:13px}
  .sel{padding:6px 8px;background:var(--pnl);border:1px solid var(--bd);border-radius:6px;
    color:var(--fg);font:inherit;font-size:11px;cursor:pointer;outline:0}
  .sel:focus{border-color:var(--acc)}

  /* ── Table ────────────────────────────────────────────────────────── */
  main{flex:1;padding:14px 20px;overflow:auto}
  /* Table wrapper enables horizontal scroll on narrow screens without breaking
     the page layout. The wrapper is created by render() around the <table>. */
  .table-wrap{overflow-x:auto;border:1px solid var(--bd);border-radius:8px}
  table{width:100%;border-collapse:collapse;background:var(--pnl);
    border:0;overflow:hidden;min-width:560px}
  thead th{background:var(--pnl2);color:var(--acc);text-align:left;
    padding:10px 14px;font-size:10px;text-transform:uppercase;letter-spacing:.6px;
    border-bottom:1px solid var(--bd);position:sticky;top:0;z-index:2;user-select:none}
  thead th.sortable{cursor:pointer}
  thead th.sortable:hover{color:var(--fg2);background:rgba(255,255,255,.07)}
  /* Caret discoverability: a non-sorted sortable header shows a faint
     hint on hover (CSS-only, no JS) so the user can tell it's clickable —
     otherwise the caret is invisible until first click (the round-3 VLM
     note). Once a column is active the caret is full-opacity and points
     the right way. */
  thead th .caret{display:inline-block;width:0;height:0;margin-left:5px;vertical-align:middle;opacity:0;transition:opacity .12s}
  /* Default hint on hover for a sortable-but-not-yet-sorted header: a
     subtle up-down indicator. Achieved with two stacked triangles via
     border tricks on a 0-size box is messy, so instead we show a faint
     down-triangle (the most common initial sort direction). */
  thead th.sortable:not(.sorted-asc):not(.sorted-desc) .caret{border-left:4px solid transparent;border-right:4px solid transparent;border-top:5px solid var(--dim)}
  thead th.sortable:hover .caret{opacity:.7}
  thead th.sorted-asc .caret{border-left:0;border-right:0;border-top:0;border-bottom:5px solid var(--acc);opacity:1}
  thead th.sorted-desc .caret{border-left:0;border-right:0;border-bottom:0;border-top:5px solid var(--acc);opacity:1}
  tbody td{padding:9px 14px;border-bottom:1px solid var(--bd2)}
  tbody tr:last-child td{border-bottom:none}
  tbody tr{transition:background .1s}
  /* Zebra striping on the main CDN table — subtle alternating lift so the
     eye can track a row across wide lines without per-row borders doing
     all the work (reduces the "border fatigue" of a pure black+white grid).
     Hover is a stronger lift so the pointed row is still obvious. */
  tbody tr:nth-child(even){background:rgba(255,255,255,.025)}
  tbody tr:hover{background:rgba(255,255,255,.08)}
  /* Selected rows (bulk-action mode): accent-tinted background + left accent
     bar so the selection set is scannable at a glance. Survives the 50 ms
     refresh because the .selected class lives on the stable <tr> node. */
  tbody tr.selected{background:rgba(255,255,255,.12);box-shadow:inset 3px 0 0 var(--acc)}
  tbody tr.selected:hover{background:rgba(255,255,255,.18)}
  tbody tr.new{animation:flash 1.1s ease-out}
  tbody tr.copy-flash{animation:copyflash .8s ease-out}
  @keyframes flash{0%{background:rgba(255,255,255,.20)}100%{background:transparent}}
  @keyframes copyflash{0%{background:rgba(63,185,80,.30)}100%{background:transparent}}
  td .host{color:var(--fg2);font-weight:500}
  td .port{color:var(--ylw);font-weight:700;font-variant-numeric:tabular-nums}
  td a{color:var(--acc);text-decoration:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:280px;display:inline-block;vertical-align:bottom}
  td a:hover{text-decoration:underline}
  td .copy-btn{opacity:0;transition:opacity .12s;background:none;border:1px solid var(--bd2);
    color:var(--dim);border-radius:4px;padding:2px 7px;font-size:10px;cursor:pointer;margin-left:6px}
  tbody tr:hover .copy-btn{opacity:1}
  td .copy-btn:hover{color:var(--grn);border-color:var(--grn)}
  td .copy-btn.curl-btn:hover{color:var(--cy);border-color:var(--cy)}
  td .idx{color:var(--dim);font-variant-numeric:tabular-nums}
  /* Health cell: dot + status + RTT + last-seen */
  .health{display:flex;align-items:center;gap:8px;min-width:170px}
  .hdot{width:8px;height:8px;border-radius:50%;flex-shrink:0;background:var(--dim)}
  .hdot.ok{background:var(--ok);box-shadow:0 0 5px rgba(63,185,80,.6)}
  .hdot.warn{background:var(--warn)}
  .hdot.bad{background:var(--bad);box-shadow:0 0 5px rgba(248,81,73,.6)}
  .hdot.unknown{background:var(--dim);opacity:.4}
  .hmeta{display:flex;flex-direction:column;line-height:1.25;font-size:10px;color:var(--dim)}
  /* Status text stays white+bold in ALL cases — the dot carries the color
     signal (ok/warn/bad). The old version tinted the status text red/amber
     on a faint colored background, which read as a low-contrast "alert
     badge" on black and was hard to scan. Now it's clean white with the dot
     as the only color cue — easier to read and to scan down the column. */
  .hmeta .status{font-weight:700;color:var(--fg2);font-size:11px;letter-spacing:.2px}
  .hmeta .status.bad{color:var(--bad)}
  .hmeta .status.warn{color:var(--warn)}
  .hmeta .sub{color:var(--dim)}
  .errbadge{display:inline-block;font-size:9px;font-weight:700;padding:0 5px;border-radius:8px;
    margin-left:5px;line-height:14px;vertical-align:middle;border:1px solid;cursor:default}
  .errbadge.warn{color:var(--warn);border-color:var(--warn);background:rgba(210,153,34,.1)}
  .errbadge.bad{color:var(--bad);border-color:var(--bad);background:rgba(248,81,73,.12)}
  .empty{padding:50px;text-align:center;color:var(--dim)}
  .empty .big{font-size:30px;color:var(--cy);margin-bottom:10px;font-weight:700}
  .empty .sub{font-size:12px;line-height:1.7;max-width:420px;margin:0 auto}
  /* "Clear filter" button inside the empty state — the recovery path when a
     filter narrows the table to 0 rows. Styled to match the toolbar buttons
     but slightly larger + centered. */
  .empty-clear{display:inline-flex;align-items:center;gap:6px;padding:7px 16px;
    background:var(--pnl);border:1px solid var(--bd);border-radius:6px;color:var(--fg);
    font:inherit;font-size:12px;font-weight:600;cursor:pointer;transition:background .12s,border-color .12s}
  .empty-clear:hover{background:var(--pnl2);border-color:var(--acc)}

  /* ── Flag chips ───────────────────────────────────────────────────── */
  /* Smaller + tighter so 14 chips don't dominate the footer. OFF chips are
     very faint (they're just inventory); ON chips get the green accent so
     the active config is scannable at a glance. */
  .flags{display:flex;gap:3px;flex-wrap:wrap;max-width:60%}
  .chip{font-size:8px;padding:1px 5px;border-radius:8px;border:1px solid var(--bd2);
    color:var(--dim);text-transform:uppercase;letter-spacing:.3px;cursor:default;opacity:.5}
  .chip.on{color:var(--grn);border-color:var(--grn);background:rgba(63,185,80,.08);opacity:1}

  /* ── Footer ───────────────────────────────────────────────────────── */
  footer{padding:9px 20px;border-top:1px solid var(--bd);background:var(--pnl);
    color:var(--dim);font-size:11px;display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center}
  .ft-updated{color:var(--grn)}
  .ft-updated.stale{color:var(--ylw)}
  .ft-updated.dead{color:var(--red)}
  .err{color:var(--red)}
  .toast{position:fixed;bottom:54px;left:50%;transform:translateX(-50%) translateY(20px);
    background:var(--grn);color:#fff;padding:8px 18px;border-radius:6px;font-size:12px;font-weight:600;
    opacity:0;transition:opacity .2s,transform .2s;pointer-events:none;z-index:100;box-shadow:0 4px 14px rgba(0,0,0,.3)}
  .toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
  .toast.warn{background:var(--warn)}
  .kbd{display:inline-block;padding:1px 5px;border:1px solid var(--bd);border-radius:3px;
    background:var(--pnl2);color:var(--dim);font-size:9px;font-weight:700}
  /* Help overlay (? key) */
  .help-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:center;justify-content:center;z-index:300;padding:20px}
  .help-overlay.open{display:flex}
  .help-card{background:var(--pnl);border:1px solid var(--bd);border-radius:10px;width:100%;max-width:440px;max-height:85vh;overflow:hidden;box-shadow:0 8px 32px rgba(0,0,0,.4)}
  .help-head{padding:14px 18px;border-bottom:1px solid var(--bd2);display:flex;align-items:center;justify-content:space-between;font-size:13px;font-weight:700;color:var(--fg2)}
  .help-close{background:none;border:0;color:var(--dim);font-size:20px;cursor:pointer;padding:0 4px}
  .help-close:hover{color:var(--red)}
  .help-body{padding:10px 18px;overflow:auto}
  .help-row{display:flex;align-items:center;gap:12px;padding:7px 0;border-bottom:1px solid var(--bd2)}
  .help-row:last-child{border-bottom:0}
  .help-row .kbd{min-width:36px;text-align:center;font-size:10px;padding:2px 6px}
  .help-row span:last-child{color:var(--fg);font-size:12px}
  .help-foot{padding:10px 18px;border-top:1px solid var(--bd2);color:var(--dim);font-size:10px}
</style>
</head>
<body>
<header>
  <span class="pulse" id="pulse" title="connection health"></span>
  <h1>CDN Viewer</h1>
  <span class="target" id="target-wrap">
    <span id="target"></span>
  </span>
  <span class="proxy" id="proxy" title="proxy listen address"></span>
  <span class="uptime" id="uptime" title="process uptime">—</span>
</header>

<div class="toolbar">
  <div class="bar-group">
    <span>Refresh</span><span class="rate" id="rate">50ms</span>
    <select class="sel" id="rate-sel" title="Change Poll Interval">
      <option value="50">50 ms</option>
      <option value="250">250 ms</option>
      <option value="1000">1 s</option>
      <option value="2000">2 s</option>
      <option value="0">paused</option>
    </select>
  </div>
  <div class="search">
    <input type="text" id="filter" placeholder="Filter CDN Hosts…" autocomplete="off">
    <span class="count" id="filter-count"></span>
  </div>
  <span class="tb-sep" aria-hidden="true"></span>
  <button class="btn" id="pause-btn" title="Pause / Resume Polling (P)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M5 3.5v9a.5.5 0 0 0 .8.4l7-4.5a.5.5 0 0 0 0-.8l-7-4.5A.5.5 0 0 0 5 3.5Z"/></svg>
    <span id="pause-label">Pause</span>
  </button>
  <span class="tb-sep" aria-hidden="true"></span>
  <button class="btn" id="open-all-btn" title="Open Every CDN Port in a New Tab (O)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M3.75 2A1.75 1.75 0 0 0 2 3.75v8.5c0 .966.784 1.75 1.75 1.75h8.5A1.75 1.75 0 0 0 14 12.25v-8.5A1.75 1.75 0 0 0 12.25 2h-8.5ZM5 10.75a.75.75 0 0 1 .75-.75h4.5a.75.75 0 0 1 0 1.5h-4.5a.75.75 0 0 1-.75-.75Z"/></svg>
    Open All
  </button>
  <button class="btn" id="broken-btn" title="Select Broken CDNs (5xx/4xx) — Filters the Table to Hosts With Errors (B)" style="border-color:var(--bad);color:var(--bad)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0ZM5.5 4.75a.75.75 0 0 0-1.5 0v3.5a.75.75 0 0 0 1.5 0v-3.5Zm.25 6a1 1 0 1 0 2 0 1 1 0 0 0-2 0Z"/></svg>
    Broken
  </button>
  <button class="btn" id="copy-all-btn" title="Copy All (Visible) CDN URLs to Clipboard (Shift+A)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M0 6.75C0 5.784.784 5 1.75 5h1.5a.75.75 0 0 1 0 1.5h-1.5a.25.25 0 0 0-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 0 0 .25-.25v-1.5a.75.75 0 0 1 1.5 0v1.5A1.75 1.75 0 0 1 9.25 16h-7.5A1.75 1.75 0 0 1 0 14.25Z"/><path d="M5 1.75C5 .784 5.784 0 6.75 0h7.5C15.216 0 16 .784 16 1.75v7.5A1.75 1.75 0 0 1 14.25 11h-7.5A1.75 1.75 0 0 1 5 9.25Zm1.75-.25a.25.25 0 0 0-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 0 0 .25-.25v-7.5a.25.25 0 0 0-.25-.25Z"/></svg>
    Copy All
  </button>
  <span class="tb-sep tb-sel-grp" aria-hidden="true" style="display:none"></span>
  <button class="btn" id="open-sel-btn" title="Open the Selected CDN URLs in New Tabs" style="display:none">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M3.75 2A1.75 1.75 0 0 0 2 3.75v8.5c0 .966.784 1.75 1.75 1.75h8.5A1.75 1.75 0 0 0 14 12.25v-8.5A1.75 1.75 0 0 0 12.25 2h-8.5Zm6.5 9.25a.75.75 0 0 1-1.5 0V6.56L6.78 8.28a.75.75 0 0 1-1.06-1.06l3-3a.75.75 0 0 1 1.06 0l3 3a.75.75 0 1 1-1.06 1.06L9.75 6.56v4.69Z"/></svg>
    <span id="open-sel-label">Open 0</span>
  </button>
  <button class="btn" id="copy-sel-btn" title="Copy the Selected CDN URLs (S)" style="display:none">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M3.75 1.5a.75.75 0 0 0 0 1.5h8.5a.75.75 0 0 0 0-1.5h-8.5ZM3 5.75A.75.75 0 0 1 3.75 5h8.5a.75.75 0 0 1 0 1.5h-8.5A.75.75 0 0 1 3 5.75Zm0 4A.75.75 0 0 1 3.75 9h8.5a.75.75 0 0 1 0 1.5h-8.5A.75.75 0 0 1 3 9.75ZM3.75 13a.75.75 0 0 0 0 1.5h8.5a.75.75 0 0 0 0-1.5h-8.5Z"/></svg>
    <span id="copy-sel-label">Copy 0</span>
  </button>
  <span class="tb-sep" aria-hidden="true"></span>
  <button class="btn" id="reset-btn" title="Reset All Counters for a Clean Benchmark Window (R)" style="border-color:var(--warn);color:var(--warn)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M8 3a5 5 0 1 1-4.546 2.914.75.75 0 0 0-1.36-.632A6.5 6.5 0 1 0 8 1.5v-.994a.25.25 0 0 0-.382-.214L4.5 2.5l3.118 1.993A.25.25 0 0 0 8 4.279Z"/></svg>
    Reset
  </button>
  <button class="btn" id="theme-btn" title="Toggle Dark / Light Theme (T)">
    <svg viewBox="0 0 16 16" fill="currentColor"><path d="M8 11a3 3 0 1 1 0-6 3 3 0 0 1 0 6Zm0-4.5a1.5 1.5 0 1 0 0 3 1.5 1.5 0 0 0 0-3ZM8 0a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0V.75A.75.75 0 0 1 8 0Zm0 13a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 8 13ZM2.34 2.34a.75.75 0 0 1 1.06 0l1.06 1.06A.75.75 0 0 1 3.4 4.46L2.34 3.4a.75.75 0 0 1 0-1.06Zm9.14 9.14a.75.75 0 0 1 1.06 0l1.06 1.06a.75.75 0 1 1-1.06 1.06l-1.06-1.06a.75.75 0 0 1 0-1.06ZM0 8a.75.75 0 0 1 .75-.75h1.5a.75.75 0 0 1 0 1.5h-1.5A.75.75 0 0 1 0 8Zm13 0a.75.75 0 0 1 .75-.75h1.5a.75.75 0 0 1 0 1.5h-1.5A.75.75 0 0 1 13 8ZM2.34 13.66a.75.75 0 0 1 0-1.06l1.06-1.06a.75.75 0 1 1 1.06 1.06l-1.06 1.06a.75.75 0 0 1-1.06 0Zm9.14-9.14a.75.75 0 0 1 0-1.06l1.06-1.06a.75.75 0 1 1 1.06 1.06l-1.06 1.06a.75.75 0 0 1-1.06 0Z"/></svg>
    Theme
  </button>
</div>
<main>
  <div class="table-wrap">
  <table>
    <thead><tr>
      <th class="sortable" data-sort="idx" style="width:42px">#<span class="caret"></span></th>
      <th class="sortable" data-sort="host">CDN HOST<span class="caret"></span></th>
      <th class="sortable" data-sort="port" style="width:80px">PORT<span class="caret"></span></th>
      <th class="sortable" data-sort="health" style="width:170px">HEALTH<span class="caret"></span></th>
      <th class="sortable" data-sort="url">LOCAL URL<span class="caret"></span></th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  </div>
  <div class="empty" id="empty" style="display:none">
    <div class="big" id="empty-big">No MULTIPORT CDNs yet</div>
    <div class="sub" id="empty-sub">They appear here in real time as the proxy registers them.<br>Browse the target site to populate this table.</div>
    <button class="btn empty-clear" id="empty-clear" style="display:none;margin-top:14px">Clear Filter</button>
  </div>
</main>
<footer>
  <span id="ft-left"><span class="tag" id="latency">— ms</span> · <span class="ft-updated" id="ft-updated" title="Time Since the Last Successful Poll">Updated —</span> · <span class="kbd">P</span> Pause <span class="kbd">/</span> Filter <span class="kbd">?</span> Help</span>
  <div class="stats">
    <div class="stat" data-tip="CDN hosts with a dedicated port"><span class="n" id="s-active">0</span><span class="l">active</span></div>
    <div class="stat" data-tip="CDN servers still starting up"><span class="n" id="s-pending">0</span><span class="l">pending</span></div>
    <div class="stat" data-tip="CDN hosts sharing /__s2l_ext__/ (no dedicated port)"><span class="n" id="s-shared">0</span><span class="l">shared</span></div>
    <div class="stat" data-tip="Total requests proxied through the main server"><span class="n" id="s-proxied">0</span><span class="l">proxied</span></div>
    <div class="stat" data-tip="CDN assets fetched from upstream"><span class="n" id="s-cdn">0</span><span class="l">cdn</span></div>
    <div class="stat" data-tip="HTTP request+response pairs written to captures/"><span class="n" id="s-cap">0</span><span class="l">captured</span></div>
    <div class="stat" data-tip="WebSocket messages written to captures_ws/"><span class="n" id="s-wscap">0</span><span class="l">ws msg</span></div>
    <div class="stat stat-broken" data-tip="CDN hosts with 4xx/5xx status or errors (click to filter)"><span class="n" id="s-broken">0</span><span class="l">broken</span></div>
    <div class="stat stat-selected" data-tip="rows selected for bulk action (Shift+click for range)" style="display:none"><span class="n" id="s-selected">0</span><span class="l">sel</span></div>
  </div>
  <span class="flags" id="flags"></span>
  <span id="ft-right">—</span>
</footer>
<div class="toast" id="toast"></div>
<!-- Help overlay (? key) -->
<div class="help-overlay" id="help-overlay">
  <div class="help-card">
    <div class="help-head"><span>S2L Viewer — Keyboard Shortcuts</span><button class="help-close" id="help-close">&times;</button></div>
    <div class="help-body">
      <div class="help-row"><span class="kbd">P</span><span>Pause / Resume Polling</span></div>
      <div class="help-row"><span class="kbd">T</span><span>Toggle Dark / Light Theme</span></div>
      <div class="help-row"><span class="kbd">O</span><span>Open All CDN Ports in New Tabs</span></div>
      <div class="help-row"><span class="kbd">⇧A</span><span>Copy All CDN URLs to Clipboard</span></div>
      <div class="help-row"><span class="kbd">Esc</span><span>Clear Filter / Selection / Close Overlay</span></div>
      <div class="help-row"><span class="kbd">R</span><span>Reset All Counters (Benchmark Window)</span></div>
      <div class="help-row"><span class="kbd">/</span><span>Focus the CDN Host Filter</span></div>
      <div class="help-row"><span class="kbd">H</span><span>Sort by Health (Broken CDNs First)</span></div>
      <div class="help-row"><span class="kbd">B</span><span>Toggle the Broken-CDN Filter</span></div>
      <div class="help-row"><span class="kbd">⇧B</span><span>Select All Broken CDNs (Filter + Select in One Step)</span></div>
      <div class="help-row"><span class="kbd">S</span><span>Copy the Selected CDN URLs</span></div>
      <div class="help-row"><span class="kbd">⇧O</span><span>Open the Selected CDN URLs in New Tabs</span></div>
      <div class="help-row"><span class="kbd">⌃A</span><span>Select All Visible CDN Rows</span></div>
      <div class="help-row"><span class="kbd">?</span><span>Toggle This Help Overlay</span></div>
      <div class="help-row"><span class="kbd">Esc</span><span>Clear Filter / Selection / Close Overlay</span></div>
    </div>
    <div class="help-foot">Click any column header to sort · Click a row to select it (Shift+click = range, Ctrl+click = add) · Polling auto-pauses while a selection is active · Hover a row for copy/cURL buttons</div>
  </div>
</div>
<script>
(function(){
  "use strict";
  var DEFAULT_REFRESH=50, rowsEl=document.getElementById("rows"), empty=document.getElementById("empty");
  var rowMap=Object.create(null);
  var inFlight=false, consecutiveErr=0, paused=false, refreshMs=DEFAULT_REFRESH;
  var sortKey="idx", sortDir="asc", filterText="";
  var lastData=null;
  var lastPollOk=0;                  // ms timestamp of last successful poll (for "updated Xs ago")
  // Bulk-selection state: a Set of host names the user has selected (click on
  // a row's non-button area toggles; Shift+click selects a range from the
  // last anchor). The selection survives the 50 ms refresh because it lives
  // on the stable <tr> node (.selected class) + in this Set.
  var selectedHosts=new Set();
  var selAnchorHost=null;            // last clicked host (for Shift+click range)
  // Auto-pause-on-selection state: selPausedPolling is true when WE paused
  // polling because a selection started (so we know to resume when it clears).
  // selWasPaused is true when the USER manually paused — we don't auto-resume
  // in that case (their manual choice takes precedence).
  var selPausedPolling=false, selWasPaused=false;

  // ── Theme persistence ────────────────────────────────────────────────
  function applyStoredTheme(){
    var t = localStorage.getItem("s2l-theme");
    if(t === "light") document.documentElement.setAttribute("data-theme","light");
  }
  applyStoredTheme();
  // ── localStorage persistence: remember the user's sort column/direction
  // and refresh rate between browser sessions so they don't have to
  // re-configure every time they open the viewer. Filter text is NOT
  // persisted (a stale filter would hide data on reload — confusing).
  (function applyStoredPrefs(){
    var sk = localStorage.getItem("s2l-sort-key");
    var sd = localStorage.getItem("s2l-sort-dir");
    if(sk && ["idx","host","port","health","url"].indexOf(sk) !== -1) sortKey = sk;
    if(sd === "asc" || sd === "desc") sortDir = sd;
    var rr = localStorage.getItem("s2l-refresh");
    if(rr){ var n = parseInt(rr, 10); if(!isNaN(n) && n >= 0) refreshMs = n; }
  })();
  function persistSort(){ localStorage.setItem("s2l-sort-key", sortKey); localStorage.setItem("s2l-sort-dir", sortDir); }
  function persistRefresh(){ localStorage.setItem("s2l-refresh", String(refreshMs)); }

  // ── Deep-linking: ?host=<hostname> pre-filters the CDN table on load.
  // Lets the capture browser's "back to viewer" link return to a filtered
  // view, and lets users bookmark/share a focused view of one CDN host.
  (function(){
    var m = location.search.match(/[?&]host=([^&]+)/);
    if(m){
      filterText = decodeURIComponent(m[1]).trim();
      // Defer setting the input value until the filter element exists (it's
      // created below in the same IIFE). We'll apply it after DOM init.
      setTimeout(function(){
        var el = document.getElementById("filter");
        if(el){ el.value = filterText; }
      }, 0);
    }
  })();

  function $(id){return document.getElementById(id);}
  function setText(el,v){ if(el.textContent!==v) el.textContent=v; }
  function fmtUptime(s){
    if(s<60) return s+"s";
    if(s<3600) return Math.floor(s/60)+"m "+(s%60)+"s";
    if(s<86400) return Math.floor(s/3600)+"h "+Math.floor((s%3600)/60)+"m";
    return Math.floor(s/86400)+"d "+Math.floor((s%86400)/3600)+"h";
  }
  function fmtAgo(s){
    if(s<2) return "now";
    if(s<60) return Math.floor(s)+"s ago";
    if(s<3600) return Math.floor(s/60)+"m ago";
    return Math.floor(s/3600)+"h ago";
  }
  // "updated Xs ago" indicator in the footer. Green when fresh (<3s), amber
  // when getting stale (<15s), red when clearly lagging or never polled.
  // Ticked every 1s so it stays accurate even while polling is paused (then
  // it visibly drifts to red, which is itself useful signal).
  function renderUpdated(){
    var el=$("ft-updated"); if(!el) return;
    if(!lastPollOk){ el.textContent="Updated —"; el.className="ft-updated dead"; return; }
    var s=(Date.now()-lastPollOk)/1000;
    el.textContent="Updated "+fmtAgo(s);
    el.className = s<3 ? "ft-updated" : s<15 ? "ft-updated stale" : "ft-updated dead";
  }
  function showToast(msg, kind){
    var t=$("toast"); t.textContent=msg; t.className="toast"+(kind?" "+kind:""); t.classList.add("show");
    clearTimeout(t._tid); t._tid=setTimeout(function(){t.classList.remove("show");},1800);
  }
  function pulse(state){
    var p=$("pulse"); p.className="pulse"+(state==="stale"?" stale":state==="dead"?" dead":"");
  }

  // ── Health cell per host ─────────────────────────────────────────────
  // Health cell: a STABLE structure (built once per row, never recreated).
  // Only the dot's class + the text spans are touched on each refresh — the
  // innerHTML is never rebuilt per tick, so nothing inside it can be yanked
  // out from under an in-progress mouse event. Sparkline removed (kept the
  // dot + status + error badge + sub-line, which is all that's needed).
  function buildHealthCell(){
    var wrap=document.createElement("div");
    wrap.className="health";
    wrap.setAttribute("role","img");
    wrap.innerHTML='<span class="hdot unknown" aria-hidden="true"></span>'
      + '<span class="hmeta"><span class="status">—</span>'
      + '<span class="errbadge" style="display:none"></span>'
      + '<span class="sub">no traffic yet</span></span></span>';
    return wrap;
  }
  function updateHealthCell(wrap, h){
    var dot=wrap.firstChild, meta=wrap.lastChild;
    var stEl=meta.children[0], eb=meta.children[1], sub=meta.children[2];
    if(!h){
      dot.className="hdot unknown";
      stEl.className="status"; stEl.textContent="—";
      eb.style.display="none";
      sub.textContent="no traffic yet";
      wrap.setAttribute("aria-label","host health: no traffic yet");
      return;
    }
    var st=h.last_status||0;
    var cls="ok", statusCls="";
    if(st===0||st>=500){ cls="bad"; statusCls="bad"; }
    else if(st>=400||h.errors>0){ cls="warn"; statusCls="warn"; }
    dot.className="hdot "+cls;
    stEl.className="status "+statusCls; stEl.textContent=String(st||"—");
    var ago=h.last_seen?fmtAgo(Date.now()/1000 - h.last_seen):"—";
    var rtt=(h.last_rtt_ms!=null)?h.last_rtt_ms+"ms":"—";
    var reqs=h.requests||0, errs=h.errors||0;
    sub.textContent=rtt+" · "+ago+" · "+reqs+" req";
    if(errs>0){
      var rate=reqs>0?(errs/reqs*100):100;
      var ecls=(rate>=10||st>=500)?"bad":"warn";
      eb.className="errbadge "+ecls;
      eb.textContent="!"+errs;
      eb.title=errs+" error(s) in "+reqs+" request(s) ("+rate.toFixed(1)+"%)";
      eb.style.display="";
    } else { eb.style.display="none"; }
    wrap.setAttribute("aria-label","host health: status "+(st||"unknown")+", "+rtt+" RTT, "+reqs+" requests"+(errs?(", "+errs+" errors"):"")+", seen "+ago);
  }

  // ── Sorting + filtering ──────────────────────────────────────────────
  // Health "badness" rank for the sortable HEALTH column: higher = more
  // broken. asc = worst-first (surfaces broken CDNs to the top on the first
  // click), desc = best-first. Used by applySortFilter AND by the filtered
  // export so the two stay consistent.
  function healthBadness(h){
    if(!h) return 0.5;                  // no health record at all = mild unknown
    var st=h.last_status||0, errs=h.errors||0;
    var base = 0;                        // 2xx/3xx = healthy baseline
    if(st===0) base = 1;                 // unknown / no traffic yet
    else if(st>=500) base = 5;           // server error
    else if(st>=400) base = 3;           // client error
    return base + Math.min(errs, 50)*0.02; // errors nudge worse, capped
  }
  function applySortFilter(active){
    var f=filterText.toLowerCase();
    var brokenMode = f === "__broken__";
    var filtered;
    if(!f){ filtered = active.slice(); }
    else if(brokenMode){ filtered = active.filter(isBrokenHost); }
    else { filtered = active.filter(function(h){
      return h.host.toLowerCase().indexOf(f)!==-1 || String(h.port).indexOf(f)!==-1 || h.url.toLowerCase().indexOf(f)!==-1;
    }); }
    filtered.sort(function(a,b){
      var av, bv;
      if(sortKey==="idx"){ av=active.indexOf(a)+1; bv=active.indexOf(b)+1; }
      else if(sortKey==="port"){ av=a.port; bv=b.port; }
      else if(sortKey==="health"){ av=healthBadness(a.health); bv=healthBadness(b.health); }
      else { av=a[sortKey]; bv=b[sortKey]; }
      var r = av<bv?-1:av>bv?1:0;
      // For HEALTH, invert the direction so the FIRST click (asc) surfaces the
      // WORST CDNs to the top — that's the useful default (find what's broken).
      // Second click (desc) → best-first. Other columns keep normal asc/desc.
      if(sortKey==="health"){ return sortDir==="asc"? -r : r; }
      return sortDir==="asc"? r : -r;
    });
    return filtered;
  }

  function renderFlags(flags){
    var el=$("flags"), order=["CAPTURE","CAPTURE_WS","HOOK_GUI","MULTIPORT","PROXY_CDN","CACHE_CDN",
                              "OFFLINE","PREMAP_CDN","SSE_PROXY","TCP_TUNNEL","UDP_TUNNEL","WS_DEFLATE","WS_AUTO_RECONNECT",
                              "WEB_CDNVIEWER"];
    var html=[];
    for(var i=0;i<order.length;i++){
      var k=order[i], on=!!flags[k];
      html.push('<span class="chip '+(on?"on":"")+'" title="'+k+' = '+(on?"on":"off")+'">'+k.replace(/_/g," ")+'</span>');
    }
    el.innerHTML=html.join("");
  }

  // Apply the current selection set to the visible <tr> nodes (add/remove
  // the .selected class). Called after a selection change AND at the end of
  // every render() so the class sticks across the 50 ms refresh (the <tr>
  // node is stable, so toggling its class is cheap and non-destructive).
  function applySelectionToRows(){
    for(var host in rowMap){
      var tr=rowMap[host].tr;
      tr.classList.toggle("selected", selectedHosts.has(host));
    }
  }
  // Update the header "sel" stat + the "Copy N"/"Open N" buttons + the
  // selection-group separator visibility based on the current selection
  // size. All hidden when nothing is selected.
  function updateSelStat(){
    var n=selectedHosts.size;
    var ssel=$("s-selected"), sselParent=ssel?ssel.parentNode:null;
    var csel=$("copy-sel-btn"), cslabel=$("copy-sel-label");
    var osel=$("open-sel-btn"), oslabel=$("open-sel-label");
    var sep=document.querySelector(".tb-sel-grp");
    if(ssel) setText(ssel, String(n));
    if(sselParent) sselParent.style.display = n>0 ? "" : "none";
    if(csel) csel.style.display = n>0 ? "" : "none";
    if(cslabel) cslabel.textContent = "Copy "+n;
    if(osel) osel.style.display = n>0 ? "" : "none";
    if(oslabel) oslabel.textContent = "Open "+n;
    if(sep) sep.style.display = n>0 ? "" : "none";
    // Auto-pause-on-selection (candidate m): when the user starts a selection,
    // pause polling so the table is stable for inspection. When the selection
    // is cleared (by Esc or by clicking away), resume if the user hadn't
    // manually paused. Track the user's manual pause state separately so we
    // don't clobber it.
    if(n>0 && !paused && !selWasPaused){
      selPausedPolling = true;
      doPause(true, "selection");
    } else if(n===0 && selPausedPolling){
      selPausedPolling = false;
      doPause(false, "");
    }
  }

  function render(d){
    lastData=d;
    $("rate").textContent = (d.refresh_ms||DEFAULT_REFRESH)+"ms";
    setText($("target"), d.target||"");
    setText($("proxy"), d.proxy||"");
    setText($("uptime"), "up "+fmtUptime(d.uptime_s||0));
    setText($("s-active"), String(d.active.length));
    setText($("s-pending"), String(d.pending||0));
    setText($("s-shared"), String(d.shared||0));
    setText($("s-proxied"), String(d.proxied||0));
    setText($("s-cdn"), String(d.cdn_fetched||0));
    setText($("s-cap"), String(d.captured||0));
    setText($("s-wscap"), String(d.ws_captured||0));
    // Broken stat: count hosts with 4xx/5xx status or errors. Red-tinted
    // when >0 so a degraded CDN set is visible at a glance in the header.
    var brokenCount = d.active.filter(isBrokenHost).length;
    var bstat=$("s-broken").parentNode;
    setText($("s-broken"), String(brokenCount));
    bstat.classList.toggle("has-broken", brokenCount>0);
    bstat.classList.toggle("active", filterText==="__broken__");
    setText($("latency"), ($("latency")._last||"—")+" ms");
    if(d.flags) renderFlags(d.flags);
    var sorted = applySortFilter(d.active);
    var seen=Object.create(null);
    var prevTr=null;
    // Smart-merge render (the click-bug fix):
    //  · existing <tr> are reused as-is — buttons/links are created ONCE on
    //    first appearance and never recreated or detached;
    //  · a row is moved only with insertBefore when it's actually out of
    //    position (minimal DOM mutation), never by re-appending a fragment
    //    that detaches every row each tick — that detach/reattach at 50 ms
    //    was what killed in-progress clicks;
    //  · the health cell is a stable structure updated via textContent /
    //    className, so its innerHTML is never rebuilt per tick either.
    for(var i=0;i<sorted.length;i++){
      var h=sorted[i], key=h.host;
      seen[key]=true;
      var r=rowMap[key];
      if(!r){
        var tr=document.createElement("tr");
        var cN=document.createElement("td"), cH=document.createElement("td"),
            cP=document.createElement("td"), cHl=document.createElement("td"), cU=document.createElement("td");
        cH.className="host"; cP.className="port";
        var a=document.createElement("a"); a.target="_blank";
        var cb=document.createElement("button"); cb.className="copy-btn"; cb.textContent="copy";
        (function(url,btn){btn.onclick=function(ev){ev.preventDefault();ev.stopPropagation();
          navigator.clipboard && navigator.clipboard.writeText(url);
          btn.closest("tr").classList.add("copy-flash");
          setTimeout(function(){btn.closest("tr").classList.remove("copy-flash");},800);
          showToast("Copied "+url);};})(h.url,cb);
        // "cURL" button: copies a ready-to-paste `curl` command for this CDN
        // host's REAL upstream URL (https://<host>/), not the local proxy URL.
        // This lets a developer debug the upstream CDN directly from a
        // terminal, bypassing the proxy.
        var curlBtn=document.createElement("button"); curlBtn.className="copy-btn curl-btn"; curlBtn.textContent="cURL";
        (function(host,url,btn){btn.onclick=function(ev){ev.preventDefault();ev.stopPropagation();
          var realUrl='https://'+host+'/';
          var cmd='curl -sS -D - "'+realUrl+'" \\\n  -H "User-Agent: Mozilla/5.0 (site2local)" \\\n  -H "Accept: */*"';
          if(navigator.clipboard && navigator.clipboard.writeText){
            navigator.clipboard.writeText(cmd).then(function(){
              btn.closest("tr").classList.add("copy-flash");
              setTimeout(function(){btn.closest("tr").classList.remove("copy-flash");},800);
              showToast("Copied cURL command for "+host);
            }, function(){ showToast("Clipboard write failed","warn"); });
          } else {
            var ta=document.createElement("textarea"); ta.value=cmd; document.body.appendChild(ta);
            ta.select(); try{document.execCommand("copy"); showToast("Copied cURL for "+host);}catch(e){showToast("Copy failed","warn");}
            ta.remove();
          }
        };})(h.host,h.url,curlBtn);
        cU.appendChild(a); cU.appendChild(cb); cU.appendChild(curlBtn);
        var hc=buildHealthCell();
        cHl.appendChild(hc);
        tr.appendChild(cN); tr.appendChild(cH); tr.appendChild(cP); tr.appendChild(cHl); tr.appendChild(cU);
        tr.className="new";
        // Row click (on the non-button area) toggles selection for bulk
        // actions. Shift+click selects a range from the last anchor row.
        // Button/link clicks inside the URL cell stop propagation so they
        // don't trigger selection. Bound ONCE at row creation.
        (function(hostKey){
          tr.addEventListener("click", function(ev){
            // Ignore clicks on the URL cell's interactive children (the <a>,
            // copy, cURL buttons) — they have their own handlers + stopPropagation.
            if(ev.target.closest("a, button")) return;
            var sortedHosts = lastData ? applySortFilter(lastData.active).map(function(x){return x.host;}) : [];
            if(ev.shiftKey && selAnchorHost && sortedHosts.indexOf(selAnchorHost)!==-1){
              // Range select from selAnchorHost to hostKey (inclusive).
              var i0=sortedHosts.indexOf(selAnchorHost), i1=sortedHosts.indexOf(hostKey);
              if(i0===-1||i1===-1) return;
              var lo=Math.min(i0,i1), hi=Math.max(i0,i1);
              for(var k=lo;k<=hi;k++){ selectedHosts.add(sortedHosts[k]); }
            } else if(ev.ctrlKey || ev.metaKey){
              // Ctrl/Cmd+click = additive toggle (keep existing selection).
              if(selectedHosts.has(hostKey)) selectedHosts.delete(hostKey);
              else selectedHosts.add(hostKey);
              selAnchorHost=hostKey;
            } else {
              // Plain click = exclusive toggle (clear others).
              if(selectedHosts.has(hostKey) && selectedHosts.size===1){ selectedHosts.clear(); }
              else { selectedHosts.clear(); selectedHosts.add(hostKey); }
              selAnchorHost=hostKey;
            }
            applySelectionToRows();
            updateSelStat();
          });
        })(key);
        rowMap[key]={tr:tr, cN:cN, cH:cH, cP:cP, cHl:hc, cU:a};
        r=rowMap[key];
      }
      // Update cell text in place — never recreate the row or its buttons.
      setText(r.cN, String(i+1));
      r.cN.className="idx";
      setText(r.cH, h.host);
      setText(r.cP, ":"+h.port);
      updateHealthCell(r.cHl, h.health);
      if(r.cU.textContent!==h.url){ r.cU.textContent=h.url; r.cU.href=h.url; r.cU.title=h.url; }
      // Position with minimal DOM mutation: insertBefore only when the row
      // isn't already in the right slot. This is what keeps a click on a
      // copy/cURL button from being cancelled by the next 50 ms refresh tick.
      var tr=r.tr;
      if(prevTr===null){
        if(rowsEl.firstChild!==tr){ rowsEl.insertBefore(tr, rowsEl.firstChild); }
      } else {
        if(prevTr.nextSibling!==tr){ rowsEl.insertBefore(tr, prevTr.nextSibling); }
      }
      prevTr=tr;
    }
    // Row lifecycle: distinguish "host gone from the data" (delete + remove
    // its <tr>) from "host filtered out by the current filter/sort" (just
    // hide its <tr>, keep it in rowMap so toggling the filter back restores
    // the SAME node — buttons, hover state, flash animations all survive).
    // This is what makes the Broken filter toggle non-destructive.
    var activeHosts=Object.create(null);
    d.active.forEach(function(h){ activeHosts[h.host]=true; });
    var visibleCount=0;
    for(var k in rowMap){
      var rr=rowMap[k];
      if(!activeHosts[k]){                   // host left the data entirely
        rr.tr.remove(); delete rowMap[k];
      } else if(!seen[k]){                  // host exists but filtered out → hide
        rr.tr.style.display="none";
      } else {                              // host is visible
        if(rr.tr.style.display==="none") rr.tr.style.display=""; visibleCount++;
      }
    }
    setText($("filter-count"), filterText ? visibleCount+"/"+d.active.length : "");
    // Filter-aware empty state: when a filter narrows the table to 0 rows,
    // show a centered "no match" message with a Clear Filter button instead
    // of the default "No CDNs yet" message (which is misleading — the data
    // exists, the filter just doesn't match). Prevents the black-void "dead
    // end" the VLM flagged.
    var hasData = d.active.length > 0;
    var filteredEmpty = hasData && visibleCount === 0 && !!filterText;
    var showEmpty = !hasData || filteredEmpty;
    empty.style.display = showEmpty ? "block" : "none";
    var ebig=$("empty-big"), esub=$("empty-sub"), eclear=$("empty-clear");
    if(filteredEmpty){
      if(ebig) ebig.textContent = "No hosts match \""+filterText+"\"";
      if(esub) esub.innerHTML = "The filter returned 0 of "+d.active.length+" CDN hosts.<br>Try a different search term, or clear the filter to see all hosts.";
      if(eclear) eclear.style.display = "";
    } else {
      if(ebig) ebig.textContent = "No MULTIPORT CDNs yet";
      if(esub) esub.innerHTML = "They appear here in real time as the proxy registers them.<br>Browse the target site to populate this table.";
      if(eclear) eclear.style.display = "none";
    }
    var ths=document.querySelectorAll("th.sortable");
    for(var j=0;j<ths.length;j++){
      var th=ths[j];
      th.classList.remove("sorted-asc","sorted-desc");
      if(th.getAttribute("data-sort")===sortKey){
        th.classList.add(sortDir==="asc"?"sorted-asc":"sorted-desc");
      }
    }
    // Re-apply the selection class to all rows so it survives the refresh
    // (the <tr> nodes are stable, so this just re-syncs the .selected class
    // in case a host's visibility changed).
    applySelectionToRows();
    updateSelStat();
  }

  function poll(){
    if(inFlight || paused || refreshMs===0){ schedule(); return; }
    inFlight=true;
    var t0=performance.now();
    var p = (window.__qaPoll ? window.__qaPoll() :
             fetch("/__s2l_viewer_data__",{cache:"no-store"}).then(function(r){if(!r.ok)throw new Error("HTTP "+r.status);return r.json();}));
    p.then(function(d){
      var lat=Math.round(performance.now()-t0);
      $("latency")._last=lat; $("latency").textContent=lat+" ms"; $("latency").className="tag ok";
      lastPollOk=Date.now(); render(d); renderUpdated(); consecutiveErr=0; pulse("ok");
      $("ft-right").textContent="OK"; $("ft-right").className="";
    }).catch(function(e){
      consecutiveErr++;
      $("ft-right").textContent=String(e); $("ft-right").className="err";
      if(consecutiveErr>3) pulse("dead"); else pulse("stale");
    }).then(function(){ inFlight=false; schedule(); });
  }
  function schedule(){ if(paused||refreshMs===0) return; setTimeout(poll, refreshMs); }

  // ── Controls ─────────────────────────────────────────────────────────
  $("rate-sel").addEventListener("change", function(e){
    var v=parseInt(e.target.value,10);
    if(v===0){ paused=true; $("pause-label").textContent="Resume"; $("pause-btn").classList.add("active"); refreshMs=DEFAULT_REFRESH; }
    else { paused=false; refreshMs=v; $("pause-label").textContent="Pause"; $("pause-btn").classList.remove("active"); schedule(); }
    persistRefresh();
  });
  // Restore the rate-sel dropdown's selected option from the persisted
  // refresh rate (the variable was set at init, but the <select> element
  // still shows the default 50ms option unless we sync it here).
  (function syncRateSel(){
    var rs=$("rate-sel"); if(!rs) return;
    var val = paused ? "0" : String(refreshMs);
    for(var i=0;i<rs.options.length;i++){ if(rs.options[i].value===val){ rs.selectedIndex=i; break; } }
  })();
  // Centralized pause/resume. `reason` is "" for manual, "selection" for
  // auto-pause. Tracks selWasPaused so the auto-pause logic doesn't fight a
  // manual pause: if the user manually paused, we never auto-resume.
  function doPause(state, reason){
    if(reason==="") selWasPaused = state;   // manual toggle updates the flag
    paused = state;
    $("pause-label").textContent = paused ? "Resume" : "Pause";
    var pb=$("pause-btn"); if(pb) pb.classList.toggle("active", paused);
    var rs=$("rate-sel"); if(rs) rs.value = paused ? "0" : String(refreshMs);
    if(!paused) schedule();
  }
  $("pause-btn").addEventListener("click", function(){
    doPause(!paused, "");
    showToast(paused ? "Polling paused" : "Polling resumed");
  });
  $("theme-btn").addEventListener("click", function(){
    var cur=document.documentElement.getAttribute("data-theme");
    var next = cur==="light" ? "" : "light";
    if(next) document.documentElement.setAttribute("data-theme",next);
    else document.documentElement.removeAttribute("data-theme");
    localStorage.setItem("s2l-theme", next||"dark");
    showToast((next==="light"?"Light":"Dark")+" theme");
  });

  $("open-all-btn").addEventListener("click", function(){
    if(!lastData || !lastData.active.length){ showToast("No CDN hosts to open","warn"); return; }
    if(lastData.active.length>5 && !confirm("Open "+lastData.active.length+" CDN URLs in new tabs?")) return;
    lastData.active.forEach(function(h){
      window.open(h.url, "_blank", "noopener");
    });
    showToast("Opened "+lastData.active.length+" CDN URLs");
  });
  // "Broken" button: filter the table to CDN hosts whose last status was 4xx
  // or 5xx (or that have errors), and sort them worst-first. Toggling again
  // clears it. The sentinel "__broken__" is recognized by applySortFilter so
  // it doesn't collide with a normal free-text host search.
  function isBrokenHost(h){
    var st=(h.health&&h.health.last_status)||0;
    var errs=(h.health&&h.health.errors)||0;
    return st>=400 || errs>0;
  }
  // Toggle the Broken filter (on = show only 4xx/5xx hosts, worst-first;
  // off = clear). Shared by the toolbar Broken button AND the header "broken"
  // stat (which acts as a clickable shortcut to the same filter).
  function toggleBrokenFilter(){
    if(!lastData || !lastData.active.length){ showToast("No CDN hosts","warn"); return; }
    var isOn = filterText==="__broken__";
    if(isOn){
      filterText=""; $("filter").value="";
      var bb=$("broken-btn"); if(bb) bb.classList.remove("active");
      if(lastData) render(lastData);
      showToast("Cleared broken filter");
      return;
    }
    var broken = lastData.active.filter(isBrokenHost);
    if(!broken.length){ showToast("No broken CDNs — all healthy","warn"); return; }
    filterText="__broken__";
    var bb2=$("broken-btn"); if(bb2) bb2.classList.add("active");
    sortKey="health"; sortDir="asc";           // worst-first (see applySortFilter)
    if(lastData) render(lastData);
    showToast("Showing "+broken.length+" broken CDN"+(broken.length===1?"":"s")+" · worst-first");
  }
  $("broken-btn").addEventListener("click", toggleBrokenFilter);
  // Clicking the header "broken" stat also toggles the filter — a stat that
  // doubles as an action is a natural triage entry point.
  var bstatEl=$("s-broken");
  if(bstatEl) bstatEl.parentNode.addEventListener("click", toggleBrokenFilter);
  // Copy CDN URLs to clipboard. When a filter is active (free-text OR the
  // Broken filter), copies ONLY the visible/filtered URLs — that's the set
  // the user is looking at, so it's almost always what they want. With no
  // filter, copies all (same as before).
  function copyURLs(hosts, label){
    if(!hosts || !hosts.length){ showToast("No CDN URLs to copy","warn"); return; }
    var urls = hosts.map(function(h){ return h.url; }).join("\n");
    if(navigator.clipboard && navigator.clipboard.writeText){
      navigator.clipboard.writeText(urls).then(function(){
        showToast("Copied "+hosts.length+" CDN URL"+(hosts.length===1?"":"s")+label);
      }, function(){ showToast("Clipboard write failed","warn"); });
    } else {
      var ta=document.createElement("textarea"); ta.value=urls; document.body.appendChild(ta);
      ta.select(); try{document.execCommand("copy"); showToast("Copied "+hosts.length+" CDN URL"+(hosts.length===1?"":"s")+label);}catch(e){showToast("Copy failed","warn");}
      ta.remove();
    }
  }
  $("copy-all-btn").addEventListener("click", function(){
    if(!lastData || !lastData.active.length){ showToast("No CDN hosts to copy","warn"); return; }
    // Visible = applySortFilter result (respects free-text + Broken filter).
    var visible = applySortFilter(lastData.active);
    var label = (filterText && filterText!=="__broken__") ? " · filtered"
              : filterText==="__broken__" ? " · broken" : "";
    copyURLs(visible, label);
  });
  // Copy ONLY the selected CDN URLs (the bulk-action target). Order matches
  // the current sort so the pasted list reads top-to-bottom like the screen.
  $("copy-sel-btn").addEventListener("click", function(){
    if(!lastData){ showToast("No data yet"); return; }
    if(!selectedHosts.size){ showToast("No rows selected — click a row to select","warn"); return; }
    // Preserve current sort order in the copied list.
    var ordered = applySortFilter(lastData.active).filter(function(h){ return selectedHosts.has(h.host); });
    copyURLs(ordered, " · selected");
  });
  // Open ONLY the selected CDN URLs in new tabs. Same ordering as copy-sel.
  // Confirms before opening >5 to avoid a tab explosion.
  $("open-sel-btn").addEventListener("click", function(){
    if(!lastData){ showToast("No data yet"); return; }
    if(!selectedHosts.size){ showToast("No rows selected — click a row to select","warn"); return; }
    var ordered = applySortFilter(lastData.active).filter(function(h){ return selectedHosts.has(h.host); });
    if(ordered.length>5 && !confirm("Open "+ordered.length+" CDN URLs in new tabs?")) return;
    ordered.forEach(function(h){ window.open(h.url, "_blank", "noopener"); });
    showToast("Opened "+ordered.length+" CDN URL"+(ordered.length===1?"":"s"));
  });
  // Select all visible rows (Ctrl+A). "Visible" = the current filtered+sorted
  // set, so it respects the Broken filter and free-text search. Ctrl+A is
  // normally "select all page text" — preventDefault so we own it here.
  function selectAllVisible(){
    if(!lastData || !lastData.active.length){ return; }
    var visible = applySortFilter(lastData.active);
    visible.forEach(function(h){ selectedHosts.add(h.host); });
    applySelectionToRows();
    updateSelStat();
    showToast("Selected "+visible.length+" visible CDN"+(visible.length===1?"":"s"));
  }
  $("filter").addEventListener("input", function(e){ filterText=e.target.value.trim();
    // Typing into the filter takes over from the Broken button — clear its
    // active state so the two filter modes don't fight each other.
    var bb=$("broken-btn"); if(bb) bb.classList.remove("active");
    if(lastData) render(lastData); });
  // "Clear filter" button in the empty state — clears the free-text filter
  // (or the Broken filter) and re-renders so the user escapes the dead-end.
  var emptyClearBtn=$("empty-clear");
  if(emptyClearBtn) emptyClearBtn.onclick=function(){
    filterText=""; $("filter").value="";
    var bb=$("broken-btn"); if(bb) bb.classList.remove("active");
    if(lastData) render(lastData);
    showToast("Filter cleared");
  };
  var ths=document.querySelectorAll("th.sortable");
  for(var i=0;i<ths.length;i++){
    ths[i].addEventListener("click", function(){
      var key=this.getAttribute("data-sort");
      if(sortKey===key){ sortDir = sortDir==="asc"?"desc":"asc"; }
      else { sortKey=key; sortDir="asc"; }
      persistSort();
      if(lastData) render(lastData);
    });
  }
  // Reset stats: POST to /__s2l_reset_stats__ (zeroes counters + event log).
  $("reset-btn").addEventListener("click", function(){
    if(!confirm("Reset all counters to zero? (for a clean benchmark window)")) return;
    fetch("/__s2l_reset_stats__",{method:"POST",cache:"no-store"})
      .then(function(r){return r.json();})
      .then(function(d){
        showToast("Stats reset — counters zeroed");
        // Force an immediate poll so the UI reflects the reset.
        if(lastData){ lastData.captured=0; lastData.ws_captured=0; lastData.proxied=0; lastData.cdn_fetched=0; lastData.req_per_s=0; render(lastData); }
      })
      .catch(function(e){ showToast("Reset failed: "+e,"warn"); });
  });
  // Help overlay (? key) — discoverable keyboard shortcut reference.
  var helpOverlay=document.getElementById("help-overlay");
  function toggleHelp(){ helpOverlay.classList.toggle("open"); }
  document.getElementById("help-close").onclick=function(){ helpOverlay.classList.remove("open"); };
  helpOverlay.addEventListener("click", function(e){ if(e.target===this) helpOverlay.classList.remove("open"); });
  document.addEventListener("keydown", function(e){
    if(e.target.tagName==="INPUT") return;
    if(e.key==="?" || (e.shiftKey && e.key==="/")){ e.preventDefault(); toggleHelp(); return; }
    if(e.key==="Escape"){
      helpOverlay.classList.remove("open");
      // Esc clears things in priority order: first an active filter (free-text
      // or Broken), then any row selection, then it just closes the overlay.
      var hadFilter=!!filterText;
      var hadSelection=selectedHosts.size>0;
      // Esc clears one thing at a time (priority: filter → selection → close).
      // This gives the user granular control — they can clear the filter
      // without losing their row selection, then press Esc again to clear
      // the selection.
      if(hadFilter){
        filterText=""; $("filter").value=""; var bb=$("broken-btn"); if(bb) bb.classList.remove("active");
        if(lastData) render(lastData);
        showToast("Filter cleared");
      } else if(hadSelection){
        selectedHosts.clear(); selAnchorHost=null;
        if(lastData) render(lastData);
        showToast("Selection cleared");
      }
      return;
    }
    if(e.key==="p"||e.key==="P"){ $("pause-btn").click(); }
    else if(e.key==="t"||e.key==="T"){ $("theme-btn").click(); }
    else if(e.key==="o"||e.key==="O"){ $("open-all-btn").click(); }
    else if(e.key==="r"||e.key==="R"){ $("reset-btn").click(); }
    else if(e.key==="h"||e.key==="H"){
      // Sort by health: first press = worst-first, again = flip direction.
      var th=document.querySelector('th[data-sort="health"]');
      if(th) th.click();
    }
    else if(e.key==="b"||e.key==="B"){
      // Shift+B = "Select all broken": activate the Broken filter AND select
      // all the broken hosts in one step (so you can immediately bulk
      // copy/open them). Plain B just toggles the filter (no selection).
      if(e.shiftKey && lastData){
        var broken = lastData.active.filter(isBrokenHost);
        if(!broken.length){ showToast("No broken CDNs — all healthy","warn"); return; }
        // Activate the Broken filter first (so the selected rows are visible).
        if(filterText !== "__broken__") toggleBrokenFilter();
        // Then select all visible (broken) hosts.
        selectedHosts.clear();
        broken.forEach(function(h){ selectedHosts.add(h.host); });
        applySelectionToRows();
        updateSelStat();
        showToast("Selected "+broken.length+" broken CDN"+(broken.length===1?"":"s"));
      } else {
        $("broken-btn").click();
      }
    }
    else if(e.key==="s"||e.key==="S"){
      // Copy selected (bulk-action shortcut). If nothing is selected, no-op
      // silently — pressing S by accident shouldn't toast a warning.
      if(selectedHosts.size) $("copy-sel-btn").click();
    }
    else if(e.shiftKey && (e.key==="a"||e.key==="A")){ $("copy-all-btn").click(); }
    else if(e.shiftKey && (e.key==="o"||e.key==="O")){
      // Shift+O = open selected (mirrors O = open all). No-op if nothing
      // is selected (pressing by accident shouldn't toast a warning).
      if(selectedHosts.size) $("open-sel-btn").click();
    }
    else if(e.ctrlKey || e.metaKey){
      // Ctrl/Cmd+A = select all visible rows (prevents the browser's
      // "select all page text" behavior). Only triggers on A.
      if(e.key==="a"||e.key==="A"){ e.preventDefault(); selectAllVisible(); }
    }
    else if(e.key==="/"){ e.preventDefault(); $("filter").focus(); }
  });

  poll();
  // Tick the "updated Xs ago" footer indicator every second so it stays
  // accurate even while polling is paused (then it drifts to red — useful
  // signal that the live view is frozen). Cheap: only touches one span.
  setInterval(renderUpdated, 1000);
  // ── Auto-pause on text selection ───────────────────────────────────
  // When the user selects text in the table (or anywhere in the page),
  // pause polling so the selection doesn't get blown away by the 50 ms
  // refresh. Resume when the selection is cleared. Uses the
  // 'selectionchange' event which fires on any selection change.
  var _selectionPauseTimer = null;
  document.addEventListener("selectionchange", function(){
    var sel = window.getSelection ? window.getSelection().toString() : "";
    if(sel && sel.length > 0){
      // User has an active text selection — pause if not already paused.
      if(!paused && !selWasPaused && !selPausedPolling){
        selPausedPolling = true;
        doPause(true, "selection");
      }
      // Reset the resume timer — keep paused while selection is active.
      clearTimeout(_selectionPauseTimer);
    } else {
      // Selection cleared — resume after a short delay (so a quick
      // click-drag-release doesn't cause a flicker).
      clearTimeout(_selectionPauseTimer);
      _selectionPauseTimer = setTimeout(function(){
        if(selPausedPolling && !selWasPaused){
          selPausedPolling = false;
          doPause(false, "");
        }
      }, 400);
    }
  });
})();
</script>
</body>
</html>"""
