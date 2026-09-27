// tempdes live view. The server renders every number once; this script keeps the page in
// step with the simulation: it applies each frame from the event stream to the elements the
// server tagged (data-bind, data-bar, data-heat, data-vbar, data-svgw, data-width,
// data-show, data-alarm), draws the charts and the shard map, and posts control changes.
// Structure that changes (pods, hotspots, tables) is re-rendered by the server as fragments.
(() => {
  'use strict';

  // --- formatting, mirrored from src/ui/views.rs -------------------------------------------
  const F = {
    pct: x => ((x >= 0.995 && x < 1) ? (x * 100).toFixed(1) : (x * 100).toFixed(0)) + '%',
    rate: x => x === 0 ? '0' : x >= 10000 ? (x / 1000).toFixed(1) + 'k/s' : x >= 100 ? x.toFixed(0) + '/s' : x >= 1 ? x.toFixed(1) + '/s' : x.toFixed(3) + '/s',
    ms: v => {
      const us = v * 1000;
      if (!isFinite(us)) return '∞';
      if (us < 1) return Math.round(us * 1000) + 'ns';
      if (us < 1000) return us.toFixed(0) + 'µs';
      if (us < 1e6) { const ms = us / 1000; return (ms < 10 ? ms.toFixed(2) : ms < 100 ? ms.toFixed(1) : ms.toFixed(0)) + 'ms'; }
      if (us < 60e6) return (us / 1e6).toFixed(2) + 's';
      if (us < 3600e6) return (us / 60e6).toFixed(1) + 'm';
      return (us / 3600e6).toFixed(1) + 'h';
    },
    int: x => x.toFixed(0),
    secs: x => x.toFixed(1) + ' s',
    mult: x => '×' + x.toFixed(2),
    num: x => x.toFixed(1),
    speed: x => x <= 0 ? 'max' : x.toFixed(1) + '×',
    text: x => String(x),
    key: x => { const s = String(x); const i = s.indexOf('.'); return (i < 0 ? s : s.slice(i + 1)).replace(/namespace/g, 'ns'); },
  };
  const heat = u => u < 0.05 ? 0 : u < 0.30 ? 1 : u < 0.55 ? 2 : u < 0.75 ? 3 : u < 0.90 ? 4 : 5;
  const clamp01 = v => Math.max(0, Math.min(1, Number(v) || 0));

  function get(o, path) {
    for (const s of path.split('.')) {
      if (o == null) return undefined;
      o = o[s];
    }
    return o == null ? undefined : o;
  }
  function fmt(v, f) {
    if (v === undefined) return '–';
    if (f === 'text' || f === 'key') return F[f](v);
    return (F[f] || String)(Number(v));
  }

  // --- state -----------------------------------------------------------------------------------
  let frame = null;
  let lastAnalysis = -1;
  let lastRun = -1;
  let owners = null;
  let dragging = false;
  const series = [];
  const events = [];
  const $ = id => document.getElementById(id);

  // --- binding ---------------------------------------------------------------------------------
  function bind(root, f) {
    root.querySelectorAll('[data-bind]').forEach(el => {
      const [p, fm] = el.dataset.bind.split('|');
      let s = fmt(get(f, p), fm);
      if (s === '' && el.dataset.empty) s = el.dataset.empty;
      if (el.textContent !== s) el.textContent = s;
    });
    root.querySelectorAll('[data-bar]').forEach(el => {
      el.style.width = (clamp01(get(f, el.dataset.bar)) * 100).toFixed(1) + '%';
    });
    root.querySelectorAll('[data-heat]').forEach(el => {
      el.dataset.level = heat(clamp01(get(f, el.dataset.heat)));
    });
    root.querySelectorAll('[data-vbar]').forEach(el => {
      const u = clamp01(get(f, el.dataset.vbar));
      const h = +el.dataset.h, base = +el.dataset.base;
      el.setAttribute('height', (h * u).toFixed(1));
      el.setAttribute('y', (base - h * u).toFixed(1));
    });
    root.querySelectorAll('[data-svgw]').forEach(el => {
      el.setAttribute('width', (+el.dataset.w * clamp01(get(f, el.dataset.svgw))).toFixed(1));
    });
    const vmax = Math.max(1, ...f.flows.map(x => x.per_s));
    root.querySelectorAll('[data-width]').forEach(el => {
      const v = Number(get(f, el.dataset.width)) || 0;
      el.setAttribute('stroke-width', (1 + 5 * Math.sqrt(v / vmax)).toFixed(2));
    });
    root.querySelectorAll('[data-show]').forEach(el => {
      const v = get(f, el.dataset.show);
      const on = typeof v === 'number' ? v > 0 : !!v;
      if (el instanceof SVGElement) el.setAttribute('visibility', on ? 'visible' : 'hidden');
      else el.hidden = !on;
    });
    root.querySelectorAll('[data-alarm]').forEach(el => {
      el.dataset.on = (Number(get(f, el.dataset.alarm)) || 0) > 0 ? 'true' : 'false';
    });
  }

  async function refresh(id, url) {
    try {
      const r = await fetch(url);
      if (!r.ok) return;
      const html = await r.text();
      const el = $(id);
      if (!el) return;
      el.innerHTML = html;
      if (frame) bind(el, frame);
    } catch (e) { /* the next frame retries */ }
  }

  // --- charts ----------------------------------------------------------------------------------
  const point = f => ({
    t: f.t, offered: f.workload.offered_per_s, started: f.workload.started_per_s,
    completed: f.workload.completed_per_s, cpu: f.services.map(s => s.cpu_max),
    db: f.persistence.util, rejected: f.rejected_per_s, backlog: f.matching.backlog,
    pending: f.history.pending_tasks, wft_s2s_p99: f.latency.wft_schedule_to_start.p99_ms,
    e2e_p99: f.latency.e2e.p99_ms, start_p99: f.latency.start.p99_ms,
    lock_wait_p99: f.history.lock_wait.p99_ms, shard_io_max: f.history.shard_io_max,
    sync_match: f.matching.sync_match_ratio, running: f.workload.running, load: f.load_scale,
  });
  const CHARTS = {
    throughput: { series: [p => p.completed, p => p.offered], min: 1, fmt: 'rate' },
    cpu: { series: [p => p.cpu[1], p => p.cpu[0], p => p.cpu[2]], min: 1, cap: 1, fmt: 'pct' },
    db: { series: [p => p.db], min: 1, cap: 1, fmt: 'pct' },
    rejected: { series: [p => p.rejected], min: 1, fmt: 'rate' },
    backlog: { series: [p => p.backlog], min: 10, fmt: 'int' },
    wft: { series: [p => p.wft_s2s_p99], min: 10, fmt: 'ms' },
    e2e: { series: [p => p.e2e_p99], min: 100, fmt: 'ms' },
    start: { series: [p => p.start_p99], min: 10, fmt: 'ms' },
  };
  const WINDOW = 180;
  function drawCharts(f) {
    if (series.length < 2) return;
    const t1 = series[series.length - 1].t, t0 = Math.max(0, t1 - WINDOW);
    const pts = series.filter(p => p.t >= t0);
    const W = 320, H = 90, L = 34, R = 6, T = 6, B = 14;
    const sx = t => L + (t - t0) / Math.max(1e-9, t1 - t0) * (W - L - R);
    const marks = events.filter(e => e.t >= t0 && e.t <= t1);
    document.querySelectorAll('.chart').forEach(fig => {
      const c = CHARTS[fig.dataset.chart];
      const svg = fig.querySelector('svg');
      if (!c || !svg) return;
      let ymax = c.min;
      for (const s of c.series) for (const p of pts) ymax = Math.max(ymax, s(p));
      if (c.cap) ymax = Math.min(ymax, c.cap);
      const sy = v => T + (1 - Math.min(v, ymax) / ymax) * (H - T - B);
      let out = '';
      if (f.warmup_s > t0) {
        out += `<rect class="warm" x="${sx(t0).toFixed(1)}" y="${T}" width="${(sx(Math.min(f.warmup_s, t1)) - sx(t0)).toFixed(1)}" height="${H - T - B}"></rect>`;
      }
      for (const k of [0, 0.5, 1]) {
        const y = sy(ymax * k).toFixed(1);
        out += `<line class="grid" x1="${L}" x2="${W - R}" y1="${y}" y2="${y}"></line>`;
        out += `<text class="axis" x="${L - 3}" y="${(+y + 3).toFixed(1)}" text-anchor="end">${fmt(ymax * k, c.fmt)}</text>`;
      }
      for (const e of marks) {
        const x = sx(e.t).toFixed(1);
        out += `<line class="mark" x1="${x}" x2="${x}" y1="${T}" y2="${H - B}"><title>${esc(e.text)}</title></line>`;
      }
      c.series.forEach((s, i) => {
        let d = '';
        pts.forEach((p, j) => { d += (j ? 'L' : 'M') + sx(p.t).toFixed(1) + ',' + sy(s(p)).toFixed(1); });
        if (i === 0) {
          out += `<path class="area" d="${d}L${sx(t1).toFixed(1)},${sy(0).toFixed(1)}L${sx(pts[0].t).toFixed(1)},${sy(0).toFixed(1)}Z"></path>`;
        }
        out += `<path class="s${i}" d="${d}"></path>`;
      });
      out += `<text class="axis" x="${L}" y="${H - 3}">${t0.toFixed(0)}s</text><text class="axis" x="${W - R}" y="${H - 3}" text-anchor="end">${t1.toFixed(0)}s</text>`;
      svg.innerHTML = out;
    });
  }
  const esc = s => String(s).replace(/[&<>"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]));

  // --- shard map -------------------------------------------------------------------------------
  function drawShards(f) {
    const canvas = $('shards');
    if (!canvas) return;
    if (f.shard_owners) owners = f.shard_owners;
    const heatv = f.shard_heat;
    const n = heatv.length;
    if (!n) return;
    const css = getComputedStyle(document.documentElement);
    const colors = [0, 1, 2, 3, 4, 5].map(i => css.getPropertyValue('--h' + i).trim());
    const ink = css.getPropertyValue('--muted').trim();
    const cssW = canvas.clientWidth || 1000;
    const groups = [];
    if (owners && owners.owner.length === n) {
      owners.pods.forEach((name, i) => groups.push({ name, ids: [] }));
      owners.owner.forEach((o, i) => groups[o].ids.push(i));
    } else {
      groups.push({ name: 'shards', ids: heatv.map((_, i) => i) });
    }
    const s = n <= 512 ? 8 : n <= 2048 ? 5 : 3, gap = 1, labelW = 130, rowGap = 8;
    const cols = Math.max(1, Math.floor((cssW - labelW) / (s + gap)));
    let height = 4;
    for (const g of groups) height += Math.max(1, Math.ceil(g.ids.length / cols)) * (s + gap) + rowGap;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(cssW * dpr);
    canvas.height = Math.round(height * dpr);
    canvas.style.height = height + 'px';
    const cx = canvas.getContext('2d');
    cx.scale(dpr, dpr);
    cx.font = '11px ' + css.getPropertyValue('--mono');
    cx.textBaseline = 'top';
    let y = 2;
    for (const g of groups) {
      cx.fillStyle = ink;
      cx.fillText(g.name + ' · ' + g.ids.length, 0, y);
      g.ids.forEach((id, k) => {
        const level = heat(heatv[id] / 200);
        cx.fillStyle = colors[level];
        cx.fillRect(labelW + (k % cols) * (s + gap), y + Math.floor(k / cols) * (s + gap), s, s);
      });
      y += Math.max(1, Math.ceil(g.ids.length / cols)) * (s + gap) + rowGap;
    }
  }

  // --- frames ----------------------------------------------------------------------------------
  function addEvents(list) {
    const ul = $('events');
    if (!ul) return;
    for (const e of list) {
      const li = document.createElement('li');
      const t = document.createElement('span');
      t.className = 'mono muted';
      t.textContent = e.t.toFixed(1) + 's';
      li.append(t, ' ' + e.text);
      ul.prepend(li);
    }
    while (ul.children.length > 60) ul.lastChild.remove();
  }

  function onFrame(f) {
    frame = f;
    if (f.run !== lastRun) {
      if (lastRun !== -1) {
        series.length = 0;
        events.length = 0;
        const ul = $('events');
        if (ul) ul.innerHTML = '';
        owners = null;
      }
      lastRun = f.run;
    }
    bind(document, f);
    const sig = f.pods.filter(p => p.alive).map(p => p.name).join(',');
    const topo = $('topology');
    if (topo && topo.dataset.sig !== sig) {
      topo.dataset.sig = sig;
      refresh('topology', '/fragment/topology');
    }
    if (f.analysis.seq !== lastAnalysis) {
      lastAnalysis = f.analysis.seq;
      refresh('hotspots', '/fragment/hotspots');
      refresh('detail', '/fragment/detail');
    }
    const phase = $('phase');
    if (phase) {
      phase.dataset.phase = f.paused ? 'paused' : f.phase;
      phase.textContent = f.paused ? 'paused' : f.phase === 'warmup' ? 'warming up' : 'measuring';
    }
    const pause = $('btn-pause');
    if (pause) {
      pause.dataset.paused = f.paused ? 'true' : 'false';
      pause.textContent = f.paused ? 'Resume' : 'Pause';
    }
    const speed = $('speed');
    if (speed && document.activeElement !== speed) speed.value = String(f.speed);
    const load = $('load');
    if (load && !dragging) {
      load.value = f.load_scale.toFixed(2);
      $('load-out').textContent = '×' + f.load_scale.toFixed(2);
    }
    series.push(point(f));
    while (series.length > 800) series.shift();
    if (f.events.length) {
      events.push(...f.events);
      while (events.length > 200) events.shift();
      addEvents(f.events);
    }
    drawCharts(f);
    drawShards(f);
  }

  // --- controls --------------------------------------------------------------------------------
  async function control(body) {
    try {
      const r = await fetch('/api/control', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) console.warn('control rejected:', await r.text());
    } catch (e) {
      console.warn('control failed:', e);
    }
  }
  function wire() {
    $('btn-pause')?.addEventListener('click', () => control({ action: frame && frame.paused ? 'resume' : 'pause' }));
    $('speed')?.addEventListener('change', e => control({ action: 'speed', value: Number(e.target.value) }));
    $('btn-restart')?.addEventListener('click', () => control({ action: 'restart' }));
    $('btn-reseed')?.addEventListener('click', () => control({ action: 'restart', seed: Math.floor(Math.random() * 1e9) }));
    const load = $('load');
    if (load) {
      load.addEventListener('pointerdown', () => { dragging = true; });
      load.addEventListener('input', () => { $('load-out').textContent = '×' + Number(load.value).toFixed(2); });
      load.addEventListener('change', () => { dragging = false; control({ action: 'load', value: Number(load.value) }); });
    }
    document.querySelectorAll('[data-scale]').forEach(btn => btn.addEventListener('click', () => {
      if (!frame) return;
      const svc = btn.dataset.scale;
      const idx = ['frontend', 'history', 'matching', 'worker'].indexOf(svc);
      const current = frame.services[idx].replicas;
      control({ action: 'replicas', service: svc, value: current + Number(btn.dataset.delta) });
    }));
    const key = $('dc-key'), val = $('dc-value');
    if (key && val) {
      key.addEventListener('change', () => { val.value = key.selectedOptions[0].dataset.current; });
      $('dc-apply')?.addEventListener('click', () => control({ action: 'dc', key: key.value, value: Number(val.value) }));
    }
  }

  // --- start -----------------------------------------------------------------------------------
  async function start() {
    wire();
    try {
      const r = await fetch('/api/history');
      if (r.ok) series.push(...await r.json());
    } catch (e) { /* charts fill from frames */ }
    const es = new EventSource('/events');
    es.addEventListener('frame', e => onFrame(JSON.parse(e.data)));
    es.addEventListener('error', () => {
      const phase = $('phase');
      if (phase) { phase.dataset.phase = 'offline'; phase.textContent = 'reconnecting'; }
    });
    window.addEventListener('resize', () => { if (frame) { drawShards(frame); } });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
