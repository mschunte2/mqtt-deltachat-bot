'use strict';

const SAMPLES_MAX_AGE = 300;
const SAMPLES_MAX_COUNT = 200;

const state = {
  devices: {},
  active: null,
  history: {},          // device -> [{ts, power}] live ring buffer (last 5 min, client-side)
  historyWindow: 86400, // selected window in seconds; 0 = live
  serverHistory: {},    // device -> { window_seconds, bucket_seconds, power_points, energy_points }
};

// Restore the last-chosen time window from localStorage (per-device UI
// preference; doesn't sync to other chat members, which is the right
// thing for a per-user view setting).
try {
  const saved = localStorage.getItem('windowSeconds');
  if (saved !== null) {
    const n = parseInt(saved, 10);
    if (Number.isFinite(n) && n >= 0) state.historyWindow = n;
  }
} catch (_e) { /* localStorage may be disabled; ignore */ }

const $ = (id) => document.getElementById(id);
const picker = $('device-picker');
const onlineDot = $('online-dot');
const stateText = $('state-text');
const statePower = $('state-power');
const stateEnergy = $('state-energy');
const sparkline = $('sparkline');
const dailyBars = $('daily-bars');
const dailyFoot = $('daily-foot');
const chartMax = $('chart-max');
const chartFoot = $('chart-foot');
const windowPick = $('window-pick');
const lastUpdate = $('last-update');
const eventsList = $('events-list');
const tuneWatts = $('tune-watts');
const tuneSecs = $('tune-secs');
const tuneApply = $('tune-apply');
const tuneSave = $('tune-save');
const tuneStatus = $('tune-status');
const offCount = $('off-count');
const onCount = $('on-count');
const offRulesList = $('off-rules-list');
const onRulesList = $('on-rules-list');

function activeDevice() { return state.devices[state.active] || null; }

function send(req) {
  if (!state.active) return;
  req.device = state.active;
  req.ts = Math.floor(Date.now() / 1000);
  window.webxdc.sendUpdate({ payload: { request: req } }, '');
}

$('btn-on').addEventListener('click', () => send({ action: 'on' }));
$('btn-off').addEventListener('click', () => send({ action: 'off' }));
$('btn-toggle').addEventListener('click', () => send({ action: 'toggle' }));

// "Add rule" buttons: one per direction.
function readRuleForm(direction) {
  // direction is 'off' or 'on'. Returns the auto_off / auto_on payload.
  const root = document.querySelector(`.rule-form[data-action="${direction}"]`);
  if (!root) return null;
  const mode = root.querySelector(`input[name="${direction}-mode"]:checked`).value;
  const policy = {};
  if (mode === 'timer') {
    const mins = parseInt(root.querySelector(`.${direction}-timer-min`).value, 10) || 0;
    policy.timer_seconds = mins * 60;
  } else if (mode === 'tod') {
    const v = root.querySelector(`.${direction}-tod`).value || '22:00';
    const [h, m] = v.split(':').map(n => parseInt(n, 10));
    policy.time_of_day = [h, m];
    policy.recurring_tod = root.querySelector(`.${direction}-tod-daily`).checked;
  } else if (mode === 'idle') {
    policy.idle = {
      threshold: parseFloat(root.querySelector(`.${direction}-idle-w`).value),
      duration_s: parseInt(root.querySelector(`.${direction}-idle-s`).value, 10),
    };
  } else if (mode === 'consumed') {
    policy.consumed = {
      threshold_wh: parseFloat(root.querySelector(`.${direction}-cons-wh`).value),
      window_s: parseInt(root.querySelector(`.${direction}-cons-min`).value, 10) * 60,
    };
  }
  const onceBox = root.querySelector(`.${direction}-once`);
  if (onceBox && onceBox.checked) policy.once = true;
  return policy;
}

document.querySelectorAll('.add-rule-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const direction = btn.dataset.action;
    const policy = readRuleForm(direction);
    if (!policy) return;
    const verb = direction === 'off' ? 'auto-off' : 'auto-on';
    const key  = direction === 'off' ? 'auto_off' : 'auto_on';
    send({ action: verb, [key]: policy });
  });
});

picker.addEventListener('change', () => {
  state.active = picker.value;
  render();
  if (state.historyWindow > 0) requestHistory();
});

windowPick.addEventListener('change', () => {
  state.historyWindow = parseInt(windowPick.value, 10) || 0;
  try { localStorage.setItem('windowSeconds', String(state.historyWindow)); }
  catch (_e) { /* ignore */ }
  if (state.historyWindow > 0) requestHistory();
  renderSparkline();
});

// Pre-select the restored window in the <select> element BEFORE any
// snapshot arrives, so the picker shows the persisted choice.
if (windowPick) {
  const opt = Array.from(windowPick.options)
    .find(o => o.value === String(state.historyWindow));
  if (opt) windowPick.value = String(state.historyWindow);
}

function requestHistory() {
  if (!state.active || state.historyWindow <= 0) return;
  window.webxdc.sendUpdate({
    payload: {
      request: {
        device: state.active,
        action: 'history',
        window_seconds: state.historyWindow,
        ts: Math.floor(Date.now() / 1000),
      }
    }
  }, '');
}

function requestEvents() {
  if (!state.active) return;
  window.webxdc.sendUpdate({
    payload: {
      request: {
        device: state.active,
        action: 'events',
        window_seconds: 7 * 86400,
        limit: 50,
        ts: Math.floor(Date.now() / 1000),
      }
    }
  }, '');
}

const recentEventsEl = document.querySelector('details.recent-events');
if (recentEventsEl) {
  recentEventsEl.addEventListener('toggle', () => {
    if (recentEventsEl.open) requestEvents();
  });
}

function _sendTune(persist) {
  if (!state.active) return;
  const w = parseFloat(tuneWatts.value);
  const s = parseInt(tuneSecs.value, 10);
  if (!isFinite(w) || !isFinite(s)) return;
  for (const [param, value] of [
    ['power_threshold_watts', w],
    ['power_threshold_duration_s', s],
  ]) {
    window.webxdc.sendUpdate({
      payload: {
        request: {
          device: state.active, action: 'set_param',
          param, value, persist: persist || undefined,
          ts: Math.floor(Date.now() / 1000),
        }
      }
    }, '');
  }
  if (tuneStatus) {
    tuneStatus.textContent = persist
      ? 'Saved to devices.json (will survive bot restart).'
      : 'Applied (in-memory; lost on bot restart).';
    setTimeout(() => { if (tuneStatus) tuneStatus.textContent = ''; }, 4000);
  }
}

if (tuneApply) tuneApply.addEventListener('click', () => _sendTune(false));
if (tuneSave)  tuneSave.addEventListener('click',  () => _sendTune(true));

function render() {
  const names = Object.keys(state.devices).sort();
  // Re-populate picker only when set changes
  const current = Array.from(picker.options).map(o => o.value);
  if (current.join() !== names.join()) {
    picker.innerHTML = names.map(n => `<option value="${n}">${n}</option>`).join('');
  }
  if (!state.active && names.length) state.active = names[0];
  if (state.active) picker.value = state.active;

  const dev = activeDevice();
  if (!dev) {
    stateText.textContent = '—';
    statePower.textContent = '— W';
    stateEnergy.textContent = '';
    onlineDot.textContent = '⚪';
    return;
  }
  const f = dev.fields || {};
  onlineDot.textContent =
    f.online === true ? '🟢' : f.online === false ? '🔴' : '⚪';
  if (typeof f.output === 'boolean') {
    stateText.textContent = f.output ? 'ON' : 'OFF';
  } else {
    stateText.textContent = '?';
  }
  statePower.textContent =
    typeof f.apower === 'number' ? `${f.apower.toFixed(0)} W` : '— W';
  stateEnergy.textContent =
    typeof f.aenergy === 'number' ? `(${(f.aenergy / 1000).toFixed(2)} kWh)` : '';

  if (dev.last_update_ts) {
    const d = new Date(dev.last_update_ts * 1000);
    lastUpdate.textContent = `last update: ${d.toLocaleTimeString()}`;
  }
  renderSparkline();
  renderRulesList(dev);
  renderEnergySummary(dev);
  renderTuningInputs(dev);
  renderDailyBars(dev);
}

function renderDailyBars(dev) {
  if (!dailyBars) return;
  const days = dev.daily_energy_wh;
  if (!Array.isArray(days) || days.length < 2) {
    dailyBars.innerHTML = '';
    if (dailyFoot) dailyFoot.textContent = '';
    return;
  }
  // days is [[ts, wh], …] oldest first.
  const W = 200, H = 36;
  const maxWh = Math.max(1, ...days.map(d => d[1]));
  const w = W / days.length;
  let totalWh = 0;
  const rects = days.map(([ts, wh], i) => {
    totalWh += wh;
    const h = (wh / maxWh) * (H - 2);
    return `<rect x="${(i * w).toFixed(2)}" y="${(H - h).toFixed(2)}" `
         + `width="${(w * 0.85).toFixed(2)}" height="${h.toFixed(2)}" `
         + `fill="#5ac8fa"/>`;
  }).join('');
  dailyBars.innerHTML = rects + `<text x="${W - 2}" y="10" font-size="9" `
    + `text-anchor="end" fill="#888">peak ${maxWh.toFixed(0)} Wh</text>`;
  if (dailyFoot) {
    dailyFoot.textContent = `30-day total: ${(totalWh / 1000).toFixed(2)} kWh`;
  }
}

function fmtKwh(kwh) {
  if (kwh == null) return '—';
  if (kwh < 0.01) return `${(kwh * 1000).toFixed(1)} Wh`;
  return `${kwh.toFixed(2)} kWh`;
}

function fmtIntervalEntry(entry) {
  // entry shape: {kwh, partial_since_ts | null} or legacy bare number.
  if (entry == null) return '—';
  if (typeof entry === 'number') return fmtKwh(entry);
  const text = fmtKwh(entry.kwh);
  // Compact: a star indicates "data starts later than the window";
  // verbose since-date suppressed by request.
  return entry.partial_since_ts ? text + '*' : text;
}

function renderEnergySummary(dev) {
  const e = dev.energy;
  const set = (id, val) => { const el = $(id); if (el) el.textContent = val; };
  if (!e) {
    ['kwh-last-hour','kwh-today','kwh-last-24h','kwh-this-week',
     'kwh-last-7d','kwh-this-month','kwh-last-30d','kwh-total']
      .forEach(id => set(id, '—'));
    return;
  }
  set('kwh-last-hour',  fmtIntervalEntry(e.kwh_last_hour));
  set('kwh-today',      fmtIntervalEntry(e.kwh_today));
  set('kwh-last-24h',   fmtIntervalEntry(e.kwh_last_24h));
  set('kwh-this-week',  fmtIntervalEntry(e.kwh_this_week));
  set('kwh-last-7d',    fmtIntervalEntry(e.kwh_last_7d));
  set('kwh-this-month', fmtIntervalEntry(e.kwh_this_month));
  set('kwh-last-30d',   fmtIntervalEntry(e.kwh_last_30d));
  set('kwh-total',
      e.current_total_wh != null ? fmtKwh(e.current_total_wh / 1000) : '—');
}

function renderTuningInputs(dev) {
  const params = dev.params || {};
  if (typeof params.power_threshold_watts === 'number'
      && document.activeElement !== tuneWatts) {
    tuneWatts.value = params.power_threshold_watts;
  }
  if (typeof params.power_threshold_duration_s === 'number'
      && document.activeElement !== tuneSecs) {
    tuneSecs.value = params.power_threshold_duration_s;
  }
}

function renderEvents(rows) {
  if (!eventsList) return;
  if (!rows || !rows.length) {
    eventsList.textContent = '(no events recorded yet)';
    return;
  }
  eventsList.innerHTML = rows.slice(0, 50).map(r => {
    const d = new Date(r.ts * 1000);
    const stamp = d.toLocaleString();
    const kind = r.kind || '(unknown)';
    return `<div class="ev"><span class="ts">${stamp}</span>`
         + `<span class="kind">${kind}</span></div>`;
  }).join('');
}

function renderSparkline() {
  // Live mode: client-side ring buffer (last 5 min).
  // Window mode: server-pushed history (1h/6h/12h/24h/31d).
  // The x-axis is anchored to the REQUESTED window, not the data span.
  // So if you ask for 31d and only have 1d of data, the line shows on
  // the right ~1/31 of the canvas — not stretched across.
  let pts;        // [[ts, w, out|null], ...]
  let footText = '';
  let xMin, xMax; // x-axis bounds in unix seconds (REQUESTED window)
  if (state.historyWindow === 0) {
    const hist = (state.history[state.active] || []).slice();
    pts = hist.map(s => [s.ts, s.power, s.out]);
    xMax = Math.floor(Date.now() / 1000);
    xMin = xMax - SAMPLES_MAX_AGE;
    footText = pts.length ? `${pts.length} samples (live)` : '(live, waiting…)';
  } else {
    const sh = state.serverHistory[state.active];
    if (!sh || !sh.power_points) {
      pts = [];
      footText = '(loading…)';
    } else {
      pts = sh.power_points;
      xMin = (typeof sh.since_ts === 'number') ? sh.since_ts
            : (pts.length ? pts[0][0] : Math.floor(Date.now() / 1000));
      xMax = (typeof sh.until_ts === 'number') ? sh.until_ts
            : (pts.length ? pts[pts.length - 1][0] : Math.floor(Date.now() / 1000));
      // Authoritative total from the bot's hybrid energy_consumed_in
      // (energy_minute first, power_minute fallback). Falls back to the
      // older energy_hour delta for old responses without total_wh.
      let totalWh;
      if (typeof sh.total_wh === 'number') {
        totalWh = sh.total_wh;
      } else {
        const e = sh.energy_points || [];
        totalWh = e.length >= 2 ? (e[e.length - 1][1] - e[0][1]) : 0;
      }
      const bucketLabel = fmtSecs(sh.bucket_seconds || 60);
      footText = pts.length
        ? `${pts.length} pts · bucket ${bucketLabel} · ${(totalWh / 1000).toFixed(2)} kWh in window`
        : '(no data in this window yet)';
    }
  }
  if (pts.length < 2) {
    sparkline.innerHTML = '';
    chartMax.textContent = '';
    chartFoot.textContent = footText;
    return;
  }
  // tSpan is the WINDOW span, not the data span. So sparse data renders
  // at its actual position, not stretched.
  const tMin = xMin;
  const tMax = xMax;
  const tSpan = Math.max(1, tMax - tMin);
  const pMax = Math.max(1, ...pts.map(p => p[1]));
  const W = 200, H = 60;
  const yOff = H - 2;  // baseline 0-line (for off segments)
  // Build two paths: green (on / unknown) tracing apower; red flat baseline
  // for off segments. Each segment connects pts[i] → pts[i+1] coloured by
  // pts[i].output.
  const onSegs = [];
  const offSegs = [];
  for (let i = 0; i < pts.length - 1; i++) {
    const [t1, w1, o1] = pts[i];
    const [t2, w2] = pts[i + 1];
    const x1 = ((t1 - tMin) / tSpan) * W;
    const x2 = ((t2 - tMin) / tSpan) * W;
    if (o1 === 0) {
      offSegs.push(`M${x1.toFixed(1)},${yOff} L${x2.toFixed(1)},${yOff}`);
    } else {
      const y1 = H - (w1 / pMax) * (H - 6) - 3;
      const y2 = H - (w2 / pMax) * (H - 6) - 3;
      onSegs.push(`M${x1.toFixed(1)},${y1.toFixed(1)} L${x2.toFixed(1)},${y2.toFixed(1)}`);
    }
  }
  let svg = '';
  if (offSegs.length) {
    svg += `<path fill="none" stroke="#ff3b30" stroke-width="2"
                  stroke-linecap="round" d="${offSegs.join(' ')}"/>`;
  }
  if (onSegs.length) {
    svg += `<path fill="none" stroke="#34c759" stroke-width="1.5"
                  stroke-linecap="round" d="${onSegs.join(' ')}"/>`;
  }
  sparkline.innerHTML = svg;
  chartMax.textContent = `max ${pMax.toFixed(0)} W`;
  chartFoot.textContent = footText;
}

function describeRule(j) {
  // Build a human description from a scheduled_jobs row.
  const parts = [];
  if (j.deadline_ts) {
    const remaining = Math.max(0, j.deadline_ts - Math.floor(Date.now() / 1000));
    if (j.time_of_day) {
      const [h, m] = j.time_of_day;
      const suffix = j.recurring_tod ? ' daily' : '';
      const hh = String(h).padStart(2, '0');
      const mm = String(m).padStart(2, '0');
      parts.push(`at ${hh}:${mm}${suffix} (in ${fmtSecs(remaining)})`);
    } else {
      parts.push(`in ${fmtSecs(remaining)}`);
    }
  }
  if (j.idle) {
    parts.push(`when ${j.idle.field || 'apower'} < ${j.idle.threshold}W `
             + `for ${fmtSecs(j.idle.duration_s)}`);
  }
  if (j.consumed) {
    parts.push(`when used < ${j.consumed.threshold_wh}Wh in `
             + `${fmtSecs(j.consumed.window_s)}`);
  }
  let s = parts.join(' or ') || '(empty)';
  if (j.once) s += ' · once';
  return s;
}

function renderRulesList(dev) {
  const jobs = dev.scheduled_jobs || [];
  const offJobs = jobs.filter(j => j.target_action === 'off');
  const onJobs  = jobs.filter(j => j.target_action === 'on');
  if (offCount) offCount.textContent = `(${offJobs.length})`;
  if (onCount)  onCount.textContent  = `(${onJobs.length})`;

  const renderInto = (ul, list, action) => {
    if (!ul) return;
    if (!list.length) {
      ul.innerHTML = '<li class="empty"><span class="rule-text" '
        + 'style="color:#8e8e93">no rules</span></li>';
      return;
    }
    ul.innerHTML = list.map(j => {
      const desc = describeRule(j);
      const rid  = j.rule_id || '';
      return `<li><span class="rule-text">${esc(desc)}</span>`
           + `<span class="rule-id">${esc(rid)}</span>`
           + `<button class="delete-btn" data-action="${action}" `
           + `data-rule-id="${esc(rid)}">×</button></li>`;
    }).join('');
  };
  renderInto(offRulesList, offJobs, 'off');
  renderInto(onRulesList,  onJobs,  'on');
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Click delegation for the per-rule delete buttons (since the lists
// are re-rendered on every snapshot, individual listeners would leak).
document.addEventListener('click', (e) => {
  const btn = e.target.closest('.delete-btn');
  if (!btn) return;
  const direction = btn.dataset.action;
  const rid = btn.dataset.ruleId;
  if (!rid) return;
  const cancel_action = direction === 'off' ? 'cancel-auto-off' : 'cancel-auto-on';
  send({ action: cancel_action, rule_id: rid });
});

function fmtSecs(s) {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  const h = Math.floor(s / 3600);
  const m = Math.round((s % 3600) / 60);
  return m ? `${h}h${m}m` : `${h}h`;
}

function appendSample(name, ts, power, output) {
  if (typeof power !== 'number') return;
  const h = (state.history[name] = state.history[name] || []);
  if (h.length && h[h.length - 1].ts === ts) return;  // dedup same-second pushes
  // output: true → 1, false → 0, anything else → null (unknown)
  const out = output === true ? 1 : output === false ? 0 : null;
  h.push({ ts, power, out });
  const cutoff = ts - SAMPLES_MAX_AGE;
  while (h.length && h[0].ts < cutoff) h.shift();
  if (h.length > SAMPLES_MAX_COUNT) h.splice(0, h.length - SAMPLES_MAX_COUNT);
}

window.webxdc.setUpdateListener((update) => {
  const p = update.payload;
  if (!p) return;
  // History response.
  if (p.history && p.history.device) {
    state.serverHistory[p.history.device] = p.history;
    renderSparkline();
    return;
  }
  // Events response.
  if (p.events && p.events.device) {
    if (p.events.device === state.active) renderEvents(p.events.rows);
    return;
  }
  // Regular snapshot.
  if (!p.devices) return;
  state.devices = p.devices;
  const ts = p.server_ts || Math.floor(Date.now() / 1000);
  for (const [name, dev] of Object.entries(p.devices)) {
    if (dev.fields && typeof dev.fields.apower === 'number') {
      appendSample(name, ts, dev.fields.apower, dev.fields.output);
    }
  }
  render();
  // First time we know which device is active → kick off history fetch.
  if (state.active && state.historyWindow > 0
      && !state.serverHistory[state.active]) {
    requestHistory();
  }
}, 0);

// keep countdowns live
setInterval(() => {
  const dev = activeDevice();
  if (dev) renderRulesList(dev);
}, 1000);
