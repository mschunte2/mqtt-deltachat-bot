'use strict';

// One outbound message kind from the bot: a `snapshot` payload that
// bundles everything we need to render. We cache the latest in
// localStorage so a reload paints last-known state instantly, then
// fire a refresh request. Window changes are render-only — no fetch.

const STORAGE_KEY = 'latestSnapshot';
const WINDOW_KEY = 'windowSeconds';

const state = {
  active: null,
  serverTs: 0,
  // devices: { name: { fields, last_update_ts, energy, daily_energy_wh,
  //                    scheduled_jobs, params, power_history: {minute, hour} } }
  devices: {},
  windowSeconds: 86400,
};

// Restore last-chosen window from localStorage.
try {
  const saved = localStorage.getItem(WINDOW_KEY);
  if (saved !== null) {
    const n = parseInt(saved, 10);
    if (Number.isFinite(n) && n >= 3600) state.windowSeconds = n;
  }
} catch (_) { /* localStorage may be disabled; ignore */ }

// Hydrate from cached snapshot if present, so the app renders before
// any refresh roundtrip lands.
try {
  const cached = localStorage.getItem(STORAGE_KEY);
  if (cached) {
    const obj = JSON.parse(cached);
    if (obj && obj.devices) {
      state.devices = obj.devices;
      state.serverTs = obj.server_ts || 0;
    }
  }
} catch (_) { /* corrupt cache: ignore */ }

const $ = (id) => document.getElementById(id);
const picker = $('device-picker');
const onlineDot = $('online-dot');
const lastUpdate = $('last-update');
const stateText = $('state-text');
const statePower = $('state-power');
const stateEnergy = $('state-energy');
const sparkline = $('sparkline');
const dailyBars = $('daily-bars');
const dailyFoot = $('daily-foot');
const chartMax = $('chart-max');
const chartFoot = $('chart-foot');
const windowPick = $('window-pick');
const offCount = $('off-count');
const onCount = $('on-count');
const offRulesList = $('off-rules-list');
const onRulesList = $('on-rules-list');

function activeDevice() { return state.devices[state.active] || null; }

function send(req) {
  if (!state.active && req.action !== 'refresh') return;
  if (state.active) req.device = state.active;
  req.ts = Math.floor(Date.now() / 1000);
  window.webxdc.sendUpdate({ payload: { request: req } }, '');
}

$('btn-on').addEventListener('click', () => send({ action: 'on' }));
$('btn-off').addEventListener('click', () => send({ action: 'off' }));
$('btn-toggle').addEventListener('click', () => send({ action: 'toggle' }));
$('btn-refresh').addEventListener('click', () => sendRefresh());

function sendRefresh() {
  // Refresh is class-scoped (not device-scoped); the bot resolves the
  // class from the requesting msgid.
  window.webxdc.sendUpdate({
    payload: { request: { action: 'refresh', ts: Math.floor(Date.now() / 1000) } }
  }, '');
}

// Add-rule buttons.
function readRuleForm(direction) {
  const root = document.querySelector(`.rule-form[data-action="${direction}"]`);
  if (!root) return null;
  const checked = root.querySelector(`input[name="${direction}-mode"]:checked`);
  if (!checked) return null;
  const mode = checked.value;
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
});

windowPick.addEventListener('change', () => {
  state.windowSeconds = parseInt(windowPick.value, 10) || 86400;
  try { localStorage.setItem(WINDOW_KEY, String(state.windowSeconds)); }
  catch (_) { /* ignore */ }
  renderSparkline();
});

if (windowPick) {
  const opt = Array.from(windowPick.options)
    .find(o => o.value === String(state.windowSeconds));
  if (opt) windowPick.value = String(state.windowSeconds);
}

// Click delegation for per-rule delete buttons.
document.addEventListener('click', (e) => {
  const btn = e.target.closest('.delete-btn');
  if (!btn) return;
  const direction = btn.dataset.action;
  const rid = btn.dataset.ruleId;
  if (!rid) return;
  const cancel_action = direction === 'off' ? 'cancel-auto-off' : 'cancel-auto-on';
  send({ action: cancel_action, rule_id: rid });
});

// --- Render -------------------------------------------------------------

function render() {
  const names = Object.keys(state.devices).sort();
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
  stateText.textContent =
    typeof f.output === 'boolean' ? (f.output ? 'ON' : 'OFF') : '?';
  statePower.textContent =
    typeof f.apower === 'number' ? `${f.apower.toFixed(0)} W` : '— W';
  stateEnergy.textContent =
    typeof f.aenergy === 'number' ? `(${(f.aenergy / 1000).toFixed(2)} kWh)` : '';

  renderSparkline();
  renderDailyBars(dev);
  renderEnergySummary(dev);
  renderRulesList(dev);
  renderAge();
}

function renderAge() {
  if (!state.serverTs) { lastUpdate.textContent = '—'; return; }
  const age = Math.max(0, Math.floor(Date.now() / 1000) - state.serverTs);
  if (age < 60) lastUpdate.textContent = `${age}s ago`;
  else if (age < 3600) lastUpdate.textContent = `${Math.round(age / 60)}min ago`;
  else lastUpdate.textContent = `${Math.round(age / 3600)}h ago`;
}

function renderSparkline() {
  const dev = activeDevice();
  if (!dev || !dev.power_history) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data yet)';
    return;
  }
  // Pick resolution based on window. ≤24h → minute, 31d → hour.
  const useHour = state.windowSeconds >= 7 * 86400;
  const series = useHour ? dev.power_history.hour : dev.power_history.minute;
  if (!Array.isArray(series) || series.length < 2) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data in this window)';
    return;
  }
  const now = Math.floor(Date.now() / 1000);
  const xMax = now;
  const xMin = now - state.windowSeconds;
  // Slice series to the window.
  const pts = series.filter(p => p[0] >= xMin && p[0] <= xMax);
  if (pts.length < 2) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data in this window)';
    return;
  }
  const tSpan = Math.max(1, xMax - xMin);
  const pMax = Math.max(1, ...pts.map(p => p[1]));
  const W = 200, H = 60;
  const yOff = H - 2;

  const onSegs = [], offSegs = [], offlineSegs = [];
  for (let i = 0; i < pts.length - 1; i++) {
    const [t1, w1, o1] = pts[i];
    const [t2] = pts[i + 1];
    const x1 = ((t1 - xMin) / tSpan) * W;
    const x2 = ((t2 - xMin) / tSpan) * W;
    if (o1 === null) {
      offlineSegs.push(`M${x1.toFixed(1)},${yOff} L${x2.toFixed(1)},${yOff}`);
    } else if (o1 === 0) {
      offSegs.push(`M${x1.toFixed(1)},${yOff} L${x2.toFixed(1)},${yOff}`);
    } else {
      const [, w2] = pts[i + 1];
      const y1 = H - (w1 / pMax) * (H - 6) - 3;
      const y2 = H - (w2 / pMax) * (H - 6) - 3;
      onSegs.push(`M${x1.toFixed(1)},${y1.toFixed(1)} L${x2.toFixed(1)},${y2.toFixed(1)}`);
    }
  }
  let svg = '';
  if (offlineSegs.length) {
    svg += `<path fill="none" stroke="#8e8e93" stroke-width="2"
                  stroke-linecap="round" d="${offlineSegs.join(' ')}"/>`;
  }
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
  // Total kWh from energy summary if available, else integrate from points.
  const e = dev.energy;
  let kwh = null;
  if (e) {
    const map = {
      3600: 'kwh_last_hour', 86400: 'kwh_last_24h',
      [7 * 86400]: 'kwh_last_7d', [30 * 86400]: 'kwh_last_30d',
    };
    const key = map[state.windowSeconds];
    if (key && e[key] && typeof e[key].kwh === 'number') kwh = e[key].kwh;
  }
  const tail = kwh !== null ? ` · ${kwh.toFixed(2)} kWh in window` : '';
  chartFoot.textContent = `${pts.length} pts${tail}`;
}

function renderDailyBars(dev) {
  if (!dailyBars) return;
  const days = dev.daily_energy_wh;
  if (!Array.isArray(days) || days.length < 2) {
    dailyBars.innerHTML = '';
    if (dailyFoot) dailyFoot.textContent = '';
    return;
  }
  const W = 200, H = 36;
  const maxWh = Math.max(1, ...days.map(d => d[1]));
  const w = W / days.length;
  let totalWh = 0;
  const rects = days.map(([_ts, wh], i) => {
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
  if (entry == null) return '—';
  if (typeof entry === 'number') return fmtKwh(entry);
  const text = fmtKwh(entry.kwh);
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

function describeRule(j) {
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

function fmtSecs(s) {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  const h = Math.floor(s / 3600);
  const m = Math.round((s % 3600) / 60);
  return m ? `${h}h${m}m` : `${h}h`;
}

// --- Inbound -----------------------------------------------------------

window.webxdc.setUpdateListener((update) => {
  const p = update.payload;
  if (!p || !p.snapshot) return;
  const snap = p.snapshot;
  state.serverTs = snap.server_ts || Math.floor(Date.now() / 1000);
  state.devices = snap.devices || {};
  // Persist for fast hydration on reload.
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      server_ts: state.serverTs, devices: state.devices,
    }));
  } catch (_) { /* ignore quota / disabled */ }
  render();
}, 0);

// On first paint render whatever we hydrated from localStorage, then
// fire one refresh request to pull fresh data from the bot.
render();
sendRefresh();

// Live age + countdown updates.
setInterval(() => {
  renderAge();
  const dev = activeDevice();
  if (dev) renderRulesList(dev);
}, 1000);
