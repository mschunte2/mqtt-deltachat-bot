'use strict';

// One outbound message kind from the bot: a `snapshot` payload that
// bundles everything we need to render. We cache the latest in
// localStorage so a reload paints last-known state instantly, then
// fire a refresh request. Window changes are render-only — no fetch.

const STORAGE_KEY = 'latestSnapshot';
const WINDOW_KEY = 'windowSeconds';
const ACTIVE_KEY = 'activeDevice';

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

// Restore last-active device. Validated against the snapshot in render().
try {
  const saved = localStorage.getItem(ACTIVE_KEY);
  if (saved) state.active = saved;
} catch (_) { /* ignore */ }

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
const deviceDesc = $('device-desc');
const onlineDot = $('online-dot');
const lastUpdate = $('last-update');
const stateIcon = $('state-icon');
const stateText = $('state-text');
const statePower = $('state-power');
const sparkline = $('sparkline');
const chartLabel = $('chart-label');
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

// Counter reset — confirms before firing, then sends the action.
// The bot stores baseline = current aenergy.total; the next snapshot
// will show kwh_since_reset ~= 0.
const btnReset = $('btn-reset');
if (btnReset) {
  btnReset.addEventListener('click', () => {
    if (!state.active) return;
    const ok = window.confirm(
      `Reset the Counter for "${state.active}"? ` +
      `(Lifetime stays unchanged. The bot stores the current ` +
      `lifetime as a baseline; Counter starts back at 0.)`
    );
    if (ok) send({ action: 'reset-counter' });
  });
}

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
    policy.timer_minutes = mins;
  } else if (mode === 'tod') {
    const v = root.querySelector(`.${direction}-tod`).value || '22:00';
    const [h, m] = v.split(':').map(n => parseInt(n, 10));
    policy.time_of_day = [h, m];
    policy.recurring_tod = root.querySelector(`.${direction}-tod-daily`).checked;
  } else if (mode === 'idle') {
    policy.idle = {
      threshold: parseFloat(root.querySelector(`.${direction}-idle-w`).value),
      duration_minutes: parseInt(root.querySelector(`.${direction}-idle-min`).value, 10),
    };
  } else if (mode === 'consumed') {
    policy.consumed = {
      threshold_wh: parseFloat(root.querySelector(`.${direction}-cons-wh`).value),
      window_minutes: parseInt(root.querySelector(`.${direction}-cons-min`).value, 10),
    };
  } else if (mode === 'avg') {
    policy.avg = {
      threshold_w: parseFloat(root.querySelector(`.${direction}-avg-w`).value),
      window_minutes: parseInt(root.querySelector(`.${direction}-avg-min`).value, 10),
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
  try { localStorage.setItem(ACTIVE_KEY, state.active); }
  catch (_) { /* ignore */ }
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
  // If the restored active device is no longer in the snapshot
  // (renamed, removed), fall back to the first.
  if (state.active && !names.includes(state.active)) state.active = null;
  if (!state.active && names.length) state.active = names[0];
  if (state.active) picker.value = state.active;

  const dev = activeDevice();
  deviceDesc.textContent = (dev && dev.description) || '';
  if (!dev) {
    stateText.textContent = '—';
    statePower.textContent = '— W';
    stateEnergy.textContent = '';
    onlineDot.textContent = '⚪';
    stateIcon.className = 'state-icon off';
    return;
  }
  const f = dev.fields || {};
  // Header dot: green=on, red=off, grey=offline-or-unknown.
  // (Was showing online/offline only — confusing because a plug can
  // be online AND off, which used to look identical to online AND on.)
  if (f.online === false) {
    onlineDot.textContent = '⚫';            // offline
  } else if (f.output === true) {
    onlineDot.textContent = '🟢';            // on
  } else if (f.output === false) {
    onlineDot.textContent = '🔴';            // off
  } else {
    onlineDot.textContent = '⚫';            // unknown / no data yet
  }
  // Big state text + bulb icon: explicitly say "offline" when LWT
  // reports the plug unreachable, instead of the stale ON/OFF that
  // the relay last claimed. Icon mirrors: bright bulb = ON,
  // dimmed = OFF, dimmed + red ✕ = offline / unknown.
  if (f.online === false) {
    stateText.textContent = 'offline';
    stateIcon.className = 'state-icon offline';
  } else if (f.output === true) {
    stateText.textContent = 'ON';
    stateIcon.className = 'state-icon';
  } else if (f.output === false) {
    stateText.textContent = 'OFF';
    stateIcon.className = 'state-icon off';
  } else {
    stateText.textContent = '?';
    stateIcon.className = 'state-icon offline';
  }
  statePower.textContent =
    typeof f.apower === 'number' ? `${f.apower.toFixed(0)} W` : '— W';

  renderSparkline();
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
  if (!dev) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data yet)';
    return;
  }
  // At long windows the per-bucket power chart conveys little; switch
  // to a daily-Wh bar chart (counter-diff per day, no trapezoid).
  if (state.windowSeconds >= 31 * 86400) {
    if (chartLabel) chartLabel.textContent = 'Energy';
    renderDailyEnergyChart(dev,
      Math.round(state.windowSeconds / 86400));
    return;
  }
  if (chartLabel) chartLabel.textContent = 'Power';
  if (!dev.power_history) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data yet)';
    return;
  }
  // Pick resolution based on window:
  //   ≤24h    → minute series (with live tail at "now")
  //   7d      → hour series
  // 31d / 365d branches handled above as daily energy bars.
  let series;
  if (state.windowSeconds >= 7 * 86400) {
    series = dev.power_history.hour;
  } else {
    series = dev.power_history.minute;
  }
  if (!Array.isArray(series) || series.length < 2) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data in this window)';
    return;
  }
  const now = Math.floor(Date.now() / 1000);
  const xMax = now;
  const xMin = now - state.windowSeconds;
  // Each point is [ts, min_w, max_w, avg_w, output] (new) or
  // [ts, max_w, avg_w, output] (legacy 4-tuple from older cached
  // payloads). Helpers normalise the access.
  const pAt = (p, key) => {
    if (p.length >= 5) {
      if (key === 'min') return p[1];
      if (key === 'max') return p[2];
      if (key === 'avg') return p[3];
      return p[4];                              // output
    }
    // legacy 4-tuple: no min, fall back to avg
    if (key === 'min') return p[2];
    if (key === 'max') return p[1];
    if (key === 'avg') return p[2];
    return p[3];                                // output
  };
  const pts = series.filter(p => p[0] >= xMin && p[0] <= xMax);
  if (pts.length < 2) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data in this window)';
    return;
  }
  // Unified rendering for all series (minute / hour / day):
  //   on bucket  → grey min..max bar + green avg dot
  //   off bucket → red dot at chart bottom
  //   offline    → no dot, no bar, breaks the connecting line
  // A thin grey line connects each consecutive pair of dots.
  const tSpan = Math.max(1, xMax - xMin);
  const pMax = Math.max(1, ...pts.map(p => pAt(p, 'max') ?? pAt(p, 'avg')));
  const W = 200, H = 60;
  const yOff = H - 2;
  const yOf = (w) => H - (w / pMax) * (H - 6) - 3;

  const dots = [];          // {x, y, fill}
  const bars = [];          // SVG path d for grey min..max ticks
  const linePts = [];       // [{x,y} or null] — null = offline gap
  for (const p of pts) {
    const o = pAt(p, 'output');
    const x = ((p[0] - xMin) / tSpan) * W;
    if (o === 1) {
      const minW = pAt(p, 'min') ?? pAt(p, 'avg');
      const maxW = pAt(p, 'max') ?? pAt(p, 'avg');
      const avgW = pAt(p, 'avg');
      bars.push(
        `M${x.toFixed(1)},${yOf(maxW).toFixed(1)} ` +
        `L${x.toFixed(1)},${yOf(minW).toFixed(1)}`
      );
      const y = yOf(avgW);
      dots.push({ x, y, fill: '#34c759' });    // green = on
      linePts.push({ x, y });
    } else if (o === 0) {
      const y = yOff;                          // chart bottom
      dots.push({ x, y, fill: '#ff3b30' });    // red = off whole bucket
      linePts.push({ x, y });
    } else {
      linePts.push(null);                      // offline gap
    }
  }

  let svg = '';
  // Connecting line (under bars/dots), broken across offline gaps.
  let path = '', pen = false;
  for (const lp of linePts) {
    if (lp === null) { pen = false; continue; }
    path += pen
      ? ` L${lp.x.toFixed(1)},${lp.y.toFixed(1)}`
      : `M${lp.x.toFixed(1)},${lp.y.toFixed(1)}`;
    pen = true;
  }
  if (path) {
    svg += `<path fill="none" stroke="#8e8e93" stroke-width="1" d="${path}"/>`;
  }
  if (bars.length) {
    svg += `<path fill="none" stroke="#8e8e93" stroke-width="1"
                  stroke-linecap="round" d="${bars.join(' ')}"/>`;
  }
  for (const d of dots) {
    svg += `<circle cx="${d.x.toFixed(1)}" cy="${d.y.toFixed(1)}" r="1" fill="${d.fill}"/>`;
  }
  sparkline.innerHTML = svg;
  // Header always shows both max and avg from the visible points,
  // computed from the underlying max / avg fields regardless of
  // rendering mode.
  const maxOfPts = Math.max(...pts.map(p => pAt(p, 'max') ?? pAt(p, 'avg')));
  const avgOfPts = pts.reduce((s, p) => s + pAt(p, 'avg'), 0) / pts.length;
  chartMax.textContent = `max ${maxOfPts.toFixed(0)} W · avg ${avgOfPts.toFixed(0)} W`;
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

function renderDailyEnergyChart(dev, days) {
  // Daily-Wh bar chart that replaces the power chart at long windows.
  // Reads `daily_energy_wh` (counter-diff per day; no trapezoid) and
  // renders the last `days` entries.
  const all = dev.daily_energy_wh;
  if (!Array.isArray(all) || all.length < 1) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no daily energy data yet)';
    return;
  }
  const slice = all.slice(Math.max(0, all.length - days));
  if (slice.length < 1) {
    sparkline.innerHTML = ''; chartMax.textContent = '';
    chartFoot.textContent = '(no data in this window)';
    return;
  }
  const W = 200, H = 60;
  const maxWh = Math.max(1, ...slice.map(d => d[1]));
  const w = W / slice.length;
  let totalWh = 0;
  const rects = slice.map(([_ts, wh], i) => {
    totalWh += wh;
    const h = (wh / maxWh) * (H - 2);
    return `<rect x="${(i * w).toFixed(2)}" y="${(H - h).toFixed(2)}" `
         + `width="${(w * 0.85).toFixed(2)}" height="${h.toFixed(2)}" `
         + `fill="#5ac8fa"/>`;
  }).join('');
  sparkline.innerHTML = rects;
  chartMax.textContent =
    `max ${maxWh.toFixed(0)} Wh/day`;
  chartFoot.textContent =
    `${slice.length}-day total: ${(totalWh / 1000).toFixed(2)} kWh`;
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
  const resetWhen = $('reset-when');
  if (!e) {
    ['kwh-last-hour','kwh-today','kwh-last-24h','kwh-this-week',
     'kwh-last-7d','kwh-this-month','kwh-last-30d','kwh-last-365d',
     'kwh-total','kwh-since-reset']
      .forEach(id => set(id, '—'));
    if (resetWhen) resetWhen.textContent = '';
    return;
  }
  set('kwh-last-hour',  fmtIntervalEntry(e.kwh_last_hour));
  set('kwh-today',      fmtIntervalEntry(e.kwh_today));
  set('kwh-last-24h',   fmtIntervalEntry(e.kwh_last_24h));
  set('kwh-this-week',  fmtIntervalEntry(e.kwh_this_week));
  set('kwh-last-7d',    fmtIntervalEntry(e.kwh_last_7d));
  set('kwh-this-month', fmtIntervalEntry(e.kwh_this_month));
  set('kwh-last-30d',   fmtIntervalEntry(e.kwh_last_30d));
  set('kwh-last-365d',  fmtIntervalEntry(e.kwh_last_365d));
  set('kwh-total',
      e.current_total_wh != null ? fmtKwh(e.current_total_wh / 1000) : '—');
  // Resettable counter — green to draw the eye, but the row sits at the
  // bottom of the grid alongside Lifetime so the layout stays familiar.
  set('kwh-since-reset',
      typeof e.kwh_since_reset === 'number' ? fmtKwh(e.kwh_since_reset) : '—');
  if (resetWhen) {
    if (e.reset_at_ts) {
      const age = Math.max(0, Math.floor(Date.now() / 1000) - e.reset_at_ts);
      let when;
      if (age < 60) when = `${age}s`;
      else if (age < 3600) when = `${Math.round(age / 60)}min`;
      else if (age < 86400) when = `${Math.round(age / 3600)}h`;
      else when = `${Math.round(age / 86400)}d`;
      resetWhen.textContent = `last reset: ${when} ago`;
    } else {
      resetWhen.textContent = 'never reset (Counter == Lifetime)';
    }
  }
}

function currentObservedText(j) {
  // Live observed values vs. the rule's threshold. Each policy's
  // current_window_minutes is the actual elapsed time since the rule
  // started observing (or was last reset by a manual toggle),
  // capped at the configured window. The bot now speaks minutes at
  // this boundary; internal seconds are converted once on send.
  const parts = [];
  if (j.idle && typeof j.idle.current_max_w === 'number') {
    const wm = j.idle.current_window_minutes ?? j.idle.duration_minutes;
    parts.push(`max ${j.idle.current_max_w.toFixed(0)}W in ${fmtMins(wm)}`);
  }
  if (j.consumed && typeof j.consumed.current_wh === 'number') {
    const wm = j.consumed.current_window_minutes ?? j.consumed.window_minutes;
    parts.push(`${j.consumed.current_wh.toFixed(2)}Wh in ${fmtMins(wm)}`);
  }
  if (j.avg && typeof j.avg.current_avg_w === 'number') {
    const wm = j.avg.current_window_minutes ?? j.avg.window_minutes;
    parts.push(`avg ${j.avg.current_avg_w.toFixed(1)}W in ${fmtMins(wm)}`);
  }
  return parts.join(' · ');
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
             + `for ${fmtMins(j.idle.duration_minutes)}`);
  }
  if (j.consumed) {
    parts.push(`when used < ${j.consumed.threshold_wh}Wh in `
             + `${fmtMins(j.consumed.window_minutes)}`);
  }
  if (j.avg) {
    parts.push(`when avg < ${j.avg.threshold_w}W in `
             + `${fmtMins(j.avg.window_minutes)}`);
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
      const cur  = currentObservedText(j);
      return `<li><span class="rule-text">${esc(desc)}</span>`
           + `<span class="rule-current">${esc(cur)}</span>`
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

function fmtMins(m) {
  if (m == null) return '—';
  if (m < 1) return `${Math.round(m * 60)}s`;
  if (m < 60) return Number.isInteger(m) ? `${m}m` : `${m.toFixed(1)}m`;
  const h = Math.floor(m / 60);
  const rem = Math.round(m - h * 60);
  return rem ? `${h}h${rem}m` : `${h}h`;
}

// --- Inbound -----------------------------------------------------------

window.webxdc.setUpdateListener((update) => {
  const p = update.payload;
  // The bot pushes {class, server_ts, devices} at the top level of
  // `payload`. (No wrapping `snapshot` key — that would be one extra
  // indirection for no reason.)
  if (!p || !p.devices) return;
  state.serverTs = p.server_ts || Math.floor(Date.now() / 1000);
  state.devices = p.devices;
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
