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

const $ = (id) => document.getElementById(id);
const picker = $('device-picker');
const onlineDot = $('online-dot');
const stateText = $('state-text');
const statePower = $('state-power');
const stateEnergy = $('state-energy');
const sparkline = $('sparkline');
const bars = $('bars');
const chartMax = $('chart-max');
const chartFoot = $('chart-foot');
const windowPick = $('window-pick');
const lastUpdate = $('last-update');
const autoStatus = $('auto-status');
const btnApply = $('btn-apply');
const btnCancel = $('btn-cancel');
const eventsList = $('events-list');
const tuneWatts = $('tune-watts');
const tuneSecs = $('tune-secs');
const tuneApply = $('tune-apply');

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
btnCancel.addEventListener('click', () => send({ action: 'cancel-auto-off' }));

btnApply.addEventListener('click', () => {
  const mode = document.querySelector('input[name="auto"]:checked').value;
  if (mode === 'none') {
    send({ action: 'cancel-auto-off' });
    return;
  }
  const auto_off = {};
  if (mode === 'timer') {
    const mins = parseInt($('auto-min').value, 10) || 0;
    auto_off.timer_seconds = mins * 60;
  } else if (mode === 'tod') {
    const v = $('auto-tod').value || '22:00';
    const [h, m] = v.split(':').map(n => parseInt(n, 10));
    auto_off.time_of_day = [h, m];
    auto_off.recurring_tod = $('auto-tod-daily').checked;
  } else if (mode === 'idle') {
    auto_off.idle = {
      threshold: parseFloat($('auto-idle-w').value),
      duration_s: parseInt($('auto-idle-s').value, 10),
    };
  } else if (mode === 'consumed') {
    auto_off.consumed = {
      threshold_wh: parseFloat($('auto-cons-wh').value),
      window_s: parseInt($('auto-cons-min').value, 10) * 60,
    };
  }
  // If the device is currently OFF, "Apply" turns it on AND schedules.
  // If already on or unknown, just schedule the auto-off (no toggle).
  const dev = activeDevice();
  const isOff = dev && dev.fields && dev.fields.output === false;
  if (isOff) {
    send({ action: 'on', auto_off });
  } else {
    send({ action: 'auto-off', auto_off });
  }
});

picker.addEventListener('change', () => {
  state.active = picker.value;
  render();
  if (state.historyWindow > 0) requestHistory();
});

windowPick.addEventListener('change', () => {
  state.historyWindow = parseInt(windowPick.value, 10) || 0;
  if (state.historyWindow > 0) requestHistory();
  renderSparkline();
});

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

if (tuneApply) {
  tuneApply.addEventListener('click', () => {
    if (!state.active) return;
    const w = parseFloat(tuneWatts.value);
    const s = parseInt(tuneSecs.value, 10);
    if (!isFinite(w) || !isFinite(s)) return;
    window.webxdc.sendUpdate({
      payload: {
        request: {
          device: state.active, action: 'set_param',
          param: 'power_threshold_watts', value: w,
          ts: Math.floor(Date.now() / 1000),
        }
      }
    }, '');
    window.webxdc.sendUpdate({
      payload: {
        request: {
          device: state.active, action: 'set_param',
          param: 'power_threshold_duration_s', value: s,
          ts: Math.floor(Date.now() / 1000),
        }
      }
    }, '');
  });
}

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
  renderAutoStatus(dev);
  renderEnergySummary(dev);
  renderTuningInputs(dev);
}

function fmtKwh(kwh) {
  if (kwh == null) return '—';
  if (kwh < 0.01) return `${(kwh * 1000).toFixed(1)} Wh`;
  return `${kwh.toFixed(2)} kWh`;
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
  set('kwh-last-hour',  fmtKwh(e.kwh_last_hour));
  set('kwh-today',      fmtKwh(e.kwh_today));
  set('kwh-last-24h',   fmtKwh(e.kwh_last_24h));
  set('kwh-this-week',  fmtKwh(e.kwh_this_week));
  set('kwh-last-7d',    fmtKwh(e.kwh_last_7d));
  set('kwh-this-month', fmtKwh(e.kwh_this_month));
  set('kwh-last-30d',   fmtKwh(e.kwh_last_30d));
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

function renderBars(energyPoints) {
  if (!bars) return;
  if (!Array.isArray(energyPoints) || energyPoints.length < 2) {
    bars.innerHTML = '';
    return;
  }
  // Compute Wh deltas between consecutive snapshots; clamp negatives to 0
  // (counter resets shouldn't render as negative bars).
  const deltas = [];
  for (let i = 1; i < energyPoints.length; i++) {
    const t = energyPoints[i][0];
    const dWh = Math.max(0, energyPoints[i][1] - energyPoints[i - 1][1]);
    deltas.push({ t, wh: dWh });
  }
  if (!deltas.length) { bars.innerHTML = ''; return; }
  // For 31d window we have ~744 bars; downsample to ~30 daily buckets.
  const span = deltas[deltas.length - 1].t - deltas[0].t;
  let dec = 1;
  if (deltas.length > 60) dec = Math.ceil(deltas.length / 60);
  const bucketed = [];
  for (let i = 0; i < deltas.length; i += dec) {
    let sum = 0, count = 0, t = deltas[i].t;
    for (let j = i; j < Math.min(i + dec, deltas.length); j++) {
      sum += deltas[j].wh; count++;
    }
    bucketed.push({ t, wh: sum });
  }
  const W = 200, H = 36;
  const maxWh = Math.max(1, ...bucketed.map(b => b.wh));
  const w = W / bucketed.length;
  const rects = bucketed.map((b, i) => {
    const h = (b.wh / maxWh) * (H - 2);
    return `<rect x="${(i * w).toFixed(2)}" y="${(H - h).toFixed(2)}" `
         + `width="${(w * 0.85).toFixed(2)}" height="${h.toFixed(2)}" `
         + `fill="#5ac8fa"/>`;
  }).join('');
  bars.innerHTML = rects + `<text x="${W - 2}" y="10" font-size="9" `
    + `text-anchor="end" fill="#888">max ${maxWh.toFixed(0)} Wh</text>`;
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
  // Window mode: server-pushed history (6h/12h/24h/31d).
  let pts;     // [[ts, w], ...]
  let footText = '';
  if (state.historyWindow === 0) {
    const hist = (state.history[state.active] || []).slice();
    pts = hist.map(s => [s.ts, s.power, s.out]);
    footText = pts.length ? `${pts.length} samples (live)` : '(live, waiting…)';
  } else {
    const sh = state.serverHistory[state.active];
    if (!sh || !sh.power_points) {
      pts = [];
      footText = '(loading…)';
    } else {
      pts = sh.power_points;
      const e = sh.energy_points || [];
      const totalWh = e.length >= 2 ? (e[e.length - 1][1] - e[0][1]) : 0;
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
  const tMin = pts[0][0];
  const tMax = pts[pts.length - 1][0];
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

function renderAutoStatus(dev) {
  const jobs = (dev.scheduled_jobs || []).filter(j => j.target_action === 'off');
  if (!jobs.length) {
    autoStatus.textContent = '(none)';
    btnCancel.hidden = true;
    return;
  }
  const j = jobs[0];
  const parts = [];
  if (j.deadline_ts) {
    const remaining = Math.max(0, j.deadline_ts - Math.floor(Date.now() / 1000));
    parts.push(`in ${fmtSecs(remaining)}`);
  }
  if (j.idle) parts.push(`idle<${j.idle.threshold}W`);
  if (j.consumed) parts.push(`<${j.consumed.threshold_wh}Wh/${Math.round(j.consumed.window_s / 60)}m`);
  autoStatus.textContent = `(${parts.join(' or ')})`;
  btnCancel.hidden = false;
}

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
    if (p.history.device === state.active) {
      renderBars(p.history.energy_points);
    }
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
  if (dev) renderAutoStatus(dev);
}, 1000);
