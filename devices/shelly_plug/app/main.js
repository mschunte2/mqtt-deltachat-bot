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
const chartMax = $('chart-max');
const chartFoot = $('chart-foot');
const windowPick = $('window-pick');
const lastUpdate = $('last-update');
const autoStatus = $('auto-status');
const btnApply = $('btn-apply');
const btnCancel = $('btn-cancel');

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
}

function renderSparkline() {
  // Live mode: client-side ring buffer (last 5 min).
  // Window mode: server-pushed history (6h/12h/24h/31d).
  let pts;     // [[ts, w], ...]
  let footText = '';
  if (state.historyWindow === 0) {
    const hist = (state.history[state.active] || []).slice();
    pts = hist.map(s => [s.ts, s.power]);
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
  const linePts = pts.map(([t, w]) => {
    const x = ((t - tMin) / tSpan) * W;
    const y = H - (w / pMax) * (H - 6) - 3;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  sparkline.innerHTML =
    `<polyline fill="none" stroke="#34c759" stroke-width="1.5" points="${linePts}"/>`;
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

function appendSample(name, ts, power) {
  if (typeof power !== 'number') return;
  const h = (state.history[name] = state.history[name] || []);
  if (h.length && h[h.length - 1].ts === ts) return;  // dedup same-second pushes
  h.push({ ts, power });
  const cutoff = ts - SAMPLES_MAX_AGE;
  while (h.length && h[0].ts < cutoff) h.shift();
  if (h.length > SAMPLES_MAX_COUNT) h.splice(0, h.length - SAMPLES_MAX_COUNT);
}

window.webxdc.setUpdateListener((update) => {
  const p = update.payload;
  if (!p) return;
  // History response: {history: {device, window_seconds, bucket_seconds,
  //                              power_points, energy_points, ...}}
  if (p.history && p.history.device) {
    state.serverHistory[p.history.device] = p.history;
    renderSparkline();
    return;
  }
  // Regular snapshot: {class, devices, server_ts}
  if (!p.devices) return;
  state.devices = p.devices;
  const ts = p.server_ts || Math.floor(Date.now() / 1000);
  for (const [name, dev] of Object.entries(p.devices)) {
    if (dev.fields && typeof dev.fields.apower === 'number') {
      appendSample(name, ts, dev.fields.apower);
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
