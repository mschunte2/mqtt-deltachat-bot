'use strict';

const SAMPLES_MAX_AGE = 300;
const SAMPLES_MAX_COUNT = 200;

const state = {
  devices: {},
  active: null,
  history: {},
};

const $ = (id) => document.getElementById(id);
const picker = $('device-picker');
const onlineDot = $('online-dot');
const stateText = $('state-text');
const statePower = $('state-power');
const stateEnergy = $('state-energy');
const sparkline = $('sparkline');
const chartMax = $('chart-max');
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
});

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
  const hist = (state.history[state.active] || []).slice();
  if (hist.length < 2) {
    sparkline.innerHTML = '';
    chartMax.textContent = '';
    return;
  }
  const tMin = hist[0].ts;
  const tMax = hist[hist.length - 1].ts;
  const tSpan = Math.max(1, tMax - tMin);
  const pMax = Math.max(1, ...hist.map(s => s.power));
  const W = 200, H = 60;
  const points = hist.map(s => {
    const x = ((s.ts - tMin) / tSpan) * W;
    const y = H - (s.power / pMax) * (H - 6) - 3;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  sparkline.innerHTML =
    `<polyline fill="none" stroke="#34c759" stroke-width="1.5" points="${points}"/>`;
  chartMax.textContent = `max ${pMax.toFixed(0)} W`;
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

let _dbgCount = 0;
window.webxdc.setUpdateListener((update) => {
  _dbgCount++;
  const dbgCount = $('dbg-count'); if (dbgCount) dbgCount.textContent = _dbgCount;
  const dbgPre = $('dbg-pre');
  if (dbgPre) {
    try { dbgPre.textContent = JSON.stringify(update, null, 2).slice(0, 2000); }
    catch (e) { dbgPre.textContent = 'JSON.stringify failed: ' + e; }
  }
  const p = update.payload;
  if (!p || !p.devices) return;
  state.devices = p.devices;
  const ts = p.server_ts || Math.floor(Date.now() / 1000);
  for (const [name, dev] of Object.entries(p.devices)) {
    if (dev.fields && typeof dev.fields.apower === 'number') {
      appendSample(name, ts, dev.fields.apower);
    }
  }
  render();
}, 0);

// keep countdowns live
setInterval(() => {
  const dev = activeDevice();
  if (dev) renderAutoStatus(dev);
}, 1000);
