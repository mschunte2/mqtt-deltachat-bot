# CLAUDE.md — Project context for LLM sessions

## What this project is

`mqtt-bot` is a Delta Chat ↔ MQTT bridge that runs as a Python daemon
under systemd. Users in an authorised Delta Chat conversation can
inspect and control devices over MQTT through chat commands or a
webxdc app. The first concrete use case is monitoring and controlling
Shelly plugs (Plus Plug S Gen 2, Plug M Gen 3, etc.).

The chat-side mapping is **declarative**, not hard-coded. The Python
engine knows about *device classes* — JSON files under
`devices/<class>/class.json` that describe MQTT subscribe topics,
command verbs, state-field extraction recipes, chat-event rules, and
auto-off/auto-on configurations. Adding a second device type (Tasmota
plug, generic relay, sensor, …) is config-only: drop in a new
`devices/<your_class>/` directory with `class.json` + `app/`, no Python
edits.

Modeled on the sibling `gatekeeper-bot` project (Delta Chat ↔ BLE
smart lock). The bot framework, webxdc plumbing, `app_msgids.json`
pattern, and systemd template are derived from it; the engine,
scheduler, and component layout are new.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │ devices/<class>/class.json   (auto-discovered)│
                    │   subscribe, commands, state_fields,           │
                    │   chat_events, auto_off, auto_on               │
                    └──────────────────────────────────────────────┘
                                          │ load + validate at startup
                                          ▼
Delta Chat ──┬── /<device> <verb> [clause] ──────────┐
             │                                       │
             └── webxdc {request:{device,action,...}}─┤
                                                     ▼
                                              dispatch_command()
                                                • visibility check
                                                • action whitelist
                                                • render payload template
                                                     │
                                                     ▼
                                       mqtt.publish(<prefix>/<suffix>, payload)

                  ┌── on connect: subscribe per device for each class.subscribe ──┐
                  └────────────────────────────────────────────────────────────────┘
                                                     │
                                                     ▼
                                              on_mqtt_message()
                                              (topic → device,suffix)
                                                     │
                              ┌──────────────────────┼──────────────────────┐
                              ▼                      ▼                      ▼
                      extract per                 evaluate              tick scheduler
                      state_fields                chat_events           (idle/consumed)
                      → update cache              → post msgs           → fire on_off
                                                     │
                                                     ▼
                                       push_filtered to webxdc instances
                                       (per-chat filtered snapshot)
```

## Module layout and design rules

```
bot.py                  — Delta Chat hooks; thin glue
engine.py               — generic engine (dispatch, on_message, threshold, snapshots)
scheduler.py            — action scheduler (timer/tod/idle/consumed)
config.py               — devices.json + devices/*/class.json loader
state.py                — DeviceState + extraction (json_path, bool_text)
permissions.py          — global + per-device allow-list
mqtt_client.py          — paho wrapper (daemon thread, auto-resubscribe)
webxdc_io.py            — app_msgids.json + per-chat filtered push
durations.py            — parse "30m" / "1h30m"
templating.py           — {key} substitution that leaves JSON braces alone
```

### Design rules to keep when extending

- **No module imports `bot.py`.** Dependency flow is downward only.
- **No module-level mutable state outside dataclass fields each module
  owns.** `bot.py` constructs the objects and passes them in.
- **Pure functions** in `state.py`, `templating.py`, `durations.py`,
  `permissions.py`, `config.py` — easy to test, no I/O.
- **Side effects** confined to `mqtt_client.py`, `webxdc_io.py`,
  `scheduler.py` (its thread), and `bot.py` (Delta Chat RPC).
- **One class per file** when classes appear; small free functions
  otherwise. No deep inheritance.
- The Python module dependency graph is intentionally a DAG. If you
  find yourself wanting a cycle (e.g. engine importing bot), restructure
  via a callback or constructor injection.

## Component encapsulation: `devices/<class>/`

Every device class is a self-contained directory:

```
devices/<class>/
├── class.json             # the class definition (subscribe, commands, ...)
├── app/                   # webxdc source (HTML/JS/CSS + manifest.toml)
├── <class>.xdc            # built artifact (gitignored)
└── README.md              # what this class is for
```

`config.load()` discovers classes by walking `devices/*/class.json`.
There is no registry to update when adding a class. `webxdc_io.discover_xdcs()`
walks the same tree and yields `(class_name, /path/to/<class>.xdc)`.
`build-xdc.sh` walks `devices/*/app/` and zips each into the parent
directory.

The `class_name` in `class.json` MUST match the directory name (the
`devices` field in `devices.json` references it by name).

## Configuration

### `.env/env` (never under version control)

The user-side directory is `.env/`. It contains:
- `env` — sourced by `start-mqtt-bot.sh`; holds BOT_NAME, MQTT_*,
  ALLOWED_CHATS, LOG_LEVEL, etc.
- `<backup>.tar` (optional) — Delta Chat profile backup, imported by
  `init-from-backup.sh`.

Naming convention: files inside `.env/` don't need a leading dot
(they're already inside a hidden directory). The shell scripts hard-
code `.env/env` as the env-file path.

### `devices.json` (project-relative)

User instances only:

```json
{
  "devices": [
    {
      "name": "kitchen",
      "class": "shelly_plug",
      "topic_prefix": "shellyplugmg3-XXXXXXXXXXXX",
      "description": "Kitchen counter",
      "allowed_chats": [12345],
      "power_threshold_watts": 1500,
      "power_threshold_duration_s": 30
    }
  ]
}
```

Class definitions live in `devices/<class>/class.json`. Splitting them
keeps user config small and makes class definitions reusable across
deployments (a separate clone could ship the same `devices/shelly_plug/`
without touching their own `devices.json`).

`devices.json` may be either committed (when the repo is "your
deployment") or gitignored (when the repo is "the reusable bot").
The user committed theirs in v1 with two real plugs; this is fine but
if they later want to make the repo public they should
`git rm --cached devices.json` and add it to `.gitignore`.

## Permission model

Two layers, both checked on every operation:

1. **Global gate** (`ALLOWED_CHATS` env var) — chat IDs allowed to
   talk to the bot at all. `/id` is exempt (needed for setup). Empty
   list means every command is denied — bot logs a warning at startup.

2. **Per-device** (`allowed_chats` in `devices.json`) — restricts which
   chats can see/operate which devices. If the list is empty/omitted,
   falls back to the global `ALLOWED_CHATS` (sensible single-chat default).

Visibility propagates to: text replies, webxdc app payloads (snapshot
filtered per chat), online/offline alerts, threshold alerts, auto-off
trigger messages.

`permissions.chat_can_see(chat_id, device, fallback_chats)` is the
single function answering "is this chat allowed to operate this device".
Use it everywhere — never duplicate the check inline.

## Engine internals

### Subscription planner

At construction time, `Engine.__post_init__` builds a reverse lookup
`_topic_lookup: dict[topic_str, (device_name, suffix)]` from
every device × every `class.subscribe` entry. `subscriptions_for()`
returns its keys. `on_mqtt_message(topic, payload)` does an O(1)
lookup. Re-subscribed on every paho `on_connect` so reconnects work
without manual intervention.

### State cache

`Engine._states: dict[device_name, DeviceState]`. Each `DeviceState`
holds `fields: dict[str, Any]` (latest extracted values) and
`last_update_ts`. State fields are typed implicitly (whatever the
JSONPath / bool_text extraction produces).

**Concurrency:** read/written from the MQTT thread (on_mqtt_message)
and read from the scheduler thread (snapshot_for, jobs_for_device).
No explicit lock; we rely on the GIL and the access pattern (one writer,
one reader of snapshot copies). Acceptable for v1 — revisit if races
ever surface.

### Chat-event rules

Two types, defined per device class in `class.json`:

- **on_change**: post a chat message when a state field transitions.
  Fires only when `prev != new`. The message template is keyed by
  the new value (e.g. `"true": "💡 ON"`). Boolean values are coerced to
  string `"true"`/`"false"` for the lookup. **Renders an empty string
  for unknown values** — silent if a state arrives that the rule
  doesn't have a template for.

- **threshold**: per (device, field) state machine in `_thresholds`.
  When value ≥ limit for ≥ duration, fires `above` once. When value
  falls below, fires `below` and resets. The limit and duration are
  pulled from the *device*'s `params` (e.g. `power_threshold_watts`),
  not the class — so each device tunes its own thresholds. If the
  device omits these knobs, the rule is silently disabled for that
  device. Documented; not an error.

### Dispatch flow

`engine.dispatch_command(chat_id, device_name, action, source_msgid)`:

1. Validate device exists and chat can see it; validate `action ∈ class.commands`.
2. Render the command's `payload` template via `templating.render`,
   with `client_id` substituted; this is what `<prefix>/<cmd.suffix>`
   gets published with.
3. **Manual-override cancellation:** call `scheduler.cancel(device_name,
   target_action=action)` to drop any pending job whose `target_action`
   matches. Only same-direction jobs are cancelled — a manual `off`
   does NOT clear a pending auto-on (the user's morning timer should
   still fire). For each cancelled job, post the class's
   `cancelled_manual` template to visible chats.
4. React on the source message with 🆗 (text path only).

### on_fire callback (scheduler → engine)

When the scheduler trips, it calls `engine.on_fire(device_name,
chat_id_origin, target_action, mode, ctx)`:

1. Resolve `target_action` to a class command and publish.
2. Look up the matching auto_off OR auto_on `trigger_messages`
   section by matching `section.command` against `target_action`.
3. Render the section's `mode` template with ctx (includes `name`,
   `value`, `seconds`, `field`, `hh`, `mm` where applicable).
4. Post to visible chats only (per-device `allowed_chats`).

### snapshot_for callback (webxdc_io → engine)

`engine.snapshot_for(chat_id, class_name)` returns the payload
pushed to a single (chat, class) webxdc instance, or None to skip.
Builds a `{class, devices: {<name>: {fields, scheduled_jobs, ...}},
server_ts}` shape from the state cache + scheduler jobs, filtered
to devices visible to this chat. Returning None when nothing is
visible (skip the push). The webxdc app keeps its own client-side
power history; we don't ship a sample buffer.

## Scheduler internals

### Policies (any subset can be active per job)

- **timer:** `deadline_ts` (now + N seconds), one-shot.
- **time_of_day (`tod`):** next HH:MM in local time. With
  `recurring_tod=True`, re-arms to next day after firing. Uses
  `time.mktime` with `tm_isdst=-1` so DST transitions resolve correctly
  via calendar dates rather than `+86400` arithmetic.
- **idle:** `state[idle_field] < idle_threshold` for ≥
  `idle_duration_s`. Pull-driven: `tick()` checks after each state
  field update. Falls back to None when the field isn't a number;
  resets `_below_since` when value rises above threshold.
- **consumed:** `integral(state[field]·dt)` over the last `window_s` <
  `threshold_wh`. Pull-driven via `tick()`. Maintains a per-job sample
  deque trimmed to the window. Only evaluated AFTER the window has
  been populated (now ≥ `_consumed_started_at + window_s`); otherwise
  scheduling `until used <5Wh in 10m` would fire instantly because
  zero samples = zero Wh.

Time-based policies are mutually exclusive within one job (timer XOR
tod). Idle and consumed can each appear at most once. All policies
combine OR-wise — whichever fires first wins.

### Jobs map

`Scheduler._jobs: dict[(device_name, target_action), ScheduledJob]`.

Keying by `(device, target_action)` lets a single device hold both a
pending auto-on AND a pending auto-off concurrently (the morning-on
+ evening-off use case). Cancellation by action is selective.

### Daemon thread + wake event

A single daemon thread sleeps until `min(deadline_ts)` across all
time-based jobs. `_wake: threading.Event` is set on every
`schedule()` and `cancel()` so the thread re-evaluates immediately.
The loop has two lock-protected passes inside one wakeup:

1. Pass 1: detect time-based fires, re-arm recurring TODs, drop
   one-shot fired jobs.
2. Pass 2: collect the next deadline from surviving jobs.

Then it fires (outside the lock) and waits.

Idle/consumed checks don't wake the thread; they happen in `tick()`,
which runs in whichever thread called it (typically the MQTT thread
via `engine.on_mqtt_message`).

### Persistence

**None.** Bot restart drops all pending jobs. Documented behaviour, not
a bug. Adding persistence is a single-file `_jobs.json` change but
brings replay-on-restart edge cases (does an idle job fired during the
outage need a "fired retroactively" message?). Defer until someone asks.

## Templating

`templating.render(template, ctx)` substitutes `{name}` or
`{name:fmt}` placeholders. Implemented as a regex sub
(`\{[A-Za-z_]\w*(?::[^{}]*)?\}`), **not** `str.format_map`, because
class.json command payloads embed JSON like
`{"id":1,"src":"{client_id}","method":"Switch.GetStatus","params":{"id":0}}`.
With format_map, the literal `{` would be parsed as a format spec and
crash. With our regex, only known identifiers are matched; JSON braces
pass through untouched.

Missing keys render as empty strings (not KeyError) so device-class
authors don't have to know which contexts include which optional
fields.

This was a real bug caught by the engine integration tests — be careful
to preserve this behaviour. Don't switch to format_map "for simplicity".

## Webxdc app protocol

### app → bot

```json
{"payload": {"request": {
   "device": "kitchen",
   "action": "on" | "off" | "toggle" | "auto-off" | "cancel-auto-off" | ...,
   "ts": 1714000000,
   "auto_off": {"timer_seconds": 1800, "time_of_day": [18,30], ...},
   "auto_on":  {"time_of_day": [7,0], "recurring_tod": true}
}}}
```

The bot validates `action` against `class.commands` (for the direct
verbs) or treats it as a schedule keyword. Optional `auto_off` /
`auto_on` keys carry an inline policy object that the engine assembles
into a `ScheduledPolicy` after the direct action runs. Keys
recognised inside `auto_off`/`auto_on`: `timer_seconds` (int),
`time_of_day` ([h,m] list), `recurring_tod` (bool), `idle` (object
with `field`/`threshold`/`duration_s`), `consumed` (object with
`field`/`threshold_wh`/`window_s`). Any subset → OR-combined.

### bot → app (snapshot)

```json
{"payload": {
   "class": "shelly_plug",
   "devices": {
     "kitchen": {
       "name": "kitchen",
       "description": "Kitchen counter",
       "fields": {"online": true, "output": false, "apower": 0.0, ...},
       "last_update_ts": 1714000000,
       "scheduled_jobs": [{"target_action": "off", "deadline_ts": ...,
                            "time_of_day": null, "idle": null, "consumed": null}]
     }
   },
   "server_ts": 1714000000
}}
```

Pushed on every inbound MQTT message that updates state. The app
keeps a local power-history ring buffer (last 5 min, ~60-200 samples)
for the sparkline; the bot does **not** ship history — the app
accumulates its own.

### `/apps` onboarding

`/apps` is the **sole onboarding gate**. The bot sends one xdc per
device class that has visible devices in the chat, then deletes prior
copies (always-resend pattern: late joiners get a fresh install).
Tracked in `~/.config/<BOT_NAME>/app_msgids.json` as
`{chat_id: {class_name: msgid}}`. Webxdc updates from msgids the bot
has never recorded are dropped with a log line — points the user at
`/apps`.

## Replay protection

Three windows, all enforced in `bot.py`:

- `MAX_AGE_SECONDS = 200` — typed text command must be fresh enough.
  Sized to absorb a single MQTT broker reconnect + retry without
  losing user-typed `/status`.
- `MAX_APP_AGE_SECONDS = 45` — webxdc button taps. Tighter because
  the app shows its own pending state and stale taps are usually
  unintended.
- `MAX_CLOCK_SKEW_SECONDS = 30` — accept future-dated messages within
  this skew (NTP not yet settled, sender clock ahead). Beyond this
  → drop as untrusted future-dated.

The `ts` field on app requests is required. Apps without a `ts` field
are rejected with a log line directing the user to `/apps` to refresh.

## Testing pattern

`test_mqtt_bot.py` at the repo root, stdlib `unittest`. 57 tests in
~15 ms. Pure modules need no stubs (they don't import deltachat2 or
paho). Engine integration tests stub:

- `deltachat2.MsgData` via `sys.modules` patch (so `import engine`
  doesn't fail without the package installed)
- `mqtt`, `webxdc`, `scheduler`, `bot.rpc` via small stub classes
  that record calls

Run with `python3 test_mqtt_bot.py`. Coverage: durations parser,
templating regex (incl. JSON brace passthrough), state extraction
(bool_text + json_path edges), permissions (global + per-device +
fallback), scheduler.parse_policy (every form + restricted kinds),
scheduler.integrate_wh, scheduler.next_tod_deadline, scheduler.tick
(idle fire + reset), config loader (every error path), engine
(unknown device, permission denied, unknown action, publish, template
substitution including JSON, manual-override cancellation, threshold
detector fire + clear, on_fire publish + post, snapshot filtering).

Add tests when extending — pure-module changes get pure-function tests;
engine flow changes get engine integration tests.

## Build system

Apps are packaged by `./build-xdc.sh`, a plain `zip`. No bundler; a
.xdc is just a zip with `index.html` + `manifest.toml` at the root.
The build script can take an explicit class directory or build all of
`devices/*/app`. Output is `devices/<class>/<class>.xdc` (gitignored).

`realpath -m` is used to canonicalize the output path before `cd`-ing
into the temp staging dir; it tolerates the file not existing yet.
Without `-m`, realpath would fail on a fresh build.

## --check-config dry run

`python3 bot.py --check-config` validates `devices.json` +
`devices/*/class.json` and exits 0/non-zero. Critically, it works
**without the venv installed** — the heavy imports (deltachat2,
deltabot-cli, paho, appdirs) are deferred until after the early-exit
check. Useful for CI / pre-commit / a quick check on a fresh clone.

## Deployment

```
1. sudo ./setup-mosquitto.sh   # apt + config + bot user
2. (Configure each Shelly plug to point at the broker via its web UI)
3. ./init-from-backup.sh       # if .env/<dump>.tar present
4. ./start-mqtt-bot.sh         # runs in foreground; check logs
5. /id in chat → ALLOWED_CHATS in .env/env → restart bot
6. /apps in chat
7. sudo ./install-systemd-unit.sh  # promote to a service
```

systemd unit name: `deltabot-${BOT_NAME}.service`. Multiple bot
instances on one host work as long as `BOT_NAME` differs (each gets
its own `~/.config/<name>/` and unit name).

`paho-mqtt>=2.0` is required (pinned in `lib/common.sh`). The 2.x
API requires `callback_api_version=`; we pass `VERSION1` because
that's the signature our handlers are written for. `mqtt_client.py`
falls back to the 1.x API if `CallbackAPIVersion` isn't importable.

## Known limitations / accepted trade-offs

- **No scheduler persistence.** Bot restart drops pending auto-off /
  auto-on jobs. The user can re-issue them. Adding persistence would
  bring replay-on-restart edge cases.
- **No live config reload.** `devices.json` edits require a
  `systemctl restart`. Adding watch-and-reload would mean reasoning
  about state cache/scheduler invalidation for removed devices —
  not worth the v1 complexity.
- **Engine state cache has no explicit lock.** Relies on GIL; access
  pattern is one writer (MQTT thread) and one reader (scheduler
  thread reading snapshots). Safe in practice. If we ever see weird
  state races, wrap `_states` access in a `threading.Lock`.
- **No rate-limiting on chat output.** A flapping plug produces one
  on/off message per state push. Acceptable for v1.

## webxdc app gotchas (caught the hard way)

- **`<script src="webxdc.js">` is mandatory.** The messenger only injects
  `window.webxdc` when index.html explicitly loads `webxdc.js` (a virtual
  URL the messenger intercepts). Without that script tag, `window.webxdc`
  is undefined and the picker stays empty even though the bot is pushing
  status updates correctly. Took us a debug pane to catch it; index.html
  has a comment-band reminding future devs.
- **`/apps` must seed the new instance.** Webxdc status updates are
  per-msgid; the bot's MQTT-driven push only fires on inbound messages.
  Without an explicit `webxdc.push_filtered(...)` after `send_apps`, a
  newly-installed app sees nothing until the next plug status update —
  and Shelly's `status/switch:0` is non-retained, so a quiet plug means
  a blank UI for minutes. The `_handle_apps` path calls push_filtered
  right after recording the new msgid.
- **bot.py needs `logging.basicConfig`.** deltabot-cli only attaches a
  Rich handler to its own logger; without basicConfig at the top of
  bot.py, every `mqtt_bot.engine`/`scheduler`/`mqtt`/`history` log call
  is silently dropped. We hit this trying to debug the empty-picker bug.

## History (SQLite time series)

`history.py` owns a single SQLite file at
`~/.config/<BOT_NAME>/history.sqlite`:

- `power_minute(device, ts, avg_apower_w, sample_count, output)` —
  one row per minute per device. `output` is the relay state
  (0/1/NULL); used to colour the chart red on off-segments.
- `energy_hour(device, ts, aenergy_wh)` — cumulative aenergy.total
  snapshot at the top of each hour (latest sample within the hour
  wins).
- `events(device, ts, suffix, kind, payload)` — raw events from
  `events/rpc` topics. `kind` is the JSON `method` field if present.

Engine.on_mqtt_message writes a sample on every `apower` / `aenergy`
update and writes to `events` for `events/rpc` traffic. Engine.set_bot
calls history.flush_pending_minutes via the SIGTERM handler in bot.py
(systemd sends SIGTERM on stop).

`RETENTION_DAYS` env var: `0` means keep forever, `>0` prunes
power_minute / energy_hour / events older than N days, evaluated
once per day on the next write.

## Webxdc protocol — additional message types

Beyond the v1 device snapshot and request shapes, the engine accepts:

- `{action: "history", window_seconds: N}` → response carries
  `power_points: [[ts, w, output], ...]` (3-tuples, `output` is
  0/1/null) plus `energy_points: [[ts, wh], ...]` (hourly snapshots).
  The app downsamples energy_points client-side into per-bucket bars.
- `{action: "events", window_seconds: N, limit: K}` → response carries
  `rows: [{ts, suffix, kind, payload}, …]` from the events table.
- `{action: "set_param", param: "power_threshold_watts", value: N}` →
  in-memory override of `device.params[param]`. Whitelisted to
  `power_threshold_watts` and `power_threshold_duration_s`. Lost on
  bot restart by design — persistence is via devices.json.

The snapshot now also includes a per-device `energy` block:
`{kwh_last_hour, kwh_today, kwh_last_24h, kwh_this_week, kwh_last_7d,
kwh_this_month, kwh_last_30d, current_total_wh}`. Each kwh_* is `null`
until we have an `energy_hour` snapshot at-or-after the interval start
(so on a fresh deploy, "this month" sits at null until the next
month's first hour).

## Chat command additions

- `/<device> export 7d` — bot dumps power_minute + energy_hour for the
  window to a CSV and sends as a chat attachment. Window accepts
  `s/m/h/d` (the d unit was added for this).

## Provenance and history

- 2026-05-07 v1 baseline — initial commit `10477fe`. 11 Python
  modules, ~2,830 LoC, 57 tests, single device class
  (`shelly_plug`), one webxdc app, full deployment scripts.
- 2026-05-07 v1.1 — committed in same session. Added: SQLite-backed
  history (per-minute power, hourly energy, events table) with
  RETENTION_DAYS knob; segmented red/green chart with a 0-line for
  off periods; energy summary (last hour through lifetime); per-bucket
  kWh bars; recent-events viewer; in-memory power-threshold tuning
  from the app; `/<dev> export Nd` CSV chat command; SIGTERM
  shutdown hook to flush the in-minute buffer; basicConfig logging
  fix; the `<script src=webxdc.js>` fix; `/help` and `/id` bypass the
  allow-list; `/help` auto-posts on member-add. ~79 tests.
- 2026-05-07 v1.2 — same-day. Parallel scheduled rules per
  (device, target_action), keyed by stable rule_id derived from
  policy contents. Each rule fires independently; redundant fires
  are intentionally posted to chat (audit trail > silence). New
  trigger templates use `{action_verb}`, `{threshold}`,
  `{duration_human}`, `{window_human}` so messages spell out which
  rule tripped and why. App grows two `<details>` sections (Auto-off
  rules / Auto-on rules) with × delete buttons + Add-rule forms.
- 2026-05-07 v1.3 — same-day. Maximalist data capture into SQLite:
  `samples_raw` (every status/switch:0 verbatim, lossless) and
  `energy_minute` (Shelly's authoritative `aenergy.by_minute[1..2]`
  in mWh per minute boundary). `energy_consumed_in` does a
  per-minute hybrid in one SQL query — energy_minute first,
  power_minute fallback for minutes without by_minute coverage.
  Parser became more permissive: `30 min`, `1 hour`, `200 Wh in 30
  min`, `5 W in 60 sec`, `1 day`, etc. all parse (multi-token
  durations + verbose unit names). Energy summary entries report
  `partial_since_ts` so the app can mark a `*` when our window data
  starts later than requested. `/all <verb>` chat shortcut acts on
  every visible device. App grows a 30-day daily-energy bar chart.
  Threshold tuning from the app can now persist to `devices.json`
  via a `set_param` request with `persist: true`.

## Performance + tuning

- `status_update_interval` on the Shelly plug (default ~60 s,
  minimum 1 s) controls how often `status/switch:0` arrives. We
  recommend **15 s**: ~4× the default detail in `apower` curves +
  `samples_raw`, but still very modest MQTT traffic and disk usage.
  At 15 s with two plugs, samples_raw grows ~70 MB/year. The
  authoritative `aenergy.by_minute` data is unaffected (always
  per-minute regardless of update cadence).
- SQLite WAL mode is enabled. Concurrent readers don't block
  the writer. The bot has one writer (engine) and occasional readers
  (history queries from snapshot_for / handle_history_request).
- Read methods short-circuit on `self._closed` so the MQTT thread
  draining its inbox after shutdown doesn't ProgrammingError.

## webxdc app — section order

The app is a single page; sections from top to bottom:

1. Header — device picker + online dot
2. State card — current ON/OFF + apower + aenergy
3. On / Off / Toggle buttons
4. Power chart (live or windowed) with daily-energy bars below
5. Energy consumed (8-row grid)
6. Auto-off rules (open by default — most-used surface)
7. Auto-on rules
8. Recent events (collapsed; on-open fetches last 50)
9. Tuning · power threshold (Apply = in-memory; Save = persists
   to devices.json on the bot host)
10. Footer

## Icon

`icon.svg` is the source of truth. `icon.png` is committed alongside
because the Pi doesn't ship with ImageMagick and we want fresh
clones to render the icon without an extra apt-get. Regeneration:

```sh
cd devices/shelly_plug/app
convert -background none -density 256 icon.svg -resize 256x256 icon.png
```

Run after editing the SVG.
- Designed iteratively in conversation. Notable course corrections:
  - Generic engine + class-as-data was chosen over a Shelly-specific
    bot, paying ~80 LoC up front to make adding a second device
    type config-only.
  - Components encapsulated under `devices/<class>/` mid-build (the
    initial layout had `apps/<class>.xdc` + class def in
    `devices.json`).
  - Auto-on policy added late; turned `auto_off.py` into the more
    general `scheduler.py` with `target_action` keyed jobs.
  - `time_of_day` as a 4th policy plus the rolling-energy
    `consumed` policy added in the same pass, surfacing the need
    for `PolicyDefaults` as a config-agnostic dataclass between
    engine and scheduler.
  - Templating moved from `str.format_map` to regex substitution
    after the first engine integration test crashed on a JSON
    payload's literal `{`.
- Modeled on `gatekeeper-bot` (sibling project at
  `../gatekeeper-bot/`). Reused: BotCli scaffold,
  `app_msgids.json` atomic write, `_push_state` broadcaster pattern,
  systemd template, `build-xdc.sh` zip approach, replay protection
  windows, `/id` permission-free handling, stdlib-unittest stub
  pattern. New: engine, scheduler, component layout, devices.json
  schema, multi-class apps, two-layer permissions.
