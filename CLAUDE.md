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

## Architecture (v0.2 — digital twin)

```
   ┌────────────────────────────────────────────────────────┐
   │  MQTT backend  (mqtt_client.py)                        │
   │  inbound:  topic → twins.find_by_topic → twin.on_mqtt  │
   │  outbound: twin.dispatch / twin.tick_time → publish    │
   └─────────────────────────┬──────────────────────────────┘
                             │ updates / commands
                             ▼
   ┌────────────────────────────────────────────────────────┐
   │  Digital twins  (plug.py · one PlugTwin per device)    │
   │  ground truth: fields, rules, threshold latches        │
   │  behaviours:   on_mqtt, dispatch, schedule, cancel,    │
   │                tick_time, snapshot                     │
   └─────────────────────────┬──────────────────────────────┘
                             │ snapshots
                             ▼
   ┌────────────────────────────────────────────────────────┐
   │  Publisher  (publisher.py)  — single outbound stream   │
   │  broadcast(): for every (chat, class) in webxdc msgid  │
   │               registry → snapshot.build_for_chat → push│
   │  push_unicast(): refresh button + /apps onboarding     │
   │  daemon thread: every PUBLISH_INTERVAL_S seconds       │
   └────────────────────────────────────────────────────────┘
```

The single source of truth for each plug's state is its `PlugTwin`.
All inbound MQTT, all rule evaluation (idle/consumed inline,
timer/tod via the sweeper), and all outbound dispatch live as twin
methods. The Publisher is the only outbound pipeline; nothing else
sends webxdc status updates. Chat-event text emission goes through
a single `_post_to_visible_chats` helper in `bot.py`.

External toggles (other chat user, second app instance, physical
button on the plug) all come back as `status/switch:0` echoes —
`twin.on_mqtt` detects the prev≠new edge and broadcasts. The twin
never trusts a command it sent; only the plug's MQTT echo updates
state.

## Module layout and design rules

```text
bot.py                       — entry point: env, construction, hooks, BotCli
mqtt_bot/                    — the Python package
├── __init__.py
├── commands.py              — pure parser: /<dev> <verb> + replay-window consts
├── formatters.py            — chat-reply display: device line, rule clauses
├── rehydrate.py             — rule transient-state backfill on restart
├── core/                    — declarative twin engine (class-agnostic)
│   ├── twin.py              — PlugTwin: on_mqtt, dispatch, schedule, tick_time
│   ├── twins.py             — TwinRegistry (dict + reverse topic lookup)
│   ├── rules.py             — ScheduledJob/Policy/Defaults; parse_policy;
│   │                          RulesSweeper daemon; rules.json persistence
│   ├── snapshot.py          — single function build_for_chat
│   └── state.py             — extract() pure function (json_path, bool_text)
├── io/                      — modules with side effects
│   ├── history.py           — SQLite time series
│   ├── baselines.py         — baselines.json round-trip + legacy migration
│   ├── mqtt_client.py       — paho wrapper (daemon thread, auto-resubscribe)
│   ├── publisher.py         — Publisher class (the only outbound stream)
│   └── webxdc_io.py         — app_msgids.json + send_apps + push_to_msgid
└── util/                    — pure utilities, no side effects
    ├── config.py            — devices.json + devices/*/class.json loader
    ├── durations.py         — parse "30m" / "1h30m"
    ├── permissions.py       — global + per-device allow-list
    └── templating.py        — {key} substitution that leaves JSON braces alone

devices/                     — declarative device classes (config + apps)
├── shelly_plug/
└── tasmota_plug/

tests/                       — stdlib unittest, one file per module
├── __init__.py              — sys.path + deltachat2 stub setup
├── _fixtures.py             — CLASS_JSON_OK, _build_twin, _FakeHistory
└── test_<module>.py         — per-module tests
```

### Design rules to keep when extending

- **No module imports `bot.py`.** Dependency flow is downward only.
- **No module-level mutable state outside dataclass fields each module
  owns.** `bot.py` constructs the objects and passes them in.
- **PlugTwin owns all per-device state.** No engine cache, no
  scheduler cache. The twin is the digital twin; everything reads
  from it.
- **Single outbound assembly point** — `snapshot.build_for_chat` is
  the only function that produces an app payload. **Single outbound
  pipeline** — `Publisher` is the only thing that pushes to apps.
- **Pure functions** live under `mqtt_bot/util/` (config, durations,
  permissions, templating) and `mqtt_bot/core/state.py` — easy to
  test, no I/O.
- **Side effects** confined to `mqtt_bot/io/` (mqtt_client,
  webxdc_io, history, baselines, publisher), the
  `RulesSweeper` thread inside `mqtt_bot/core/rules.py`, and
  `bot.py` (Delta Chat RPC + signal handling).
- **One class per file** when classes appear; small free functions
  otherwise. No deep inheritance.
- The Python module dependency graph is intentionally a DAG. Twins
  receive their dependencies as injected callables (`TwinDeps`) so
  they don't import bot/mqtt/webxdc/publisher.

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

## PlugTwin (per-device digital twin)

`PlugTwin` lives in `plug.py`. One instance per `Device`. Owns:

- `fields: dict[str, Any]` — current state (online, output, apower, …)
- `last_update_ts: int`
- `rules: list[ScheduledJob]`
- `threshold_latches: dict[str, ThresholdLatch]`
- a `threading.Lock()` guarding all of the above

Side-effecting collaborators are injected as plain callables in
`TwinDeps` (`mqtt_publish`, `post_to_chats`, `broadcast`,
`save_rules`, `react`, `history`, `client_id`). The twin doesn't
import bot/mqtt/webxdc/publisher — keeps the dep graph a clean DAG.

### Methods

- `on_mqtt(suffix, payload)` (MQTT thread): extract → update fields →
  evaluate `chat_events` (on_change + threshold) → reset rule windows
  on a genuine output edge (F→T resets off-target rules' transient
  state, T→F resets on-target rules; None→T/F is bot-startup
  hydration and skipped) → tick state-based rules (idle / consumed)
  → write history → call `broadcast(name)` if any state edge fired.
- `dispatch(action, source_msgid?)` (DC handler thread): validate
  action → publish → react 🆗 → broadcast. Pending rules survive
  manual toggles; explicit removal goes through `cancel`.
- `schedule(target_action, policy, chat_origin)`: build ScheduledJob;
  replace any rule with the same `(target_action, rule_id)`; save +
  broadcast.
- `cancel(target_action?, rule_id?)`: filter+remove rules; save +
  broadcast if any removed.
- `tick_time(now)` (sweeper thread): fire rules whose deadline has
  elapsed; skip-but-re-arm dormant rules; drop one-shots; save +
  broadcast.
- `next_deadline()`: smallest pending deadline; sweeper uses it.
- `to_dict()`: per-device payload included in the outbound snapshot.
- `can_chat_see(chat_id, allowed_chats)`: permission gate.

### Chat-event rules (defined per class in `class.json`)

- **on_change**: post a chat message when a state field transitions
  (`prev != new`). Boolean values are coerced to `"true"`/`"false"`
  for template lookup. Unknown value → no template → silent.
- **threshold**: per-field latch. Above limit for ≥ duration → fires
  `above`. Drop below → fires `below` and resets. Limit + duration
  come from the *device*'s `params`; if omitted the rule is silently
  disabled for that device.

### State edges that broadcast

Every chat-event fire triggers `deps.broadcast(name)`. Plus:
schedule/cancel/tick_time each broadcast on success. Power-metric
noise (apower wiggling without crossing a threshold) does NOT
broadcast — handled by the periodic timer instead.

### State-based dormancy

A rule whose target state already matches the device's `output`
field is dormant: it does not fire on tick (state-based or
time-based) and its transient counters are reset. `off`-rule on a
plug that's already off → silent. When the plug flips back, the
counter starts fresh.

## Rules subsystem (`rules.py`)

### Policies (any subset can be active per rule)

- **timer:** `deadline_ts = now + timer_seconds`, one-shot or
  recurring (default).
- **time_of_day (`tod`):** next HH:MM in local time. Uses
  `time.mktime` with `tm_isdst=-1` so DST transitions resolve via
  calendar dates rather than `+86400` arithmetic.
- **idle:** `state[idle_field] < idle_threshold` for ≥
  `idle_duration_s`. Evaluated inline by `twin.on_mqtt` after every
  state update.
- **consumed:** `integral(field·dt)` over last `window_s` <
  `threshold_wh`. Evaluated inline by `twin.on_mqtt`. Only fires
  after the window has been populated (now ≥ `_consumed_started_at +
  window_s`); zero samples ≠ "below threshold".

Time-based policies are mutually exclusive within one rule (timer
XOR tod). Idle and consumed can each appear at most once. Policies
combine OR-wise — whichever fires first wins.

### `RulesSweeper`

Single daemon thread. Each iteration:

1. Compute `min(twin.next_deadline() for twin in registry.all())`.
2. Sleep until that timestamp, or until `wake()` is called (e.g. on
   schedule/cancel).
3. Call `twin.tick_time(now)` on every twin.

The sweeper is not in the MQTT path — state-based rules are
evaluated inline in `twin.on_mqtt`.

### `once` flag — fire-and-delete vs. recurring

Default since v0.1.5: rules persist across fires. `once: True` opts
into one-shot. Recurring rules re-arm per policy: TOD → next
occurrence; timer → `now + timer_seconds`; idle → `_below_since`
reset; consumed → samples cleared, window restart.

### Persistence

`rules.py` writes a flat list of `ScheduledJob` dicts to
`~/.config/<BOT_NAME>/rules.json` atomically (`.tmp` + `os.replace`).
Each `ScheduledJob` carries its `device_name`; load dispatches each
to the matching twin via `twin.add_persisted_rule(...)`.

On startup, expired one-shots are dropped, recurring TODs re-arm to
the next occurrence, and `bot._rehydrate_rules_from_history()`
backfills consumed/idle evaluation buffers from `power_minute` so
restart doesn't force a fresh window.

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
verbs) or treats it as a schedule keyword (`auto-off`/`auto-on`/
`cancel-*`/`refresh`). Optional `auto_off` / `auto_on` keys carry an
inline policy object that gets assembled into a `ScheduledPolicy`
after the direct action runs. Keys recognised inside `auto_off`/
`auto_on`: `timer_seconds` (int), `time_of_day` ([h,m] list),
`recurring_tod` (bool), `idle` (object with `field`/`threshold`/
`duration_s`), `consumed` (object with `field`/`threshold_wh`/
`window_s`), `once` (bool). Any subset → OR-combined.

`refresh` is class-scoped (no `device` field needed): the bot
resolves the class from the requesting msgid and replies with a
`push_unicast` snapshot.

Legacy actions (`history`, `events`, `set_param`) are silently
dropped post-v0.2 — users `/apps` to upgrade to a build that no
longer emits them.

### bot → app (single message kind)

The snapshot is the only thing the bot pushes. It lives at the top
level of `payload` — no wrapping `snapshot:` key:

```json
{"payload": {
   "class": "shelly_plug",
   "server_ts": 1714000000,
   "devices": {
     "kitchen": {
       "name": "kitchen",
       "description": "Kitchen counter",
       "fields": {"online": true, "output": false, "apower": 0.0, ...},
       "last_update_ts": 1714000000,
       "energy": { "kwh_last_hour": ..., "current_total_wh": ... },
       "daily_energy_wh": [[ts, wh] ... 30],
       "scheduled_jobs": [{"target_action": "off", "deadline_ts": ...}],
       "params": { "power_threshold_watts": 1500, ... },
       "power_history": {
         "minute": [[ts, w, 1|0|null] ... 1440],
         "hour":   [[ts, w, 1|0|null] ...  744]
       }
     }
   }
}}
```

Pushed on (a) state edges, (b) periodic timer
(`PUBLISH_INTERVAL_S`, default 300 s), (c) refresh button,
(d) `/apps` onboarding. The app caches the latest snapshot in
`localStorage` and renders all chart windows from it — no on-demand
fetches. `output` in `power_history` is `1|0|null`; `null` means the
plug had no `power_minute` row for that bucket → app paints grey
(offline).

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

`tests/` directory, stdlib `unittest`, one file per module under
test (`tests/test_durations.py`, `tests/test_history.py`, …). 130
tests in ~5 s. Shared fixtures live in `tests/_fixtures.py`
(``CLASS_JSON_OK``, ``_build_twin``, ``_FakeHistory``); the
``tests/__init__.py`` package init handles ``sys.path`` setup
and stubs ``deltachat2`` so production modules import without
the real package installed.

Run with `python3 -m unittest discover tests`. Coverage: durations parser,
templating regex (incl. JSON brace passthrough), state extraction
(bool_text + json_path edges), permissions (global + per-device +
fallback), scheduler.parse_policy (every form + restricted kinds),
scheduler.integrate_wh, scheduler.next_tod_deadline, scheduler.tick
(idle fire + reset), config loader (every error path), engine
(unknown device, permission denied, unknown action, publish, template
substitution including JSON, dispatch preserves pending rules, threshold
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
  bot.py, every `mqtt_bot.*` log call (plug, rules, mqtt, history,
  publisher, snapshot, baselines, etc.) is silently dropped. We hit
  this trying to debug the empty-picker bug.

## History (SQLite time series)

`history.py` owns a single SQLite file at
`~/.config/<BOT_NAME>/history.sqlite` with three live tables:

- `samples_raw(device, ts, apower_w, voltage_v, current_a, freq_hz,
  aenergy_total_wh, output, temperature_c, payload_json)` — every
  status/switch:0 captured verbatim. `aenergy_total_wh` is the RAW
  plug counter (untouched by offset adjustment). This is the
  single source of truth for energy queries.
- `power_minute(device, ts, avg_apower_w, sample_count, output,
  max_apower_w, min_apower_w)` — one row per minute, derived
  from the apower stream in samples_raw. `avg_apower_w` is the
  mean over that minute's samples; `max_apower_w` is the peak
  (added v0.2.2); `min_apower_w` is the trough (added v0.2.6).
  Both legacy rows are backfilled idempotently from samples_raw
  on startup. Used by the app's power chart and by rule
  rehydration. `output` is the relay state (0/1/NULL).
- `aenergy_offset_events(device, ts, delta_wh)` — append-only log
  of detected hardware counter resets. Each row's delta_wh is
  added to every `aenergy_at(T)` lookup where `T >= ts`. Forever-
  retained. Typically empty (no reset events ever observed).

Energy queries: `aenergy_at(device, T) = raw_at_or_before(T) + Σ
delta_wh from aenergy_offset_events WHERE ts ≤ T`. Both lookups
hit O(log N) indexes; the offset SUM is over a tiny table. For
plugs that never have a hardware reset the math collapses to a
straight subtraction of two raw counter readings.

`PlugTwin.on_mqtt` writes apower to power_minute (via
`write_sample`) and the full status payload to samples_raw (via
`record_status`). When it detects the plug's `aenergy.total`
going backwards, it appends to aenergy_offset_events.

The SIGTERM handler in `bot.py` calls
`history.flush_pending_minutes` so an interrupted minute isn't
lost.

`RETENTION_DAYS` env var: `0` means keep forever, `>0` prunes
samples_raw + power_minute older than N days, evaluated once per
day on the next write. aenergy_offset_events is exempt (forever).

(Earlier versions had `events`, `energy_minute`, `aenergy_minute`,
and `energy_hour` tables; all dropped from the code. Existing
rows in users' SQLite files are dead data the bot never reads.)

## Snapshot energy + history blocks

Each device payload includes:

- `energy: {kwh_last_hour, kwh_today, kwh_last_24h, kwh_this_week,
  kwh_last_7d, kwh_this_month, kwh_last_30d, current_total_wh}` —
  each kWh entry is `{kwh, partial_since_ts}`; `partial_since_ts` is
  set when our oldest sample arrived noticeably later than the
  requested start (app marks a `*` suffix).
- `daily_energy_wh: [[ts, wh] * 30]` — last 30 days of daily totals
  for the bar chart.
- `power_history: {minute, hour, day}` — three pre-aggregated
  series the app picks between based on the chart window. Each
  entry is `[ts, min_w, max_w, avg_w, output]`. **Unified
  rendering across all three series:**
  - **on bucket** (`output=1`): grey vertical bar from `min_w` to
    `max_w` (the activity range) plus a green dot at `avg_w`.
  - **off bucket** (`output=0` for the whole bucket): red dot at
    the chart bottom.
  - **offline / no data** (`output=null`): nothing drawn at that
    tick — the connecting line breaks across the gap.
  - A thin grey line connects every consecutive pair of dots
    (green→green, green→red, red→green, red→red), forming a
    continuous trace except across offline gaps.
  - Live trailing point on the minute series renders as a single
    green dot at the live `apower`; min == max == avg, so the
    bar collapses to a single tick.

  Window → series mapping:
  - `≤24 h` → `minute` (with live tail at "now")
  - `7 d`, `31 d` → `hour`
  - `365 d` → `day`

  Header always reads `max X W · avg Y W` from the visible
  points. Buckets without a `power_minute` row gap-fill as
  `[ts, 0, 0, 0, null]`. Energy panels (`kWh consumed in last X`)
  always use avg-based integration — not max — so they don't
  overstate consumption.

## Chat command additions

- `/<device> export 7d` — bot dumps power_minute + samples_raw for the
  window to a CSV and sends as a chat attachment. Window accepts
  `s/m/h/d` (the d unit was added for this).

## Provenance and history

Only `v0.1.5` and `v0.2.1` correspond to actual git tags. Other
labels in older entries are historical descriptive markers, not
released versions; treat them as part of the prose. Going forward,
provenance entries reference work by date and (where helpful) the
short SHA — no invented version labels. Tags are reserved for
stable major releases the user creates by hand.

- 2026-05-09 **chart fidelity + rule fidelity + repo cleanup**
  (commits `3938c14..fd414a2`, post-`v0.2.1`).

  *Power chart fidelity.* `power_minute` gains `max_apower_w` and
  `min_apower_w` columns (idempotent migrations + backfill from
  samples_raw). Snapshot's `power_history` entry shape becomes
  `[ts, min_w, max_w, avg_w, output]`; gap-filled buckets emit
  `[ts, 0, 0, 0, null]`. The app renders every chart window with
  the same primitives: a grey min..max bar per "on" bucket, a
  green dot at avg, a red dot at the chart bottom for "off"
  buckets, and a thin grey line connecting every consecutive pair
  of dots (broken across offline gaps). At dense windows (12 h,
  24 h, 31 d) the rendering converges to a continuous line; at
  sparse windows (1 h, 7 d, 365 d) the dots and bars are
  individually legible. Window picker grows "7 days" and "365
  days" options; "6 h" dropped (no info gain over 12 h).
  Resolution per window: minute (≤24 h, with live trailing
  apower point), hour (7 d & 31 d), day (365 d). Energy panels
  (`kWh consumed in last X`) keep avg-based integration so they
  don't overstate. Fixes the "chart says 800 W but the idle rule
  never fires" mystery for cycling-load devices like espresso
  machines.

  *Rule fidelity.* Idle-rule rehydration on bot restart now reads
  `samples_raw` (not `power_minute` averages), so cycling-load
  devices no longer have their off-rule spuriously armed. Manual
  ON resets the transient state of every off-targeted rule on the
  twin (and symmetrically, manual OFF resets on-targeted rules),
  so a "off if idle for 30 min" rule always leaves a fresh
  30-min window after a manual flip. None→True bot-startup
  hydration is excluded — only genuine user/device edges trigger
  the reset. Each rule tracks `_observation_started_at` and the
  snapshot exposes `current_window_s = min(now - obs_start,
  full_window)`, so a freshly-created rule's chat label reads
  `8.30Wh in 23m` (or `1m` for a fresh rule) instead of always
  the full window. Per-rule chat display drops the redundant
  `consumed:5Wh:600s` rule-id span and shows live observed values
  computed from each rule's `_samples` deque (consumed) or a
  `samples_raw` query (idle).

  *Repo cleanup.* The 14 first-party Python modules at the
  project root collapse into a single `mqtt_bot/` package with
  three semantic sub-packages mirroring the design-rule split:
  `mqtt_bot/core/` (twin engine + rules + state + snapshot),
  `mqtt_bot/io/` (history, baselines, mqtt_client, publisher,
  webxdc_io — anything with side effects), `mqtt_bot/util/`
  (pure utilities: config, durations, permissions, templating).
  `plug.py` renamed to `mqtt_bot/core/twin.py` (the class stays
  `PlugTwin`; only the file is renamed for honesty since the
  engine has been class-agnostic since v0.2.0). Three pure pieces
  extracted from bot.py into `mqtt_bot/{commands,formatters,
  rehydrate}.py` (parser + display + startup rehydration). The
  2,096-line `test_mqtt_bot.py` becomes a `tests/` package with
  one file per module under test, run via `python3 -m unittest
  discover tests`. Architectural rules unchanged; no production
  logic touched. 130 tests.
- 2026-05-08 **v0.2.1 — energy-storage simplification + quality**.
  Same-day follow-up to v0.2.0. Drops `aenergy_minute` and
  `energy_hour` tables from the code (rows in users' SQLite stay
  forever as dead data; no DROP TABLE). `samples_raw` becomes the
  single source of truth for energy queries. Effective aenergy at
  any T = `raw_at_or_before(T) + Σ delta_wh from
  aenergy_offset_events WHERE ts ≤ T` — two index seeks plus a
  tiny SUM over a forever-tiny offset-events table. For users
  who never have a hardware counter reset (the typical case),
  the math collapses to a straight subtraction of two cumulative
  counter readings. Counter-reset offsets moved from in-memory
  twin state + baselines.json to history.aenergy_offset_events
  (queryable, persistent, append-only). Two retention knobs
  collapsed back to one (`RETENTION_DAYS` covers samples_raw +
  power_minute; aenergy_offset_events is forever). Earlier
  symptom — "Last 365 days > Plug Lifetime" — became
  unrepresentable by construction. Added: cold-start
  integration test, baselines.json round-trip + legacy-offset
  migration test, per-twin counter-reset detection test
  (writes-to-history). Extracted `baselines.py` so persistence
  logic is unit-testable without bot.py globals. Added
  `.github/workflows/ci.yml` (test + check-config + xdc build +
  guards on engine.py / scheduler.py reappearing). 118 tests.
- 2026-05-08 **digital-twin refactor**. `engine.py` and
  `scheduler.py` deleted. New: `plug.py` (PlugTwin per device),
  `twins.py` (TwinRegistry), `snapshot.py` (single
  `build_for_chat`), `publisher.py` (single outbound stream),
  `rules.py` (renamed scheduler; slimmed to sweeper + parser +
  persistence). All per-device state lives on the twin.
  Publisher pushes on (a) state edges, (b) periodic
  `PUBLISH_INTERVAL_S`, (c) refresh button, (d) `/apps`. App
  becomes standalone after a snapshot — window switching is
  render-only. Three-color chart (green/red/grey) reuses the
  existing `output: 1|0|null` convention. Symmetric propagation:
  external toggles (other chat user, second app, physical button)
  all show up via the same MQTT echo path. Dropped: `set_param`
  + Tuning UI; `events` SQLite table + Recent-events twistie;
  `history`/`events`/`set_param` webxdc actions; live-5min chart
  window. Net: roughly −500 lines across the repo. 84 tests.
- 2026-05-07 **v0.1.5 — rules persist forever by default; opt-in
  `once` for fire-and-delete** (real git tag).
  (`shelly_plug`), one webxdc app, full deployment scripts.
- 2026-05-07 SQLite-backed history. Added: SQLite-backed
  history (per-minute power, hourly energy, events table) with
  RETENTION_DAYS knob; segmented red/green chart with a 0-line for
  off periods; energy summary (last hour through lifetime); per-bucket
  kWh bars; recent-events viewer; in-memory power-threshold tuning
  from the app; `/<dev> export Nd` CSV chat command; SIGTERM
  shutdown hook to flush the in-minute buffer; basicConfig logging
  fix; the `<script src=webxdc.js>` fix; `/help` and `/id` bypass the
  allow-list; `/help` auto-posts on member-add. ~79 tests.
- 2026-05-07 parallel scheduled rules. Parallel scheduled rules per
  (device, target_action), keyed by stable rule_id derived from
  policy contents. Each rule fires independently; redundant fires
  are intentionally posted to chat (audit trail > silence). New
  trigger templates use `{action_verb}`, `{threshold}`,
  `{duration_human}`, `{window_human}` so messages spell out which
  rule tripped and why. App grows two `<details>` sections (Auto-off
  rules / Auto-on rules) with × delete buttons + Add-rule forms.
- 2026-05-07 maximalist SQLite capture. Maximalist data capture into SQLite:
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
- 2026-05-07 recurring rules by default. **Rules are recurring by default.**
  Schedule a rule once and it fires every time its condition is met
  again, until you cancel it. Opt-in `once` flag (chat keyword and
  app checkbox) preserves the old fire-and-delete behaviour.
  State-aware dormancy: an off-rule does nothing while the plug is
  already off, an on-rule does nothing while it's already on, so
  no spurious chat messages and no wasted CPU. Re-arm logic per
  policy: TOD → next occurrence, timer → `now + timer_seconds`,
  idle/consumed → reset transient counters. Migration of pre-v1.5
  rules.json: missing `once` field is inferred from `recurring_tod`
  so existing rules' behaviour is preserved across the upgrade.
  ScheduledJob gains `once` and `timer_seconds`; ScheduledPolicy
  gains `once`. parse_policy strips a `once` keyword from anywhere
  in the clause. Engine surfaces `once` in the snapshot; /rules
  marks one-shot rules with `(once)`. 110 tests.
- 2026-05-07 rules.json persistence. Rules persist to
  `~/.config/<BOT_NAME>/rules.json`; load on startup re-arms recurring
  TODs and drops expired one-shots. **Rehydrate from history**: the
  engine backfills a consumed-rule's `_samples` deque (and an idle-rule's
  `_below_since`) from `power_minute` immediately after
  `scheduler.load_persisted()`, so a `systemctl restart` no longer
  forces every rule to wait a fresh window. New chat surface:
  `/<device> rules` and `/rules` (all visible devices), with single
  rules inline and OR-combined rules rendered as bulleted multi-line
  blocks. Webxdc handler routes `action="auto-off"` /
  `action="auto-on"` straight to the scheduler instead of through
  `dispatch_command` (the old code path errored "unknown action").
  Trigger templates now spell out the rule that fired:
  `{action_verb}` + `{threshold}` + `{duration_human}` /
  `{window_human}`. App: 1-hour interval added to the chart-window
  picker; window choice persists per-device via `localStorage`; chart
  no longer stretches sparse data — x-axis bounds use the bot's
  `since_ts`/`until_ts` (or now-5min..now in live mode). History
  response carries an authoritative `total_wh` from
  `energy_consumed_in` (so 1-hour windows no longer show 0.00 kWh
  from the old energy_hour-delta computation). 105 tests.

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

1. Header — device picker + online dot + "data Ns ago" timestamp +
   refresh button
2. State card — current ON/OFF + apower + aenergy
3. On / Off / Toggle buttons
4. Power chart (1h / 6h / 12h / 24h / 31d) with daily-energy bars
   below. Three colors: green = on, red = off, grey = offline. The
   smallest window is 1 h (live 5 min was dropped in v0.2; the
   header timestamp is the real-time indicator instead).
5. Energy consumed (8-row grid)
6. Auto-off rules (open by default — most-used surface)
7. Auto-on rules

The "Recent events" twistie and "Tuning · power threshold" section
were removed in v0.2. Per-device threshold tuning is now via
`devices.json` + restart; chat events appear in chat as before.

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
