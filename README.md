# mqtt-bot

Delta Chat ↔ MQTT bridge. Lets you monitor and control devices over MQTT
from a Delta Chat conversation.

The chat-side mapping (text command ↔ MQTT publish, MQTT subscribe ↔
chat message) is **declarative**: each device class is a self-contained
component under `devices/<class>/` with its own `class.json` (what to
subscribe to, what to publish, which fields to extract, what to say in
chat, what auto-off/auto-on policies to support) and its own webxdc app.

Ships with one class — `shelly_plug` — covering Shelly plugs that speak
the JSON-RPC MQTT API (Plus Plug S Gen 2, Plug M Gen 3, etc.). Adding a
second device type (Tasmota, generic relay, sensor, …) is a matter of
dropping in a new `devices/<class>/` directory, no Python edits.

## Layout

```
mqtt-bot/
├── bot.py                         # Delta Chat hooks + routing glue + construction
├── plug.py                        # PlugTwin — digital twin per device
├── twins.py                       # TwinRegistry — dict + reverse topic lookup
├── rules.py                       # ScheduledJob/Policy + parse_policy + sweeper + persistence
├── snapshot.py                    # build_for_chat — single outbound assembly
├── publisher.py                   # Publisher — single outbound stream
├── config.py                      # devices.json + devices/*/class.json loader
├── state.py                       # state-field extraction (json_path, bool_text)
├── permissions.py                 # global ALLOWED_CHATS + per-device allow-list
├── mqtt_client.py                 # paho wrapper (daemon thread, auto-resubscribe)
├── webxdc_io.py                   # app_msgids.json + send_apps + push_to_msgid
├── history.py                     # SQLite time series (samples_raw, power_minute, energy_*)
├── durations.py                   # parse "30m" / "1h30m"
├── templating.py                  # {key} substitution that leaves JSON braces alone
├── devices/                       # device-class components (auto-discovered)
│   └── shelly_plug/
│       ├── class.json             # the class definition
│       ├── app/                   # webxdc source
│       ├── shelly_plug.xdc        # built artifact (gitignored)
│       └── README.md
├── devices.json                   # YOUR device instances (kitchen, heater, ...)
├── env.example                    # env-file template
├── .env/                          # local env files + Delta Chat backup tar (gitignored)
│   └── env                        # MQTT creds + bot identity (gitignored)
├── start-mqtt-bot.sh              # systemd entrypoint (sources .env/env, exec python -m bot)
├── lib/common.sh                  # shared shell helpers
├── build-xdc.sh                   # zip devices/<class>/app → devices/<class>/<class>.xdc
├── setup-mosquitto.sh             # apt-installs Mosquitto, drops a config, sets bot password
├── init-from-backup.sh            # one-shot: import a Delta Chat profile tar from .env/
├── install-systemd-unit.sh        # render+enable+start the systemd unit
├── systemd-unit/deltabot.service.template
└── tests/                         # stdlib unittest, 130 tests
```

## Chat commands

```
/<device> on | off | toggle | status
/<device> on for 30m              # on now + auto-off in 30 min
/<device> on until idle           # on now + auto-off when apower<5W for 60s (defaults)
/<device> on until idle 10W 120s  # on now + auto-off (custom thresholds)
/<device> on until used <5Wh in 10m
/<device> on for 1h or until idle
/<device> auto-off in 30m         # schedule auto-off, no immediate toggle
/<device> auto-off at 18h         # next 18:00 local
/<device> auto-off at 18:30 daily # recurring daily
/<device> auto-on at 7h           # next 07:00 local
/<device> auto-on at 7h daily     # recurring
/<device> cancel-auto-off | cancel-auto-on | cancel-schedule
/<device> export 7d               # CSV (samples_raw + power_minute)
/<device> rules                   # list this device's pending auto-off / auto-on rules

/all on | off | toggle            # act on every device visible to this chat
/rules          list pending rules across every visible device
/list           list devices visible to this chat
/apps           (re)deliver webxdc control apps
/id             show this chat's id (permission-free, needed for setup)
/help           command reference (permission-free)
```

Scheduling clauses tolerate verbose forms: `30min`, `30 min`, `30 minutes`,
`1 hour`, `1hr 30min`, `1 day`, `200 Wh in 30 min`, `5 W in 60 sec`. Both
`if` and `until` are accepted as keywords. Multiple rules per device per
direction coexist and fire independently — adding `/<dev> off if idle 5W
60s` AND `/<dev> off in 30m` schedules two parallel rules.

## History (SQLite time series)

The bot persists per-device time series under
`~/.config/<BOT_NAME>/history.sqlite`. This drives:

- **Webxdc app charts** — line chart over the selected window
  (1h / 6h / 12h / 24h / 31d). Three colors: green = on,
  red = off, **grey = offline** (no `power_minute` row for that
  bucket — gap-filled by the snapshot builder).
- **30-day daily-energy bars** below the line chart.
- **Energy summary** — last hour, today, last 24h, this week, last 7d,
  this month, last 30d, lifetime.
- **`/<device> export Nd`** — dumps the time series tables as CSV.

`RETENTION_DAYS` env var: `0` (default) keeps forever; `>0` prunes
older rows once per day. Storage is small — ~3 MB per device per
month at 1 sample/min plus ~70 KB per device per month for hourly
snapshots.

## Recommended plug configuration

In each Shelly plug's web UI:

| Setting | Recommended | Why |
|---|---|---|
| MQTT enabled | yes | obvious |
| Server | `<bot host>:1883` | the broker we set up |
| MQTT username/password | `MQTT_USER`/`MQTT_PASS` from `.env/env` | broker auth |
| Custom MQTT topic prefix | `shellyplug-<name>` | becomes `topic_prefix` in `devices.json` |
| **Generic status update over MQTT** | **enabled** | required — without this we get only the LWT, no `status/switch:0` flow |
| **Status update period** | **15 s** | 4× the default detail in the live `apower` curve and `samples_raw` table; still trivially small MQTT/disk overhead. The authoritative `aenergy.by_minute` data is unaffected (always per-minute). |

Lower than 15 s is fine if you want sub-15 s power resolution, but
remember every status update writes one row to `samples_raw`. At 1 s
that's ~63 MB / month / device.

## History data model (SQLite)

`~/.config/<BOT_NAME>/history.sqlite` — three live tables, all
keyed by `(device, ts)`:

| Table | What | Source |
|---|---|---|
| `samples_raw` | Every `status/switch:0` verbatim — apower, voltage, current, freq, **aenergy.total** (RAW), output, temperature.tC, plus a `payload_json` blob for the rest | one row per status update |
| `power_minute` | Per-minute average `apower` (W) | aggregated client-side from samples |
| `aenergy_offset_events` | Append-only log of detected hardware counter resets; `delta_wh` per row | written by PlugTwin.on_mqtt when the plug's aenergy.total goes backwards |

Energy queries: `aenergy_at(T) = raw_at_or_before(T) + Σ delta_wh
from aenergy_offset_events WHERE ts ≤ T`. "kWh in [a, b]" is
`aenergy_at(b) - aenergy_at(a)` — two index seeks plus a tiny
SUM. For plugs that never have a hardware reset, the offset SUM
is over zero rows and the math is a straight subtraction.

## Persistence + restart recovery

Two pieces of state survive `systemctl restart`:

| File | Holds |
|---|---|
| `~/.config/<BOT_NAME>/rules.json` | every pending auto-off / auto-on rule |
| `~/.config/<BOT_NAME>/history.sqlite` | the three tables above |
| `~/.config/<BOT_NAME>/app_msgids.json` | which webxdc msgid is registered per chat per device class |

On startup `rules.load_into(registry, ...)`:

- **drops one-shot rules whose deadline elapsed during downtime** (firing
  retroactively would surprise you),
- **re-arms recurring time-of-day rules** to their next future occurrence,
- **rehydrates consumed-rule sample buffers and idle-rule below-since
  timestamps from `power_minute`** (via `bot._rehydrate_rules_from_history`),
  so a rule like "off when used <5Wh in 10m" doesn't have to wait a
  fresh 10-minute window before it can fire — if the actual last 10
  minutes (in the database) already meet the condition, it fires on
  the next status update.

Threshold tuning is via `devices.json` + `systemctl restart` (the
in-app Tuning UI was removed in v0.2 to keep the surface area small).

Multi-clause example: `/kitchen on for 1h or until used <2Wh in 10m`
turns on, then off whichever fires first — a hard 1-hour cap *or* the
device staying near-idle for 10 min.

## Permissions

Two layers:

1. **Global gate** — `ALLOWED_CHATS` in `.env/env`. Every command except
   `/id` requires the chat to be in this list.
2. **Per-device** — `allowed_chats` per entry in `devices.json`. If
   omitted, falls back to the global list.

Visibility propagates everywhere: text replies, webxdc app payloads,
online/offline alerts, threshold alerts, auto-off trigger messages.

## First-time setup

### 1. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install deltachat2 deltabot-cli deltachat-rpc-server paho-mqtt appdirs
```

(`start-mqtt-bot.sh` also creates the venv and installs deps if missing.)

### 2. Set up Mosquitto

```bash
sudo ./setup-mosquitto.sh        # reads MQTT_USER, MQTT_PASS from .env/env
```

The plugs will connect to this broker over the LAN. Configure each plug
in its web UI: enable MQTT, point it at the bot host's IP and port,
set username/password to match `.env/env`. Confirm via:

```bash
mosquitto_sub -u <user> -P '<pass>' -t '#' -v
```

You should see periodic `<prefix>/online true` and `<prefix>/status/switch:0`
JSON appearing.

### 3. Bot identity

Either:

- **Onboard manually**: run `./start-mqtt-bot.sh` once, follow the QR /
  invite-link instructions deltabot-cli prints to stdout.
- **Import a backup**: drop a Delta Chat profile tar (`.tar`, exported
  from another DC instance) into `.env/`, then:

```bash
./init-from-backup.sh
```

The script is idempotent — it skips if the account dir is already populated.

### 4. Configure devices

Copy `env.example` → `.env/env`, fill in `MQTT_USER`/`MQTT_PASS`/etc.
Edit `devices.json` to list your plugs:

```json
{
  "devices": [
    {
      "name": "kitchen",
      "class": "shelly_plug",
      "topic_prefix": "shellyplugmg3-XXXXXXXXXXXX",
      "description": "Kitchen counter",
      "allowed_chats": [],
      "power_threshold_watts": 1500,
      "power_threshold_duration_s": 30
    }
  ]
}
```

Validate:

```bash
python3 bot.py --check-config
```

### 5. Build the webxdc apps

```bash
./build-xdc.sh        # builds every devices/*/app
```

### 6. Discover your chat id

Start the bot:

```bash
./start-mqtt-bot.sh
```

Add the bot's account to a Delta Chat group, send `/id`, and copy the
returned id into `ALLOWED_CHATS` in `.env/env`.

### 7. Install as a systemd service

```bash
sudo ./install-systemd-unit.sh
```

This renders `deltabot-mqtt-bot.service`, enables it, and starts it.
Logs: `journalctl -u deltabot-mqtt-bot -f`.

### 8. Onboard your chat

In your authorised chat:

```
/apps              # delivers shelly_plug.xdc
/list              # confirms which devices you can see
/kitchen on        # try it!
```

## Adding a new device class

1. `cp -r devices/shelly_plug devices/<your_class>`
2. Edit `devices/<your_class>/class.json`:
   - rename `name`, `app_id`
   - replace `subscribe`, `commands`, `state_fields` to match your device's MQTT API
   - update `chat_events` rules and `auto_off`/`auto_on` blocks
3. Edit `devices/<your_class>/app/` (HTML/JS/CSS) for the UI you want
4. Add an instance to `devices.json` referencing the new class
5. `./build-xdc.sh && python3 bot.py --check-config && systemctl restart deltabot-<botname>`

No Python edits needed.

## Testing

```bash
python3 -m unittest discover tests # 130 unit tests (~5 s)
```

Coverage: `durations`, `templating`, `state` extraction, `permissions`,
`rules` (parse_policy, integrate_wh, next_tod_deadline), `config` loader
(validation paths), `PlugTwin` (on_mqtt edges + threshold + dispatch +
schedule + cancel + tick_time + dormancy + counter-reset detection),
`snapshot.build_for_chat`, `Publisher` (broadcast + push_unicast),
`TwinRegistry`, `History` (samples_raw + power_minute writes,
`aenergy_at` with offset-event SUM, `record_offset_event` idempotency,
`energy_consumed_in` straddling a reset event, baselines.json
legacy-offset migration), and a cold-start integration test.

CI: `.github/workflows/ci.yml` runs all of the above plus
`bot.py --check-config` and `./build-xdc.sh` on every push.

## Known limitations

- Bot restart re-arms recurring rules and drops expired one-shots, but
  events that occurred during downtime are not retroactively replayed.
- No live config reload — edit `devices.json` then `systemctl restart`.
- The app shows a stale `data Ns ago` timestamp until the next push or
  refresh; triggers (b)/(c) usually keep that under 5 min.

## Provenance

Detailed change log lives in `CLAUDE.md` under "Provenance and history."

Modeled on [`gatekeeper-bot`](../gatekeeper-bot) (Delta Chat ↔ BLE smart
lock). The bot framework, webxdc plumbing, `app_msgids.json` pattern,
and systemd template are derived from it; the digital-twin core
(`plug.py` + `twins.py` + `snapshot.py` + `publisher.py`) is new.
