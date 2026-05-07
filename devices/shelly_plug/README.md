# shelly_plug

Self-contained device-class component for **Shelly plugs with the JSON-RPC
MQTT API** — covers Plus Plug S (Gen 2), Plug M (Gen 3), Plug US, and any
sibling plug that exposes the standard `switch:0` component over MQTT.

## What this directory contains

| File | Purpose |
|---|---|
| `class.json` | Class definition consumed by the engine (subscribe topics, commands, state fields, chat-event rules, auto-off/auto-on configs) |
| `app/` | Webxdc app source — vanilla HTML/JS/CSS, packaged by `../../build-xdc.sh devices/shelly_plug/app` into `shelly_plug.xdc` |
| `shelly_plug.xdc` | Built artifact — gitignored; rebuilt from `app/` |

## Adding a Shelly plug to your bot

1. In each plug's web UI: enable MQTT, point it at the bot's broker, set
   the username/password to match `MQTT_USER`/`MQTT_PASS` in `.env/env`.
   Note the topic prefix (default `shellyplugmg3-XXXXXXXXXXXX` on Plug M Gen 3).
2. In `../../devices.json`, add an entry:

   ```json
   {
     "name": "kitchen",
     "class": "shelly_plug",
     "topic_prefix": "shellyplugmg3-XXXXXXXXXXXX",
     "description": "Kitchen counter",
     "allowed_chats": [12345],
     "power_threshold_watts": 1500,
     "power_threshold_duration_s": 30
   }
   ```

3. Restart the bot. New devices show up in `/list` and via `/apps`.

## What's in class.json

- **subscribe**: which suffixes the bot listens to under `<topic_prefix>/`. Plug M Gen 3 publishes `online` (LWT), `status/switch:0` (JSON status, retained), and `events/rpc` (optional notifications).
- **commands**: chat verb → MQTT publish target. `on/off/toggle` go to `command/switch:0`; `status` is an RPC that asks the plug to push a fresh `status/switch:0`.
- **state_fields**: extraction recipes that turn payloads into named fields (`output`, `apower`, `aenergy`, etc.) for the state cache.
- **chat_events**: when the cache changes, which message gets posted to the visible chats. Three rules are configured: relay state transitions, online/offline transitions, and a sustained-power-threshold detector.
- **auto_off / auto_on**: which command verb to invoke when a scheduled action fires, plus the message templates and default idle/consumed parameters.

## Adding a new device class

Copy this directory, rename it (`devices/<your_class>/`), edit `class.json`,
write the `app/`. The engine auto-discovers any `devices/*/class.json`
at startup; no Python edits.
