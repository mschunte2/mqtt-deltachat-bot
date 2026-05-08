# tasmota_plug

Class definition for Tasmota smart plugs. Ships without an app of its
own: the engine + chat command surface alone are enough to use these
plugs from chat (`/<device> on`, `/<device> off`, `/rules`,
auto-off/auto-on rules, etc.).

The class exists primarily to validate the engine's class-agnostic
design — config.load discovers it the same way it discovers
`shelly_plug`, and `PlugTwin` handles tasmota status payloads with
zero Python code change.

## How Tasmota plugs talk MQTT

Tasmota uses the standard `tele/<topic>/...` (telemetry) +
`stat/<topic>/...` (state) + `cmnd/<topic>/...` (commands) split:

| Topic | Direction | Payload | What |
|---|---|---|---|
| `tele/<topic>/LWT` | inbound | `Online` / `Offline` (text) | last-will availability |
| `stat/<topic>/POWER` | inbound | `ON` / `OFF` (text) | relay state echo |
| `tele/<topic>/STATE` | inbound | JSON | periodic full-state |
| `tele/<topic>/SENSOR` | inbound | JSON with `ENERGY.Power`, `ENERGY.Total`, … | metering |
| `cmnd/<topic>/POWER` | outbound | `ON` / `OFF` / `TOGGLE` | switch the relay |
| `cmnd/<topic>/STATE` | outbound | (empty) | request full state |

In `devices.json`, set `topic_prefix` to your Tasmota `Topic` (e.g.
`tasmota_AB12CD`).

## Adding an app

This class doesn't ship an app. To add one:

1. `mkdir -p devices/tasmota_plug/app && cp -r ../shelly_plug/app/* .`
2. Edit `manifest.toml` so `name = "Tasmota Plug"`.
3. `./build-xdc.sh` from the repo root.
4. `/apps` in your authorised chat to deliver it.

The app is class-agnostic — it only reads the snapshot the bot pushes,
which has the same shape regardless of class.
