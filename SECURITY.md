# Security model

This is a small home-automation bot intended to run on a private LAN
with a known set of authorised Delta Chat conversations. It is not
hardened for hostile multi-tenant deployment. The model below is what
you should reasonably expect from it.

## Trust boundaries

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   Authorised chats     ──────►   bot.py routing layer            │
│   (Delta Chat IDs in           │   (text + webxdc requests)      │
│   ALLOWED_CHATS)               ▼                                 │
│                            PlugTwin.dispatch / .schedule         │
│                                ▼                                 │
│                            mqtt_client.publish ──────►   plugs   │
│                                                       (LAN MQTT) │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                ▲
                │ Delta Chat sync over IMAP/SMTP
                │ (TLS, end-to-end encrypted by deltachat2)
                ▼
        Authorised users' devices
```

The bot trusts:
- Anyone who can post in an authorised chat (allow-listed by chat id)
- The MQTT broker on `MQTT_HOST:MQTT_PORT` with `MQTT_USER`/`MQTT_PASS`
- Files under `~/.config/<BOT_NAME>/` (rules.json, app_msgids.json,
  history.sqlite)

The bot does NOT trust:
- Unauthorised chats — every command except `/id` and `/help` is
  rejected with "permission denied"
- Webxdc updates from msgids it never registered — silently dropped
- Replayed messages outside the allowed time windows (see below)

## Permissions — two layers

### Global gate: `ALLOWED_CHATS`

Comma-separated chat IDs in `.env/env`. Empty list → bot rejects every
command except `/id` (read-only, needed for setup). The bot logs a
warning on startup if `ALLOWED_CHATS` is empty.

### Per-device: `allowed_chats` in `devices.json`

Each device entry may list specific chat IDs. If omitted, the device
falls back to the global `ALLOWED_CHATS`.

Both checks happen in `permissions.chat_can_see`, called from text
dispatch, webxdc request handling, snapshot assembly, `/apps`, `/all`,
`/refresh` and CSV export.

**One known second copy:** `bot.py:_post_to_visible_chats` — the path
for threshold alerts, offline notices and rule-fire messages — derives
its recipients inline as `device.allowed_chats or ALLOWED_CHATS`
instead of calling the predicate. The two agree today; they will not
the moment anyone adds a deny-list, a per-device `enabled` flag, or a
mute. Consolidating it is queued.

### `/id` and `/help`

Both bypass the allow-list:
- `/id` — read-only; needed to bootstrap a new chat into
  `ALLOWED_CHATS`. Returns the chat's numeric id and nothing else.
- `/help` — read-only; lists devices the calling chat can see (so
  empty for unauthorised chats — no leakage).

## Replay protection

Three windows, all enforced in `bot.py`:

| Window | Constant | Default | Why |
|---|---|---|---|
| Typed text command | `MAX_AGE_SECONDS` | 200 s | Lenient — typed commands may sit in queue during network outages |
| Webxdc app request | `MAX_APP_AGE_SECONDS` | 45 s | Tighter — taps are usually intentional and immediate |
| Future-dated skew | `MAX_CLOCK_SKEW_SECONDS` | 30 s | Tolerates NTP drift / sender clock ahead |

Messages outside these windows get a `❌` reaction (text path) or a
drop (webxdc path), with a WARNING naming the device and action —
a stale tap and a replay attempt are otherwise indistinguishable.

The `ts` field on app requests **is required and enforced**: a missing,
null, string or boolean `ts` is rejected outright by
`commands.check_freshness`. Until 2026-07-28 the webxdc path only
applied the window when `ts` happened to be numeric and fell through
to execute the request otherwise, so any request without a usable `ts`
switched a relay with no freshness bound and no log line. That gap
contradicted this document; it is closed and covered by tests.

## Webxdc msgid registry

Apps "subscribe" implicitly by being registered in
`~/.config/<BOT_NAME>/app_msgids.json` (`{chat_id: {class:
msgid}}`). Updates from msgids the bot has never recorded are dropped
with a log line — points the user at `/apps` to refresh.

Old app instances in chats keep emitting requests forever; the bot
silently drops anything that isn't in its action whitelist (see
`_KNOWN` in `bot.py:handle_webxdc_request`). This means an old app
build won't crash the bot or leak state.

## Outbound surface

The bot makes outbound network calls only:

| To | What | Auth |
|---|---|---|
| `MQTT_HOST:MQTT_PORT` | Publish device commands; subscribe to status | Username + password (`MQTT_USER` / `MQTT_PASS`) |
| Delta Chat IMAP/SMTP | Send + receive chat messages | Delta Chat profile (encrypted backup tar imported via `init-from-backup.sh`) |

No telemetry, no auto-update checks, no third-party services.

## Local file surface

| Path | Contents | Sensitivity |
|---|---|---|
| `.env/env` | Bot identity + MQTT creds + ALLOWED_CHATS | Secrets — gitignored |
| `.env/<dump>.tar` | Delta Chat profile backup | Secrets — gitignored. NOTE: a Delta Chat backup tar is only encrypted if you set a backup passphrase; nothing here verifies that. Treat as plaintext credentials unless you know otherwise. |
| `devices.json` | Device names, MQTT topic prefixes, threshold params | Configuration |
| `~/.config/<BOT_NAME>/rules.json` | Pending auto-off / auto-on rules | Configuration |
| `~/.config/<BOT_NAME>/history.sqlite` | Per-minute power + per-hour energy | User-level metering data |
| `~/.config/<BOT_NAME>/app_msgids.json` | Which webxdc msgid is registered per chat per class | Internal state |

The `devices.json` file may contain enough info to identify a home's
plug topology (device names, MQTT topic prefixes). Treat it as you
would your other home-automation config — keep it private if your
threat model includes someone with access to the bot host but not
your chat.

## Known limitations

- **No rate limiting on chat output.** A flapping plug produces one
  on/off message per state change. Acceptable for v0.2.
- **No TLS to the MQTT broker, and no topic ACLs.**
  `setup-mosquitto.sh` writes `listener 1883 0.0.0.0` with
  `allow_anonymous false` and a password file — so the listener DOES
  face the LAN (an earlier version of this document claimed the
  opposite), credentials cross it in cleartext on every plug
  reconnect, and the single account is shared between the bot and the
  plugs. With no `acl_file`, any client holding that credential can
  publish straight to a command topic, bypassing `ALLOWED_CHATS`
  entirely, and can publish a forged `status/switch:0` that drives
  twin state, chat events, `samples_raw`, and rule evaluation.
  Binding the listener to `127.0.0.1` only works if the plugs reach
  the broker some other way; the real fixes are per-role credentials
  plus topic ACLs, and TLS if the LAN is not trusted.
- **Bot restart drops unflushed state in flight.** History buffers
  are flushed on SIGTERM; webxdc updates being sent at exit may be
  lost (deltachat2 will retry on next start).
- **The bot doesn't authenticate Shelly plugs.** The MQTT broker auth
  is the only check — any host that can authenticate as `MQTT_USER`
  can pretend to be a plug. On a trusted LAN this is fine; if you
  expose Mosquitto, use TLS + per-device certificates.

## Reporting issues

Open a GitHub issue, or for sensitive disclosure, email the address
in `git log --format='%ae' | head -1`.
