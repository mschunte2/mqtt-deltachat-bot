"""Entry point: Delta Chat hooks + thin glue between modules.

All interesting logic lives in engine.py / scheduler.py / etc. This file
is just:
  - construct objects from env + config
  - register Delta Chat hooks
  - parse incoming text into structured commands
  - hand off to the engine

Run: ./start-mqtt-bot.sh   (or `python -m bot serve --logging info`)
Validate config without running: `python3 bot.py --check-config`
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Wire our package loggers (mqtt_bot.*) to stderr — deltabot-cli only
# attaches a handler to its own logger, so without basicConfig our
# engine/scheduler/mqtt_client log calls would be silently dropped.
_LOG_LEVEL = (os.environ.get("LOG_LEVEL") or "info").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)

import config as config_mod

# --- Lightweight startup (no deltachat2 / paho dependency) ---------------

BOT_NAME = (os.environ.get("BOT_NAME") or "").strip() or "mqtt-bot"
HERE = Path(__file__).resolve().parent
DEVICES_DIR = HERE / "devices"
DEVICES_FILE = Path(os.environ.get("DEVICES_FILE") or (HERE / "devices.json"))
ALLOWED_CHATS: set[int] = config_mod.parse_allowed_chats(os.environ.get("ALLOWED_CHATS"))

# Load config eagerly so config errors fail before we touch the venv.
cfg = config_mod.load(devices_dir=DEVICES_DIR, instances_file=DEVICES_FILE)

# --check-config exits before importing heavy deps so it can run on
# environments where the venv hasn't been built yet.
if "--check-config" in sys.argv:
    print(f"OK: {len(cfg.classes)} class(es), {len(cfg.devices)} device(s)")
    for cname, c in sorted(cfg.classes.items()):
        print(f"  class {cname}: app_id={c.app_id} commands={sorted(c.commands)}"
              f" auto_off={'yes' if c.auto_off else 'no'}"
              f" auto_on={'yes' if c.auto_on else 'no'}")
    for d in cfg.devices.values():
        chats = ",".join(str(c) for c in d.allowed_chats) or "(falls back to ALLOWED_CHATS)"
        print(f"  device {d.name}: class={d.class_name} prefix={d.topic_prefix}"
              f" allowed_chats=[{chats}]")
    if not ALLOWED_CHATS:
        print("warning: ALLOWED_CHATS is empty in env; only /id will work until you set it.")
    sys.exit(0)

# --- Heavy imports below this point --------------------------------------

import json  # noqa: E402
import re  # noqa: E402
import time  # noqa: E402

from appdirs import user_config_dir  # noqa: E402
from deltachat2 import EventType, MsgData, events  # noqa: E402
from deltabot_cli import BotCli  # noqa: E402

import durations  # noqa: E402
import engine as engine_mod  # noqa: E402
import scheduler as sched_mod  # noqa: E402
from history import History  # noqa: E402
from mqtt_client import MqttClient  # noqa: E402
from webxdc_io import WebxdcIO  # noqa: E402

cli = BotCli(BOT_NAME)
log = logging.getLogger("mqtt_bot")
STATE_DIR = Path(user_config_dir(BOT_NAME))

# --- Wiring ---------------------------------------------------------------

webxdc = WebxdcIO(state_dir=STATE_DIR, devices_dir=DEVICES_DIR)
_RETENTION_DAYS = int((os.environ.get("RETENTION_DAYS") or "0").strip() or "0")
history = History(db_path=STATE_DIR / "history.sqlite",
                  retention_days=_RETENTION_DAYS)


# Flush the in-memory minute buffer on clean shutdown so a `systemctl
# stop` doesn't lose up to ~60s of un-flushed power samples per device.
def _on_shutdown(*_a) -> None:
    try:
        history.close()
    except Exception:
        log.exception("history close failed")
    sys.exit(0)


import atexit  # noqa: E402
import signal  # noqa: E402
atexit.register(_on_shutdown)
signal.signal(signal.SIGTERM, _on_shutdown)
scheduler = sched_mod.Scheduler(on_fire=lambda *a, **kw: engine.on_fire(*a, **kw))
mqtt = MqttClient(
    host=os.environ.get("MQTT_HOST", "127.0.0.1"),
    port=int(os.environ.get("MQTT_PORT", "1883")),
    username=os.environ.get("MQTT_USER", ""),
    password=os.environ.get("MQTT_PASS", ""),
    client_id=os.environ.get("MQTT_CLIENT_ID", BOT_NAME),
    keepalive=int(os.environ.get("MQTT_KEEPALIVE", "30")),
    subscriptions_for=lambda: engine.subscriptions_for(),
    on_message=lambda topic, payload: engine.on_mqtt_message(topic, payload),
)
engine = engine_mod.Engine(
    cfg=cfg,
    allowed_chats=ALLOWED_CHATS,
    mqtt=mqtt,
    webxdc=webxdc,
    scheduler=scheduler,
    client_id=os.environ.get("MQTT_CLIENT_ID", BOT_NAME),
    help_prefix=(os.environ.get("HELP_MESSAGE") or "").strip(),
    history=history,
    instances_path=DEVICES_FILE,
)


# --- Command parser -------------------------------------------------------

_GLOBAL_VERBS = {"id", "list", "apps", "help"}
_DIRECT_VERBS = {"on", "off", "toggle", "status"}
_CANCEL_VERBS = {"cancel-auto-off", "cancel-auto-on", "cancel-schedule"}
_SCHEDULE_VERBS = {"auto-off", "auto-on"}

_CMD_RE = re.compile(r"^/(\S+)(?:\s+(.*))?$", re.DOTALL)
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")

# Replay protection (text path is more lenient because typed commands
# may sit in queue during a network outage).
MAX_AGE_SECONDS = 200
MAX_APP_AGE_SECONDS = 45
MAX_CLOCK_SKEW_SECONDS = 30


def _sanitize(value, fallback: str = "?", max_len: int = 64) -> str:
    if not isinstance(value, str):
        value = str(value) if value is not None else ""
    cleaned = _CTRL_RE.sub(" ", value).strip()
    return cleaned[:max_len] if cleaned else fallback


def _parse_text_command(text: str) -> tuple[str, str, str] | None:
    m = _CMD_RE.match(text.strip())
    if not m:
        return None
    head = m.group(1).lower()
    tail = (m.group(2) or "").strip()
    if head in _GLOBAL_VERBS:
        return ("", head, tail)
    if not tail:
        return None
    parts = tail.split(maxsplit=1)
    verb = parts[0].lower()
    rest = parts[1] if len(parts) > 1 else ""
    return (head, verb, rest)


def _is_allowed(chatid: int) -> bool:
    return chatid in ALLOWED_CHATS


# --- Hooks ----------------------------------------------------------------

@cli.on(events.RawEvent)
def _log_event(bot, accid, event):
    bot.logger.debug("%s", event)


@cli.on(events.RawEvent)
def _on_webxdc_update(bot, accid, event):
    if event.kind != EventType.WEBXDC_STATUS_UPDATE:
        return
    msgid = event.msg_id
    serial = event.status_update_serial - 1
    raw = bot.rpc.get_webxdc_status_updates(accid, msgid, serial)
    try:
        update = json.loads(raw)[0]
    except (json.JSONDecodeError, IndexError):
        bot.logger.warning("failed to decode webxdc update msgid=%s", msgid)
        return
    payload = update.get("payload") or {}
    req = payload.get("request") if isinstance(payload, dict) else None
    if not isinstance(req, dict):
        return  # not a user request -- our own response or unrelated update

    msg = bot.rpc.get_message(accid, msgid)
    chatid = msg.chat_id

    if not _is_allowed(chatid):
        bot.logger.warning("webxdc update from non-allowed chat %d ignored", chatid)
        return

    ts = req.get("ts")
    if isinstance(ts, (int, float)):
        age = int(time.time()) - int(ts)
        if age > MAX_APP_AGE_SECONDS or age < -MAX_CLOCK_SKEW_SECONDS:
            bot.logger.info("webxdc cmd age=%ds dropped (chat=%d)", age, chatid)
            return

    engine.handle_webxdc_request(chatid, msgid, req)


@cli.on(events.NewMessage)
def _on_new_message(bot, accid, event):
    msg = event.msg
    chatid = msg.chat_id

    # Auto-post help when a new member is added to a group. Only do this
    # in allowed chats — staying silent in unauthorised chats (otherwise
    # the bot would dump its command surface to anyone who happened to
    # add it to a group).
    if getattr(msg, "is_info", False) and \
            getattr(msg, "system_message_type", "") == "MemberAddedToGroup":
        if _is_allowed(chatid):
            bot.rpc.send_msg(accid, chatid, MsgData(text=engine.help_text(chatid)))
        return

    text = (msg.text or "")
    parsed = _parse_text_command(text)

    # /id and /help bypass the allow-list (both are read-only; /id is needed
    # for setup discovery, /help is generic command surface — it does not
    # leak which specific devices exist for a non-allowed chat because
    # engine.help_text filters the device list per chat).
    if parsed and parsed[0] == "" and parsed[1] == "id":
        bot.rpc.send_msg(
            accid, chatid,
            MsgData(text=f"this chat's id is {chatid}; "
                         f"add it to ALLOWED_CHATS in .env/env to authorise the bot"),
        )
        return
    if parsed and parsed[0] == "" and parsed[1] == "help":
        bot.rpc.send_msg(accid, chatid, MsgData(text=engine.help_text(chatid)))
        return

    # Non-slash text in an authorised chat → stay silent. Help is now only
    # posted on explicit /help or when a new member joins.
    if parsed is None:
        return

    if not _is_allowed(chatid):
        bot.rpc.send_msg(accid, chatid, MsgData(text="permission denied"))
        return

    age = int(time.time()) - int(msg.timestamp)
    if age > MAX_AGE_SECONDS or age < -MAX_CLOCK_SKEW_SECONDS:
        bot.logger.info("text command age=%ds dropped in chat %d", age, chatid)
        try:
            bot.rpc.send_reaction(accid, msg.id, ["❌"])
        except Exception:
            pass
        return

    head, verb, rest = parsed

    if head == "":
        if verb == "list":
            bot.rpc.send_msg(accid, chatid, MsgData(text=engine.list_devices(chatid)))
            return
        if verb == "apps":
            _handle_apps(bot, accid, chatid)
            return
        if verb == "help":
            bot.rpc.send_msg(accid, chatid, MsgData(text=engine.help_text(chatid)))
            return

    # /all <verb> — apply a direct verb to every device visible to this chat.
    if head == "all" and verb in _DIRECT_VERBS and not rest:
        _handle_all(bot, accid, chatid, verb, msg.id)
        return

    device_name = head
    if verb == "status" and not rest:
        bot.rpc.send_msg(accid, chatid, MsgData(text=engine.status_for(chatid, device_name)))
        return
    if verb in _DIRECT_VERBS:
        # `off <clause>` is shorthand for scheduling auto-off — no immediate toggle.
        # `on <clause>` is "on now + auto-off when clause fires" — toggles AND schedules.
        if verb == "off" and rest:
            _handle_off_clause(bot, accid, chatid, device_name, rest)
            return
        ok, msg_text = engine.dispatch_command(chatid, device_name, verb, source_msgid=msg.id)
        if not ok and msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
            return
        if verb == "on" and rest:
            _handle_on_clause(bot, accid, chatid, device_name, rest)
        return

    if verb in _CANCEL_VERBS:
        target = _resolve_cancel_target(device_name, verb)
        ok, msg_text = engine.cancel_schedule(chatid, device_name, target_action=target)
        if msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
        return

    if verb == "export":
        _handle_export(bot, accid, chatid, device_name, rest)
        return

    if verb in _SCHEDULE_VERBS:
        device = cfg.devices.get(device_name)
        if device is None:
            bot.rpc.send_msg(accid, chatid, MsgData(text=f"unknown device: {device_name}"))
            return
        cls = cfg.device_class(device)
        if verb == "auto-off":
            section = cls.auto_off
            allowed = sched_mod.ALL_POLICY_KINDS
        else:
            section = cls.auto_on
            allowed = frozenset({"timer", "tod"})
        if section is None:
            bot.rpc.send_msg(accid, chatid,
                             MsgData(text=f"{verb} not supported for {device_name}"))
            return
        try:
            policy = sched_mod.parse_policy(
                rest, engine_mod.policy_defaults_for_section(section), allowed=allowed,
            )
        except ValueError as ex:
            bot.rpc.send_msg(accid, chatid, MsgData(text=f"bad clause: {ex}"))
            return
        ok, msg_text = engine.schedule(chatid, device_name, section.command, policy)
        if msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
        return

    bot.rpc.send_msg(accid, chatid,
                     MsgData(text=f"unknown command: {verb}. Try /help."))


def _handle_on_clause(bot, accid, chatid: int, device_name: str, clause: str) -> None:
    """`/<dev> on <clause>` scheduled an auto-off after the immediate `on` dispatched."""
    _schedule_auto_off_clause(bot, accid, chatid, device_name, clause)


def _handle_off_clause(bot, accid, chatid: int, device_name: str, clause: str) -> None:
    """`/<dev> off <clause>` — schedule auto-off without an immediate toggle."""
    _schedule_auto_off_clause(bot, accid, chatid, device_name, clause)


def _schedule_auto_off_clause(bot, accid, chatid: int, device_name: str, clause: str) -> None:
    device = cfg.devices.get(device_name)
    if device is None:
        bot.rpc.send_msg(accid, chatid, MsgData(text=f"unknown device: {device_name}"))
        return
    cls = cfg.device_class(device)
    if cls.auto_off is None:
        bot.rpc.send_msg(accid, chatid, MsgData(text="auto-off not supported for this device"))
        return
    try:
        policy = sched_mod.parse_policy(
            clause, engine_mod.policy_defaults_for_section(cls.auto_off),
        )
    except ValueError as ex:
        bot.rpc.send_msg(accid, chatid, MsgData(text=f"bad clause: {ex}"))
        return
    _ok, msg_text = engine.schedule(chatid, device_name, cls.auto_off.command, policy)
    if msg_text:
        bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))


def _resolve_cancel_target(device_name: str, verb: str) -> str | None:
    device = cfg.devices.get(device_name)
    if device is None:
        return None
    cls = cfg.device_class(device)
    if verb == "cancel-auto-off" and cls.auto_off:
        return cls.auto_off.command
    if verb == "cancel-auto-on" and cls.auto_on:
        return cls.auto_on.command
    return None  # cancel-schedule: cancel all jobs for the device


def _handle_export(bot, accid: int, chatid: int, device_name: str, rest: str) -> None:
    """Dump power_minute + energy_hour for a device to a CSV attachment."""
    import csv
    import datetime as _dt
    import tempfile

    device = cfg.devices.get(device_name)
    if device is None:
        bot.rpc.send_msg(accid, chatid, MsgData(text=f"unknown device: {device_name}"))
        return
    window_str = (rest or "7d").strip()
    try:
        window_seconds = durations.parse(window_str)
    except ValueError as ex:
        bot.rpc.send_msg(accid, chatid, MsgData(text=f"bad duration: {ex}"))
        return
    until_ts = int(time.time())
    since_ts = until_ts - window_seconds
    power_rows = history.query_power_raw(device_name, since_ts, until_ts)
    energy_rows = history.query_energy(device_name, since_ts, until_ts)
    energy_minute_rows = history.query_energy_minute(device_name, since_ts, until_ts)
    samples_rows = history.query_samples_raw(device_name, since_ts, until_ts)

    if not (power_rows or energy_rows or energy_minute_rows or samples_rows):
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=f"no history yet for {device_name} ({window_str})"))
        return

    fd, path = tempfile.mkstemp(suffix=".csv", prefix=f"{device_name}-{window_str}-")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "unix_ts", "iso_time", "device", "kind",
                # power_minute / energy_hour / energy_minute fields:
                "avg_apower_w", "output", "sample_count",
                "aenergy_wh", "energy_mwh",
                # samples_raw fields:
                "apower_w", "voltage_v", "current_a", "freq_hz",
                "aenergy_total_wh", "temperature_c",
            ])
            for ts, apower, output, count in power_rows:
                w.writerow([ts, _dt.datetime.fromtimestamp(ts).isoformat(),
                            device_name, "power_minute",
                            f"{apower:.3f}",
                            "" if output is None else output,
                            count, "", "", "", "", "", "", "", ""])
            for ts, aenergy in energy_rows:
                w.writerow([ts, _dt.datetime.fromtimestamp(ts).isoformat(),
                            device_name, "energy_hour",
                            "", "", "", f"{aenergy:.3f}", "",
                            "", "", "", "", "", ""])
            for ts, mwh in energy_minute_rows:
                w.writerow([ts, _dt.datetime.fromtimestamp(ts).isoformat(),
                            device_name, "energy_minute",
                            "", "", "", "", f"{mwh:.3f}",
                            "", "", "", "", "", ""])
            for row in samples_rows:
                ts, ap, v, c, f_hz, ae, out, tc = row
                w.writerow([ts, _dt.datetime.fromtimestamp(ts).isoformat(),
                            device_name, "samples_raw",
                            "", "", "", "", "",
                            "" if ap is None else f"{ap:.3f}",
                            "" if v is None else f"{v:.2f}",
                            "" if c is None else f"{c:.4f}",
                            "" if f_hz is None else f"{f_hz:.2f}",
                            "" if ae is None else f"{ae:.3f}",
                            "" if out is None else out,
                            "" if tc is None else f"{tc:.1f}"])
        bot.rpc.send_msg(
            accid, chatid,
            MsgData(file=path,
                    text=f"{device_name} export · {window_str} · "
                         f"{len(samples_rows)} status updates · "
                         f"{len(power_rows)} per-min · "
                         f"{len(energy_minute_rows)} energy-min · "
                         f"{len(energy_rows)} energy-hr"),
        )
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _handle_all(bot, accid: int, chatid: int, verb: str, source_msgid: int) -> None:
    """Run a direct verb against every device visible to this chat."""
    visible = [
        d for d in cfg.devices.values()
        if (d.allowed_chats and chatid in d.allowed_chats)
        or (not d.allowed_chats and chatid in ALLOWED_CHATS)
    ]
    if not visible:
        bot.rpc.send_msg(accid, chatid, MsgData(text="no devices visible"))
        return
    succeeded: list[str] = []
    failed: list[str] = []
    for d in visible:
        ok, msg_text = engine.dispatch_command(
            chatid, d.name, verb, source_msgid=None,
        )
        (succeeded if ok else failed).append(d.name)
    bits = []
    if succeeded:
        bits.append(f"sent {verb} to {', '.join(succeeded)}")
    if failed:
        bits.append(f"failed: {', '.join(failed)}")
    bot.rpc.send_msg(accid, chatid, MsgData(text=" · ".join(bits) or "(noop)"))
    try:
        bot.rpc.send_reaction(accid, source_msgid, ["🆗" if not failed else "⚠️"])
    except Exception:
        pass


def _handle_apps(bot, accid: int, chatid: int) -> None:
    visible_classes: set[str] = {
        d.class_name for d in cfg.devices.values()
        if (d.allowed_chats and chatid in d.allowed_chats)
        or (not d.allowed_chats and chatid in ALLOWED_CHATS)
    }
    sent, retracted = webxdc.send_apps(bot, accid, chatid, visible_classes)
    # Seed the freshly-installed instances with the current cache so the
    # twisty populates immediately. Without this push, the app stays
    # blank until the next inbound MQTT message — and Shelly's status
    # JSON is non-retained, so a quiet plug means a blank UI for minutes.
    if sent:
        webxdc.push_filtered(bot, accid, engine.snapshot_for)
    fragments: list[str] = []
    if sent:
        fragments.append(f"Sent: {', '.join(sent)}")
    if retracted:
        fragments.append(f"Retracted: {', '.join(retracted)}")
    if not fragments:
        fragments.append("No apps available for this chat")
    bot.rpc.send_msg(accid, chatid, MsgData(text=". ".join(fragments) + "."))


# --- Startup --------------------------------------------------------------

@cli.on_start
def _on_start(bot, _args):
    accounts = bot.rpc.get_all_account_ids()
    if not accounts:
        bot.logger.warning("no Delta Chat account configured. "
                           "Run `./init-from-backup.sh` or onboard manually.")
        return
    accid = accounts[0]
    engine.set_bot(bot, accid)
    mqtt.start()
    scheduler.start()
    bot.logger.info(
        "mqtt-bot up; classes=%s devices=%s allowed_chats=%s",
        sorted(cfg.classes), sorted(cfg.devices), sorted(ALLOWED_CHATS),
    )
    if not ALLOWED_CHATS:
        bot.logger.warning(
            "ALLOWED_CHATS is empty; every command (except /id) will be denied. "
            "Use /id in the target chat and add the returned id."
        )


if __name__ == "__main__":
    cli.start()
