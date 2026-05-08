"""Entry point: Delta Chat hooks + thin routing glue.

All interesting logic lives in plug.py (PlugTwin), rules.py (sweeper +
parser + persistence), snapshot.py (assembly), publisher.py (push
pipeline). This file just:
  - constructs everything from env + config
  - registers Delta Chat hooks
  - parses incoming text into structured commands
  - hands off to the right twin via a small set of routing functions

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
# log calls would be silently dropped.
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
ALLOWED_CHATS: set[int] = config_mod.parse_allowed_chats(
    os.environ.get("ALLOWED_CHATS")
)

cfg = config_mod.load(devices_dir=DEVICES_DIR, instances_file=DEVICES_FILE)

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
import permissions  # noqa: E402
import rules as rules_mod  # noqa: E402
import snapshot as snap_mod  # noqa: E402
from history import History  # noqa: E402
from mqtt_client import MqttClient  # noqa: E402
from plug import PlugTwin, TwinDeps  # noqa: E402
from publisher import Publisher  # noqa: E402
from twins import TwinRegistry  # noqa: E402
from webxdc_io import WebxdcIO  # noqa: E402

cli = BotCli(BOT_NAME)
log = logging.getLogger("mqtt_bot")
STATE_DIR = Path(user_config_dir(BOT_NAME))
RULES_PATH = STATE_DIR / "rules.json"

CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", BOT_NAME)
PUBLISH_INTERVAL_S = int(os.environ.get("PUBLISH_INTERVAL_S", "300"))


# --- Late-bound bot reference --------------------------------------------

class _BotState:
    """Holder for the deltabot bot + account id. Filled in by _on_start.
    Twins capture a reference at construction; their callables read
    `state.bot` lazily so they're safe to construct before the cli starts."""
    bot: object = None
    accid: int = 0


state = _BotState()


# --- Side-effect callables injected into TwinDeps ------------------------

def _post_to_visible_chats(device, text: str) -> None:
    if state.bot is None:
        log.warning("no bot ref yet; would post to %s: %s", device.name, text)
        return
    chats = device.allowed_chats or tuple(sorted(ALLOWED_CHATS))
    for chat_id in chats:
        try:
            state.bot.rpc.send_msg(state.accid, chat_id, MsgData(text=text))
            log.info("posted to chat=%d device=%s: %s",
                     chat_id, device.name, text[:80])
        except Exception:
            log.exception("post to chat %d failed", chat_id)


def _react(msgid: int, emoji: str) -> None:
    if state.bot is None:
        return
    try:
        state.bot.rpc.send_reaction(state.accid, msgid, [emoji])
    except Exception:
        log.exception("send_reaction failed")


def _mqtt_publish(topic: str, payload: str) -> None:
    mqtt.publish(topic, payload)


def _save_rules() -> None:
    rules_mod.save_all(registry, RULES_PATH)
    sweeper.wake()


def _publisher_broadcast(device_name: str | None = None) -> None:
    publisher.broadcast(device_name)


# --- Construct everything ------------------------------------------------

webxdc = WebxdcIO(state_dir=STATE_DIR, devices_dir=DEVICES_DIR)
history = History(
    db_path=STATE_DIR / "history.sqlite",
    retention_days=int((os.environ.get("RETENTION_DAYS") or "0").strip() or "0"),
)

# Build one twin per device. TwinDeps wires up the side effects.
_deps = TwinDeps(
    mqtt_publish=_mqtt_publish,
    post_to_chats=_post_to_visible_chats,
    broadcast=_publisher_broadcast,
    save_rules=_save_rules,
    react=_react,
    history=history,
    client_id=CLIENT_ID,
)
_twins = [
    PlugTwin(cls=cfg.classes[d.class_name], cfg=d, deps=_deps)
    for d in cfg.devices.values()
]
registry = TwinRegistry(_twins)
sweeper = rules_mod.RulesSweeper(registry)

publisher = Publisher(
    build=lambda chat_id, class_name: snap_mod.build_for_chat(
        chat_id, class_name, registry, ALLOWED_CHATS,
    ),
    msgids=lambda: webxdc.map_snapshot(),
    send=lambda chat_id, msgid, payload: webxdc.push_to_msgid(
        state.bot, state.accid, msgid, payload,
    ),
    interval_s=PUBLISH_INTERVAL_S,
)

mqtt = MqttClient(
    host=os.environ.get("MQTT_HOST", "127.0.0.1"),
    port=int(os.environ.get("MQTT_PORT", "1883")),
    username=os.environ.get("MQTT_USER", ""),
    password=os.environ.get("MQTT_PASS", ""),
    client_id=CLIENT_ID,
    keepalive=int(os.environ.get("MQTT_KEEPALIVE", "30")),
    subscriptions_for=lambda: registry.subscriptions(),
    on_message=lambda topic, payload: on_mqtt_message(topic, payload),
)


def _on_shutdown(*_a) -> None:
    try:
        publisher.stop()
    except Exception:
        pass
    try:
        history.close()
    except Exception:
        log.exception("history close failed")
    sys.exit(0)


import atexit  # noqa: E402
import signal  # noqa: E402
atexit.register(_on_shutdown)
signal.signal(signal.SIGTERM, _on_shutdown)


# --- Routing functions (the former engine glue) -------------------------

def on_mqtt_message(topic: str, payload: bytes) -> None:
    """MQTT thread → find twin by topic → twin.on_mqtt."""
    found = registry.find_by_topic(topic)
    if found is None:
        return
    twin, suffix = found
    twin.on_mqtt(suffix, payload)


def dispatch_command(chat_id: int, device_name: str, action: str,
                     *, source_msgid: int | None = None) -> tuple[bool, str]:
    twin = registry.get(device_name)
    if twin is None:
        return False, f"unknown device: {device_name}"
    if not twin.can_chat_see(chat_id, ALLOWED_CHATS):
        return False, "permission denied"
    return twin.dispatch(action, source_msgid)


def schedule(chat_id: int, device_name: str, target_action: str,
             policy) -> tuple[bool, str]:
    twin = registry.get(device_name)
    if twin is None:
        return False, f"unknown device: {device_name}"
    if not twin.can_chat_see(chat_id, ALLOWED_CHATS):
        return False, "permission denied"
    return twin.schedule(target_action, policy, chat_id)


def cancel_schedule(chat_id: int, device_name: str,
                    target_action: str | None = None,
                    rule_id: str | None = None) -> tuple[bool, str]:
    twin = registry.get(device_name)
    if twin is None:
        return False, f"unknown device: {device_name}"
    if not twin.can_chat_see(chat_id, ALLOWED_CHATS):
        return False, "permission denied"
    cancelled = twin.cancel(target_action=target_action, rule_id=rule_id)
    if not cancelled:
        return True, f"no pending schedule for {device_name}"
    return True, f"cancelled {len(cancelled)} schedule(s) for {device_name}"


def list_devices(chat_id: int) -> str:
    visible = registry.visible_to(chat_id, ALLOWED_CHATS)
    if not visible:
        return "no devices visible to this chat"
    lines: list[str] = []
    for t in visible:
        lines.append(_format_device_line(t))
    return "\n".join(lines)


def list_rules(chat_id: int, device_name: str | None = None) -> str:
    if device_name is not None:
        twin = registry.get(device_name)
        if twin is None:
            return f"unknown device: {device_name}"
        if not twin.can_chat_see(chat_id, ALLOWED_CHATS):
            return "permission denied"
        twins = [twin]
    else:
        twins = registry.visible_to(chat_id, ALLOWED_CHATS)
    if not twins:
        return "no devices visible to this chat"
    lines: list[str] = []
    total = 0
    for t in twins:
        jobs = t.jobs_snapshot()
        if not jobs:
            continue
        lines.append(f"{t.name}:")
        for j in jobs:
            for line in _format_rule_lines(j):
                lines.append("  " + line)
            total += 1
    return "\n".join(lines) if total else "no rules pending"


def status_for(chat_id: int, device_name: str) -> str:
    twin = registry.get(device_name)
    if twin is None:
        return f"unknown device: {device_name}"
    if not twin.can_chat_see(chat_id, ALLOWED_CHATS):
        return "permission denied"
    return _format_device_line(twin)


def help_text(chat_id: int) -> str:
    visible = registry.visible_to(chat_id, ALLOWED_CHATS)
    names = (", ".join(t.name for t in visible)
             or "(none — your chat has no visible devices)")
    base = (
        "Commands:\n"
        "  /<device> on | off | toggle | status\n"
        "  /<device> off in 30m | off at 18h | off at 18:30 daily\n"
        "  /<device> off if idle              # power<5W for 60s (defaults)\n"
        "  /<device> off if idle 10W 120s     # power<10W for 120s\n"
        "  /<device> off if idle 10Wh in 2m   # energy<10Wh in last 2 min\n"
        "  /<device> on for 30m               # on now + auto-off in 30 min\n"
        "  /<device> on for 1h or if idle     # on now + auto-off (timer or idle)\n"
        "  /<device> auto-on at 7h | at 7h daily\n"
        "  /<device> cancel-auto-off | cancel-auto-on | cancel-schedule\n"
        "  /<device> export 7d                — CSV of power + energy history\n"
        "  /<device> rules                    — list this device's rules\n"
        "  /rules               — list rules for every visible device\n"
        "  /list                — list devices visible to this chat\n"
        "  /apps                — (re)deliver webxdc control apps\n"
        "  /id                  — show this chat's id\n"
        "  /help                — this message\n"
        f"Devices in this chat: {names}\n"
    )
    prefix = (os.environ.get("HELP_MESSAGE") or "").strip()
    return f"{prefix}\n\n{base}" if prefix else base


def handle_webxdc_request(chat_id: int, msgid: int,
                          request: dict) -> None:
    cls_for_msg = webxdc.class_for_msgid(chat_id, msgid)
    if cls_for_msg is None:
        log.warning("webxdc update from unregistered msgid=%d in chat=%d "
                    "(run /apps to register)", msgid, chat_id)
        return
    device_name = str(request.get("device", "")).strip().lower()
    action = str(request.get("action", "")).strip().lower()

    # Refresh button: device may be unset; respond with a unicast push
    # for the (chat, class) of the requesting msgid.
    if action == "refresh":
        publisher.push_unicast(chat_id, msgid, cls_for_msg)
        return

    if not device_name or not action:
        return

    # Legacy actions — silently drop. App needs /apps to upgrade.
    if action in ("history", "events", "set_param"):
        log.info("legacy webxdc action %r dropped (chat=%d msgid=%d)",
                 action, chat_id, msgid)
        return

    if action in ("cancel-auto-off", "cancel-auto-on", "cancel-schedule"):
        target = _resolve_cancel_target(device_name, action)
        rid = request.get("rule_id")
        rule_id = str(rid) if isinstance(rid, str) and rid else None
        ok, msg = cancel_schedule(chat_id, device_name,
                                  target_action=target, rule_id=rule_id)
        if msg and state.bot:
            state.bot.rpc.send_msg(state.accid, chat_id, MsgData(text=msg))
        return

    if action in ("auto-off", "auto-on"):
        _schedule_from_app(chat_id, device_name, action, request)
        return

    # Direct action with optional inline auto_off / auto_on policy.
    ok, msg = dispatch_command(chat_id, device_name, action)
    if not ok and msg and state.bot:
        state.bot.rpc.send_msg(state.accid, chat_id, MsgData(text=msg))
        return

    for key in ("auto_off", "auto_on"):
        extra = request.get(key)
        if not isinstance(extra, dict):
            continue
        twin = registry.get(device_name)
        if twin is None:
            continue
        section = (twin.cls.auto_off if key == "auto_off"
                   else twin.cls.auto_on)
        if section is None:
            continue
        try:
            policy = _policy_from_app(extra, section)
        except ValueError as ex:
            if state.bot:
                state.bot.rpc.send_msg(state.accid, chat_id,
                                       MsgData(text=f"bad {key}: {ex}"))
            continue
        schedule(chat_id, device_name, section.command, policy)


def _schedule_from_app(chat_id: int, device_name: str, action: str,
                       request: dict) -> None:
    direction = "off" if action == "auto-off" else "on"
    key = "auto_off" if action == "auto-off" else "auto_on"
    extra = request.get(key)
    if not isinstance(extra, dict):
        if state.bot:
            state.bot.rpc.send_msg(state.accid, chat_id,
                                   MsgData(text=f"missing {key} body"))
        return
    twin = registry.get(device_name)
    if twin is None:
        return
    section = twin.cls.auto_off if direction == "off" else twin.cls.auto_on
    if section is None:
        if state.bot:
            state.bot.rpc.send_msg(state.accid, chat_id,
                                   MsgData(text=f"{action} not supported"))
        return
    try:
        policy = _policy_from_app(extra, section)
    except ValueError as ex:
        if state.bot:
            state.bot.rpc.send_msg(state.accid, chat_id,
                                   MsgData(text=f"bad {key}: {ex}"))
        return
    schedule(chat_id, device_name, section.command, policy)


def _policy_from_app(raw: dict, section) -> rules_mod.ScheduledPolicy:
    """Build a ScheduledPolicy from a webxdc app payload subobject."""
    defaults = _defaults_from_section(section)
    policy = rules_mod.ScheduledPolicy()
    if isinstance(raw.get("timer_seconds"), (int, float)) and raw["timer_seconds"] > 0:
        policy.timer_seconds = int(raw["timer_seconds"])
    tod = raw.get("time_of_day")
    if isinstance(tod, list) and len(tod) == 2:
        h, m = int(tod[0]), int(tod[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            policy.time_of_day = (h, m)
            policy.recurring_tod = bool(raw.get("recurring_tod", False))
    idle = raw.get("idle")
    if isinstance(idle, dict):
        policy.idle_field = str(idle.get("field", defaults.idle_field))
        policy.idle_threshold = float(idle.get("threshold", defaults.idle_threshold))
        policy.idle_duration_s = int(idle.get("duration_s", defaults.idle_duration_s))
    consumed = raw.get("consumed")
    if isinstance(consumed, dict):
        policy.consumed_field = str(consumed.get("field", defaults.consumed_field))
        policy.consumed_threshold_wh = float(consumed.get(
            "threshold_wh", defaults.consumed_threshold_wh))
        policy.consumed_window_s = int(consumed.get(
            "window_s", defaults.consumed_window_s))
    if raw.get("once") is True:
        policy.once = True
    if policy.is_empty():
        raise ValueError("no policies supplied")
    return policy


def _defaults_from_section(section) -> rules_mod.PolicyDefaults:
    if isinstance(section, config_mod.AutoOffConfig):
        return rules_mod.PolicyDefaults(
            idle_field=section.default_idle_field,
            idle_threshold=section.default_idle_threshold,
            idle_duration_s=section.default_idle_duration,
            consumed_field=section.default_consumed_field,
            consumed_threshold_wh=section.default_consumed_threshold_wh,
            consumed_window_s=section.default_consumed_window_s,
        )
    return rules_mod.PolicyDefaults()


def _resolve_cancel_target(device_name: str, verb: str) -> str | None:
    twin = registry.get(device_name)
    if twin is None:
        return None
    if verb == "cancel-auto-off" and twin.cls.auto_off:
        return twin.cls.auto_off.command
    if verb == "cancel-auto-on" and twin.cls.auto_on:
        return twin.cls.auto_on.command
    return None  # cancel-schedule: drop all rules for the device


# --- Formatting helpers ---------------------------------------------------

def _format_device_line(twin: PlugTwin) -> str:
    f = dict(twin.fields)
    online = f.get("online")
    output = f.get("output")
    apower = f.get("apower")
    aenergy = f.get("aenergy")
    bits: list[str] = [twin.name]
    bits.append("🟢" if online else "🔴" if online is False else "⚪")
    if isinstance(output, bool):
        bits.append("ON" if output else "OFF")
    elif output is None:
        bits.append("?")
    if isinstance(apower, (int, float)):
        bits.append(f"{apower:.0f}W")
    if isinstance(aenergy, (int, float)):
        bits.append(f"({aenergy / 1000.0:.2f} kWh)")
    if twin.cfg.description:
        bits.append(f"— {twin.cfg.description}")
    for job in twin.jobs_snapshot():
        action = job.target_action
        if job.deadline_ts:
            remaining = max(0, job.deadline_ts - int(time.time()))
            bits.append(f"[{action} in {durations.format(remaining)}]")
        elif job.has_idle():
            bits.append(f"[{action} on idle]")
        elif job.has_consumed():
            bits.append(f"[{action} on used<Wh]")
    if len(cfg.classes) > 1:
        bits.append(f"[{twin.cls.name}]")
    return " ".join(bits)


def _format_rule_lines(job) -> list[str]:
    """Line(s) for /rules. Single-clause rules inline; multi-clause
    (OR-combined) rules get an indented bullet list under the action header."""
    clauses = _rule_clauses(job)
    suffix = " (once)" if job.once else ""
    if not clauses:
        return [job.target_action + suffix]
    if len(clauses) == 1:
        return [f"{job.target_action} {clauses[0]}{suffix}"]
    return [f"{job.target_action}:{suffix}"] + [f"  - {c}" for c in clauses]


def _rule_clauses(job) -> list[str]:
    out: list[str] = []
    if job.deadline_ts:
        remaining = max(0, job.deadline_ts - int(time.time()))
        if job._time_mode == "tod" and job.time_of_day:
            h, m = job.time_of_day
            suffix = " daily" if job.recurring_tod else ""
            out.append(f"at {h:02d}:{m:02d}{suffix} "
                       f"(in {durations.format(remaining)})")
        else:
            out.append(f"in {durations.format(remaining)}")
    if job.has_idle():
        out.append(f"when {job.idle_field}<{job.idle_threshold:g}W "
                   f"for {durations.format(job.idle_duration_s)}")
    if job.has_consumed():
        out.append(f"when used<{job.consumed_threshold_wh:g}Wh "
                   f"in {durations.format(job.consumed_window_s)}")
    return out


# --- Text-command parser --------------------------------------------------

_GLOBAL_VERBS = {"id", "list", "apps", "help", "rules"}
_DIRECT_VERBS = {"on", "off", "toggle", "status"}
_CANCEL_VERBS = {"cancel-auto-off", "cancel-auto-on", "cancel-schedule"}
_SCHEDULE_VERBS = {"auto-off", "auto-on"}

_CMD_RE = re.compile(r"^/(\S+)(?:\s+(.*))?$", re.DOTALL)
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")

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
        return  # our own response or unrelated update

    msg = bot.rpc.get_message(accid, msgid)
    chatid = msg.chat_id
    if not _is_allowed(chatid):
        bot.logger.warning("webxdc update from non-allowed chat %d ignored",
                           chatid)
        return

    ts = req.get("ts")
    if isinstance(ts, (int, float)):
        age = int(time.time()) - int(ts)
        if age > MAX_APP_AGE_SECONDS or age < -MAX_CLOCK_SKEW_SECONDS:
            bot.logger.info("webxdc cmd age=%ds dropped (chat=%d)", age, chatid)
            return

    handle_webxdc_request(chatid, msgid, req)


@cli.on(events.NewMessage)
def _on_new_message(bot, accid, event):
    msg = event.msg
    chatid = msg.chat_id

    if (getattr(msg, "is_info", False)
            and getattr(msg, "system_message_type", "") == "MemberAddedToGroup"):
        if _is_allowed(chatid):
            bot.rpc.send_msg(accid, chatid, MsgData(text=help_text(chatid)))
        return

    text = msg.text or ""
    parsed = _parse_text_command(text)

    # /id and /help bypass the allow-list (read-only; needed for setup).
    if parsed and parsed[0] == "" and parsed[1] == "id":
        bot.rpc.send_msg(
            accid, chatid,
            MsgData(text=f"this chat's id is {chatid}; "
                         f"add it to ALLOWED_CHATS in .env/env to authorise the bot"),
        )
        return
    if parsed and parsed[0] == "" and parsed[1] == "help":
        bot.rpc.send_msg(accid, chatid, MsgData(text=help_text(chatid)))
        return

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
            bot.rpc.send_msg(accid, chatid, MsgData(text=list_devices(chatid)))
            return
        if verb == "apps":
            _handle_apps(bot, accid, chatid)
            return
        if verb == "help":
            bot.rpc.send_msg(accid, chatid, MsgData(text=help_text(chatid)))
            return
        if verb == "rules":
            bot.rpc.send_msg(accid, chatid, MsgData(text=list_rules(chatid)))
            return

    if head == "all" and verb in _DIRECT_VERBS and not rest:
        _handle_all(bot, accid, chatid, verb, msg.id)
        return

    device_name = head
    if verb == "status" and not rest:
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=status_for(chatid, device_name)))
        return
    if verb == "rules" and not rest:
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=list_rules(chatid, device_name)))
        return
    if verb in _DIRECT_VERBS:
        if verb == "off" and rest:
            _handle_off_clause(bot, accid, chatid, device_name, rest)
            return
        ok, msg_text = dispatch_command(chatid, device_name, verb,
                                        source_msgid=msg.id)
        if not ok and msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
            return
        if verb == "on" and rest:
            _handle_on_clause(bot, accid, chatid, device_name, rest)
        return

    if verb in _CANCEL_VERBS:
        target = _resolve_cancel_target(device_name, verb)
        ok, msg_text = cancel_schedule(chatid, device_name, target_action=target)
        if msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
        return

    if verb == "export":
        _handle_export(bot, accid, chatid, device_name, rest)
        return

    if verb in _SCHEDULE_VERBS:
        twin = registry.get(device_name)
        if twin is None:
            bot.rpc.send_msg(accid, chatid,
                             MsgData(text=f"unknown device: {device_name}"))
            return
        if verb == "auto-off":
            section = twin.cls.auto_off
            allowed = rules_mod.ALL_POLICY_KINDS
        else:
            section = twin.cls.auto_on
            allowed = frozenset({"timer", "tod"})
        if section is None:
            bot.rpc.send_msg(accid, chatid,
                             MsgData(text=f"{verb} not supported for {device_name}"))
            return
        try:
            policy = rules_mod.parse_policy(
                rest, _defaults_from_section(section), allowed=allowed,
            )
        except ValueError as ex:
            bot.rpc.send_msg(accid, chatid, MsgData(text=f"bad clause: {ex}"))
            return
        ok, msg_text = schedule(chatid, device_name, section.command, policy)
        if msg_text:
            bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))
        return

    bot.rpc.send_msg(accid, chatid,
                     MsgData(text=f"unknown command: {verb}. Try /help."))


# --- Sub-handlers for compound text commands -----------------------------

def _handle_on_clause(bot, accid, chatid, device_name, clause):
    _schedule_auto_off_clause(bot, accid, chatid, device_name, clause)


def _handle_off_clause(bot, accid, chatid, device_name, clause):
    _schedule_auto_off_clause(bot, accid, chatid, device_name, clause)


def _schedule_auto_off_clause(bot, accid, chatid, device_name, clause):
    twin = registry.get(device_name)
    if twin is None:
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=f"unknown device: {device_name}"))
        return
    if twin.cls.auto_off is None:
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text="auto-off not supported for this device"))
        return
    try:
        policy = rules_mod.parse_policy(
            clause, _defaults_from_section(twin.cls.auto_off),
        )
    except ValueError as ex:
        bot.rpc.send_msg(accid, chatid, MsgData(text=f"bad clause: {ex}"))
        return
    _ok, msg_text = schedule(chatid, device_name, twin.cls.auto_off.command, policy)
    if msg_text:
        bot.rpc.send_msg(accid, chatid, MsgData(text=msg_text))


def _handle_all(bot, accid, chatid, verb, source_msgid):
    visible = registry.visible_to(chatid, ALLOWED_CHATS)
    if not visible:
        bot.rpc.send_msg(accid, chatid, MsgData(text="no devices visible"))
        return
    succeeded: list[str] = []
    failed: list[str] = []
    for t in visible:
        ok, _msg = t.dispatch(verb, source_msgid=None)
        (succeeded if ok else failed).append(t.name)
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


def _handle_apps(bot, accid, chatid):
    visible_classes = registry.visible_classes_for(chatid, ALLOWED_CHATS)
    sent, retracted = webxdc.send_apps(bot, accid, chatid, visible_classes)
    # Seed any freshly-installed instance with the full ground truth.
    if sent:
        publisher.broadcast()
    fragments: list[str] = []
    if sent:
        fragments.append(f"Sent: {', '.join(sent)}")
    if retracted:
        fragments.append(f"Retracted: {', '.join(retracted)}")
    if not fragments:
        fragments.append("No apps available for this chat")
    bot.rpc.send_msg(accid, chatid, MsgData(text=". ".join(fragments) + "."))


def _handle_export(bot, accid, chatid, device_name, rest):
    """Dump power_minute + energy_hour for a device to a CSV attachment."""
    import csv
    import datetime as _dt
    import tempfile

    twin = registry.get(device_name)
    if twin is None:
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=f"unknown device: {device_name}"))
        return
    if not twin.can_chat_see(chatid, ALLOWED_CHATS):
        bot.rpc.send_msg(accid, chatid, MsgData(text="permission denied"))
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

    fd, path = tempfile.mkstemp(suffix=".csv",
                                 prefix=f"{device_name}-{window_str}-")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "unix_ts", "iso_time", "device", "kind",
                "avg_apower_w", "output", "sample_count",
                "aenergy_wh", "energy_mwh",
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


# --- Startup --------------------------------------------------------------

@cli.on_start
def _on_start(bot, _args):
    accounts = bot.rpc.get_all_account_ids()
    if not accounts:
        bot.logger.warning("no Delta Chat account configured. "
                           "Run `./init-from-backup.sh` or onboard manually.")
        return
    accid = accounts[0]
    state.bot = bot
    state.accid = accid

    # Restore persisted rules onto twins, then backfill consumed/idle
    # evaluation buffers from history so they don't have to wait a fresh
    # window before being able to fire.
    rules_mod.load_into(registry, RULES_PATH)
    _rehydrate_rules_from_history()

    mqtt.start()
    sweeper.start()
    publisher.start()

    bot.logger.info(
        "mqtt-bot up; classes=%s devices=%s allowed_chats=%s "
        "publish_interval=%ds",
        sorted(cfg.classes), sorted(cfg.devices), sorted(ALLOWED_CHATS),
        PUBLISH_INTERVAL_S,
    )
    if not ALLOWED_CHATS:
        bot.logger.warning(
            "ALLOWED_CHATS is empty; every command (except /id) will be denied. "
            "Use /id in the target chat and add the returned id."
        )


def _rehydrate_rules_from_history() -> None:
    """After load_into, backfill consumed-rule sample buffers and
    idle-rule below-since timestamps from the SQLite history. Without
    this, a `systemctl restart` would force every rule to wait a fresh
    window."""
    if history is None:
        return
    now = int(time.time())
    for twin in registry.all():
        for job in twin.jobs_snapshot():
            if job.has_consumed() and job.consumed_field == "apower":
                since = now - job.consumed_window_s
                rows = history.query_power_raw(twin.name, since, now)
                if not rows:
                    continue
                for ts, apower, _output, _count in rows:
                    job._samples.append((ts, apower))
                job._consumed_started_at = rows[0][0]
                log.info("rehydrated consumed rule %s/%s with %d samples",
                         twin.name, job.rule_id, len(rows))
            if job.has_idle() and job.idle_field == "apower":
                since = now - job.idle_duration_s
                rows = history.query_power_raw(twin.name, since, now)
                if rows and all(r[1] < job.idle_threshold for r in rows):
                    job._below_since = rows[0][0]
                    log.info("rehydrated idle rule %s/%s — below "
                             "%.1fW since %d (continuous)",
                             twin.name, job.rule_id,
                             job.idle_threshold, rows[0][0])


if __name__ == "__main__":
    cli.start()
