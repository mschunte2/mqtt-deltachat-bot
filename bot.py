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
import threading
from pathlib import Path

# Wire our package loggers (mqtt_bot.*) to stderr — deltabot-cli only
# attaches a handler to its own logger, so without basicConfig our
# log calls would be silently dropped.
_LOG_LEVEL = (os.environ.get("LOG_LEVEL") or "info").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)


class _SwallowBrokenPipe(logging.Filter):
    """deltachat2's IOTransport writer thread races our SIGTERM handler;
    the rpc subprocess closes its stdin while we're shutting down and the
    writer logs a full BrokenPipeError stacktrace. It's harmless noise —
    we've already flushed history and signalled the publisher. Drop it."""
    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "deltachat2.IOTransport":
            return True
        if record.exc_info and isinstance(record.exc_info[1], BrokenPipeError):
            return False
        return "BrokenPipe" not in record.getMessage()


logging.getLogger("deltachat2.IOTransport").addFilter(_SwallowBrokenPipe())

from mqtt_bot.util import config as config_mod

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

from mqtt_bot import commands as commands_mod  # noqa: E402
from mqtt_bot import csv_export  # noqa: E402
from mqtt_bot import formatters  # noqa: E402
from mqtt_bot import rehydrate as rehydrate_mod  # noqa: E402
from mqtt_bot.core import rules as rules_mod  # noqa: E402
from mqtt_bot.core import snapshot as snap_mod  # noqa: E402
from mqtt_bot.core.twin import PlugTwin, TwinDeps  # noqa: E402
from mqtt_bot.core.twins import TwinRegistry  # noqa: E402
from mqtt_bot.io import baselines as baselines_mod  # noqa: E402
from mqtt_bot.io.history import History  # noqa: E402
from mqtt_bot.io.mqtt_client import MqttClient  # noqa: E402
from mqtt_bot.io import dc_config  # noqa: E402
from mqtt_bot.io.publisher import Publisher  # noqa: E402
from mqtt_bot.io.webxdc_io import WebxdcIO  # noqa: E402
from mqtt_bot.util import durations, permissions  # noqa: E402

cli = BotCli(BOT_NAME)
log = logging.getLogger("mqtt_bot")
STATE_DIR = Path(user_config_dir(BOT_NAME))
RULES_PATH = STATE_DIR / "rules.json"
BASELINES_PATH = STATE_DIR / "baselines.json"

CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", BOT_NAME)
PUBLISH_INTERVAL_S = int(os.environ.get("PUBLISH_INTERVAL_S", "10800"))
DC_DELETE_DEVICE_AFTER_DAYS = int(os.environ.get("DC_DELETE_DEVICE_AFTER_DAYS", "14"))


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


def _save_baselines() -> None:
    """Persist every twin's user-Counter state to baselines.json.
    Thin wrapper around baselines.save — the meat lives there so it
    can be unit-tested without needing bot.py's module globals."""
    baselines_mod.save(registry, BASELINES_PATH)


def _load_baselines() -> int:
    """Restore baselines.json on startup. Thin wrapper around
    baselines.load_into."""
    return baselines_mod.load_into(registry, history, BASELINES_PATH)


def _publisher_broadcast(device_name: str | None = None) -> None:
    # Twins call this on every state edge. We resolve the class so the
    # publisher only pushes to apps of that class — a Tasmota toggle
    # doesn't churn unrelated Shelly app instances. Force=True so we
    # don't suppress edges with hash-equal payloads (e.g. a quick
    # toggle that lands back where it started before the plug echoes).
    only_class = None
    if device_name:
        twin = registry.get(device_name)
        if twin is not None:
            only_class = twin.cls.name
    publisher.broadcast(device_name, only_class=only_class, force=True)


# --- Construct everything ------------------------------------------------

webxdc = WebxdcIO(state_dir=STATE_DIR, devices_dir=DEVICES_DIR)
history = History(
    db_path=STATE_DIR / "history.sqlite",
    retention_days=int(
        (os.environ.get("RETENTION_DAYS") or "0").strip() or "0"
    ),
)

# Build one twin per device. TwinDeps wires up the side effects.
_deps = TwinDeps(
    mqtt_publish=_mqtt_publish,
    post_to_chats=_post_to_visible_chats,
    broadcast=_publisher_broadcast,
    save_rules=_save_rules,
    save_baselines=_save_baselines,
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

# Re-seed re-entrancy guard: a push that fails on a stale msgid triggers
# send_apps, whose own seeding pushes could fail again for the same chat.
# One in-flight re-seed per chat; concurrent/repeat failures coalesce.
_reseed_lock = threading.Lock()
_reseeding: set[int] = set()


def _reseed_stale_chat(chat_id: int, msgid: int) -> None:
    """A push to `msgid` failed because the message is gone (aged out under
    delete_device_after, or user-deleted). Re-deliver the chat's apps and
    seed the fresh instances, so pushes resume without a manual /apps."""
    with _reseed_lock:
        if chat_id in _reseeding:
            return
        _reseeding.add(chat_id)
    try:
        visible = registry.visible_classes_for(chat_id, ALLOWED_CHATS)
        if not visible:
            return
        log.info("msgid=%d gone in chat=%d; re-seeding apps", msgid, chat_id)
        sent, _ = webxdc.send_apps(state.bot, state.accid, chat_id, visible)
        for cls in sent:
            new_msgid = webxdc.map_snapshot().get(chat_id, {}).get(cls)
            if new_msgid is not None:
                publisher.push_unicast(chat_id, new_msgid, cls)
    finally:
        with _reseed_lock:
            _reseeding.discard(chat_id)


def _publisher_send(chat_id: int, msgid: int, payload: dict) -> bool:
    return webxdc.push_to_msgid(
        state.bot, state.accid, msgid, payload,
        on_missing=lambda: _reseed_stale_chat(chat_id, msgid),
    )


publisher = Publisher(
    build=lambda chat_id, class_name: snap_mod.build_for_chat(
        chat_id, class_name, registry, ALLOWED_CHATS,
    ),
    msgids=lambda: webxdc.map_snapshot(),
    send=_publisher_send,
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
    # os._exit instead of sys.exit: deltabot-cli's main thread is blocked
    # in a transport loop that doesn't propagate SystemExit cleanly, so
    # sys.exit was leaving the process alive until systemd's SIGKILL
    # (~90s after SIGTERM). os._exit terminates immediately — safe here
    # because we've already flushed everything we own.
    os._exit(0)


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
    if cancelled:
        return True, f"cancelled {len(cancelled)} schedule(s) for {device_name}"
    # No-op cancel: distinguish "device has no rules at all" from
    # "device has rules but none matched" (e.g. duplicate × clicks
    # in the app sending the same rule_id twice). Otherwise the
    # second call's reply misleadingly implies all rules are gone.
    remaining = len(twin.jobs_snapshot())
    if remaining:
        return True, (f"no matching schedule for {device_name} "
                      f"({remaining} rule(s) still active)")
    return True, f"no pending schedule for {device_name}"


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
        "  /<device> reset-counter            — zero the resettable kWh counter\n"
        "  /<device> export 7d                — CSV of power + energy history\n"
        "  /<device> rules                    — list this device's rules\n"
        "  /rules               — list rules for every visible device\n"
        "  /refresh             — push fresh state to every open webxdc app in this chat\n"
        "  /<device> refresh    — push fresh state for that device's class only\n"
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

    # Telemetry: device may be unset; record + return. Class resolves
    # from msgid (same as refresh). Never let a bad payload escape into
    # the rest of the handler.
    if action == "telemetry":
        _record_telemetry(chat_id, msgid, cls_for_msg, request)
        return

    if not device_name or not action:
        return

    # Whitelist of actions we know about. Anything else (including the
    # pre-v0.2 history/events/set_param) silently drops — old app
    # instances in the chat will keep retrying until /apps replaces them.
    _KNOWN = ("on", "off", "toggle", "status",
              "auto-off", "auto-on",
              "cancel-auto-off", "cancel-auto-on", "cancel-schedule",
              "reset-counter")
    if action not in _KNOWN:
        log.debug("ignoring webxdc action %r (chat=%d msgid=%d)",
                  action, chat_id, msgid)
        return

    if action == "reset-counter":
        _reset_counter(chat_id, device_name)
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
    """Build a ScheduledPolicy from a webxdc app payload subobject.

    The app speaks **minutes** at this boundary; we convert to seconds
    once here and feed the engine its native unit downstream.
    """
    defaults = _defaults_from_section(section)
    policy = rules_mod.ScheduledPolicy()
    timer_m = raw.get("timer_minutes")
    if isinstance(timer_m, (int, float)) and timer_m > 0:
        policy.timer_seconds = int(round(float(timer_m) * 60))
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
        idle_m = idle.get("duration_minutes")
        if isinstance(idle_m, (int, float)):
            policy.idle_duration_s = int(round(float(idle_m) * 60))
        else:
            policy.idle_duration_s = defaults.idle_duration_s
    consumed = raw.get("consumed")
    if isinstance(consumed, dict):
        policy.consumed_field = str(consumed.get("field", defaults.consumed_field))
        policy.consumed_threshold_wh = float(consumed.get(
            "threshold_wh", defaults.consumed_threshold_wh))
        cons_m = consumed.get("window_minutes")
        if isinstance(cons_m, (int, float)):
            policy.consumed_window_s = int(round(float(cons_m) * 60))
        else:
            policy.consumed_window_s = defaults.consumed_window_s
    avg = raw.get("avg")
    if isinstance(avg, dict):
        policy.avg_field = str(avg.get("field", defaults.avg_field))
        policy.avg_threshold_w = float(avg.get(
            "threshold_w", defaults.avg_threshold_w))
        avg_m = avg.get("window_minutes")
        if isinstance(avg_m, (int, float)):
            policy.avg_window_s = int(round(float(avg_m) * 60))
        else:
            policy.avg_window_s = defaults.avg_window_s
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
            avg_field=section.default_avg_field,
            avg_threshold_w=section.default_avg_threshold_w,
            avg_window_s=section.default_avg_window_s,
        )
    return rules_mod.PolicyDefaults()


def _reset_counter(chat_id: int, device_name: str) -> bool:
    """Reset the resettable counter on a device. Auth + twin lookup +
    twin.reset_counter(). Used by both the `/<dev> reset-counter`
    chat command and the app's ↺ button."""
    twin = registry.get(device_name)
    if twin is None or not twin.can_chat_see(chat_id, ALLOWED_CHATS):
        return False
    twin.reset_counter()
    return True


def _record_telemetry(chat_id: int, msgid: int, class_name: str,
                      request: dict) -> None:
    """Persist one app_telemetry row. The webxdc app posts a `telemetry`
    payload ~2s after boot with cold-start timings + replay counters;
    used to monitor offline-cold-start performance server-side.

    Best-effort: any error is logged and swallowed. Replay protection
    (MAX_APP_AGE_SECONDS, MAX_CLOCK_SKEW_SECONDS) is enforced upstream
    in _on_webxdc_update before this is called."""
    metrics = request.get("metrics")
    if not isinstance(metrics, dict):
        log.debug("telemetry from chat=%d msgid=%d missing/bad metrics",
                  chat_id, msgid)
        return
    try:
        history.record_app_telemetry(
            ts=int(request.get("ts") or time.time()),
            chat_id=chat_id,
            msgid=msgid,
            class_name=class_name,
            metrics=metrics,
        )
    except Exception:
        log.exception("record_app_telemetry failed (chat=%d msgid=%d)",
                      chat_id, msgid)
        return
    log.info(
        "app_telemetry chat=%d msgid=%d cls=%s cold=%s replay=%s "
        "replay_ms=%s hydrate_ms=%s first_render_ms=%s "
        "nav_head=%s nav_script=%s nav_render=%s nav_paint=%s "
        "nav_load=%s nav_listener=%s "
        "cache=%sB serial=%s build=%s",
        chat_id, msgid, class_name,
        metrics.get("cold_start"),
        metrics.get("replay_count"),
        metrics.get("replay_total_ms"),
        metrics.get("cache_hydrate_ms"),
        metrics.get("first_render_ms"),
        metrics.get("nav_to_head_ms"),
        metrics.get("nav_to_script_ms"),
        metrics.get("nav_to_render_ms"),
        metrics.get("nav_to_paint_ms"),
        metrics.get("nav_to_load_ms"),
        metrics.get("nav_to_listener_ms"),
        metrics.get("cache_size_bytes"),
        metrics.get("start_serial"),
        metrics.get("app_build_ts"),
    )


def _refresh_chat(chat_id: int, only_class: str | None = None) -> int:
    """Push a fresh snapshot to every webxdc instance in this chat
    (filtered to `only_class` if given). Returns the count pushed.
    Used by /refresh / /<dev> refresh chat commands."""
    pushed = 0
    for cls_name, msgid in webxdc.map_snapshot().get(chat_id, {}).items():
        if only_class is not None and cls_name != only_class:
            continue
        if publisher.push_unicast(chat_id, msgid, cls_name):
            pushed += 1
    return pushed


def _resolve_cancel_target(device_name: str, verb: str) -> str | None:
    twin = registry.get(device_name)
    if twin is None:
        return None
    if verb == "cancel-auto-off" and twin.cls.auto_off:
        return twin.cls.auto_off.command
    if verb == "cancel-auto-on" and twin.cls.auto_on:
        return twin.cls.auto_on.command
    return None  # cancel-schedule: drop all rules for the device


# --- Formatters / commands / replay-windows are imported from
# mqtt_bot.formatters and mqtt_bot.commands. Module-level aliases keep
# the call sites in this file readable.

_format_device_line = lambda twin: formatters.format_device_line(  # noqa: E731
    twin, multi_class=len(cfg.classes) > 1)
_format_rule_lines = formatters.format_rule_lines
_rule_clauses = formatters.rule_clauses

_GLOBAL_VERBS = commands_mod.GLOBAL_VERBS
_DIRECT_VERBS = commands_mod.DIRECT_VERBS
_CANCEL_VERBS = commands_mod.CANCEL_VERBS
_SCHEDULE_VERBS = commands_mod.SCHEDULE_VERBS
_sanitize = commands_mod.sanitize
_parse_text_command = commands_mod.parse_text_command

MAX_AGE_SECONDS = commands_mod.MAX_AGE_SECONDS
MAX_APP_AGE_SECONDS = commands_mod.MAX_APP_AGE_SECONDS
MAX_CLOCK_SKEW_SECONDS = commands_mod.MAX_CLOCK_SKEW_SECONDS
EXPORT_MAX_WINDOW_S = commands_mod.EXPORT_MAX_WINDOW_S
EXPORT_MAX_ROWS = commands_mod.EXPORT_MAX_ROWS


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
        if verb == "refresh":
            n = _refresh_chat(chatid)
            try:
                bot.rpc.send_reaction(accid, msg.id, ["🆗" if n else "⚠️"])
            except Exception:
                pass
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
    if verb == "refresh" and not rest:
        # /<dev> refresh: same effect as /refresh — pushes the snapshot
        # for the device's class. Useful when the user has the chat focused
        # and wants the app updated without opening it.
        twin = registry.get(device_name)
        if twin is None or not twin.can_chat_see(chatid, ALLOWED_CHATS):
            bot.rpc.send_msg(accid, chatid,
                             MsgData(text=f"unknown device: {device_name}"))
            return
        _refresh_chat(chatid, only_class=twin.cls.name)
        try:
            bot.rpc.send_reaction(accid, msg.id, ["🆗"])
        except Exception:
            pass
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

    if verb == "reset-counter" and not rest:
        ok = _reset_counter(chatid, device_name)
        if not ok:
            bot.rpc.send_msg(accid, chatid,
                             MsgData(text=f"unknown device or no permission: {device_name}"))
            return
        try:
            bot.rpc.send_reaction(accid, msg.id, ["🆗"])
        except Exception:
            pass
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
    """Dump power_minute + samples_raw for a device to a CSV attachment.
    Row shaping lives in mqtt_bot.csv_export so the column layout is
    testable."""
    import csv
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
    if window_seconds > EXPORT_MAX_WINDOW_S:
        bot.rpc.send_msg(accid, chatid, MsgData(
            text=f"export window too long ({window_str}); "
                 f"max is {EXPORT_MAX_WINDOW_S // 86400}d"))
        return
    until_ts = int(time.time())
    since_ts = until_ts - window_seconds
    power_rows = history.query_power_raw(device_name, since_ts, until_ts,
                                         limit=EXPORT_MAX_ROWS)
    samples_rows = history.query_samples_raw(device_name, since_ts, until_ts,
                                             limit=EXPORT_MAX_ROWS)
    total_samples = history.count_samples_raw(device_name, since_ts, until_ts)
    truncated = total_samples > len(samples_rows)

    if not (power_rows or samples_rows):
        bot.rpc.send_msg(accid, chatid,
                         MsgData(text=f"no history yet for {device_name} ({window_str})"))
        return

    fd, path = tempfile.mkstemp(suffix=".csv",
                                 prefix=f"{device_name}-{window_str}-")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(csv_export.HEADER)
            for row in power_rows:
                w.writerow(csv_export.power_minute_row(device_name, row))
            for row in samples_rows:
                w.writerow(csv_export.samples_raw_row(device_name, row))
        note = (f" · TRUNCATED to the newest {EXPORT_MAX_ROWS} of "
                f"{total_samples} status updates" if truncated else "")
        bot.rpc.send_msg(
            accid, chatid,
            MsgData(file=path,
                    text=f"{device_name} export · {window_str} · "
                         f"{len(samples_rows)} status updates · "
                         f"{len(power_rows)} per-min{note}"),
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

    # Account config a backup import doesn't set for us. `bot=1` so unknown
    # contacts are delivered + group events arrive; `delete_device_after`
    # bounds dc.db by pruning old messages (incl. the webxdc status-update
    # carriers the Publisher emits). Never let a config-set failure abort
    # startup.
    try:
        dc_config.ensure_bot_mode(bot.rpc, accid)
        dc_config.apply_retention(bot.rpc, accid, DC_DELETE_DEVICE_AFTER_DAYS)
    except Exception:
        bot.logger.exception("failed to apply Delta Chat account config")

    # Restore persisted rules onto twins, then backfill consumed/idle
    # evaluation buffers from history so they don't have to wait a fresh
    # window before being able to fire.
    rules_mod.load_into(registry, RULES_PATH)
    _rehydrate_rules_from_history()

    # Restore resettable-counter baselines (samples_raw + aenergy_offset_events
    # are persistent in SQLite; nothing else to load).
    _load_baselines()

    mqtt.start()
    sweeper.start()
    publisher.start()

    bot.logger.info(
        "mqtt-bot up; classes=%s devices=%s allowed_chats=%s "
        "publish_interval=%ds delete_device_after=%dd",
        sorted(cfg.classes), sorted(cfg.devices), sorted(ALLOWED_CHATS),
        PUBLISH_INTERVAL_S, DC_DELETE_DEVICE_AFTER_DAYS,
    )
    if not ALLOWED_CHATS:
        bot.logger.warning(
            "ALLOWED_CHATS is empty; every command (except /id) will be denied. "
            "Use /id in the target chat and add the returned id."
        )


def _rehydrate_rules_from_history() -> None:
    rehydrate_mod.rehydrate_rules_from_history(registry, history)


if __name__ == "__main__":
    cli.start()
