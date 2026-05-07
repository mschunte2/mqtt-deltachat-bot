"""Generic Delta Chat ↔ MQTT bridge engine.

Knows about device classes (data, loaded from devices/<class>/class.json),
not about Shelly specifically. Owns:
  - state cache (fields per device)
  - threshold detector (per (device, field))
  - subscription planner (reverse topic lookup)
  - dispatch (chat command → MQTT publish)
  - inbound routing (MQTT → chat events + webxdc + scheduler tick)
  - on_fire callback for the scheduler (auto-action firing)
  - per-chat webxdc snapshot builder

bot.py wires this up; mqtt_client and webxdc_io take callbacks pointing
back to Engine methods.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from deltachat2 import MsgData

import permissions
import scheduler as sched_mod
import state as state_mod
import templating
from config import (AutoOffConfig, AutoOnConfig, ChatEventRule, Config, Device,
                    DeviceClass)

log = logging.getLogger("mqtt_bot.engine")


@dataclass
class _ThresholdState:
    above_since: int | None = None
    active: bool = False


@dataclass
class Engine:
    cfg: Config
    allowed_chats: set[int]
    mqtt: Any            # MqttClient
    webxdc: Any          # WebxdcIO
    scheduler: Any       # Scheduler
    client_id: str

    # Filled in by set_bot() during on_start
    bot: Any = None
    accid: int = 0

    # State (engine-owned)
    _states: dict[str, state_mod.DeviceState] = field(default_factory=dict)
    _thresholds: dict[tuple[str, str], _ThresholdState] = field(default_factory=dict)
    _topic_lookup: dict[str, tuple[str, str]] = field(default_factory=dict)

    # --- lifecycle --------------------------------------------------------

    def __post_init__(self) -> None:
        for name in self.cfg.devices:
            self._states[name] = state_mod.DeviceState()
        self._build_topic_lookup()

    def set_bot(self, bot, accid: int) -> None:
        self.bot = bot
        self.accid = accid

    def _build_topic_lookup(self) -> None:
        self._topic_lookup.clear()
        for d in self.cfg.devices.values():
            cls = self.cfg.device_class(d)
            for sub in cls.subscribe:
                self._topic_lookup[f"{d.topic_prefix}/{sub.suffix}"] = (d.name, sub.suffix)

    # --- MQTT integration -------------------------------------------------

    def subscriptions_for(self) -> list[str]:
        return sorted(self._topic_lookup.keys())

    def on_mqtt_message(self, topic: str, payload: bytes) -> None:
        ds = self._topic_lookup.get(topic)
        if ds is None:
            return  # not subscribed to this topic
        device_name, suffix = ds
        device = self.cfg.devices[device_name]
        cls = self.cfg.device_class(device)
        log.debug("inbound %s -> %s/%s", topic, device_name, suffix)

        updates = state_mod.extract(cls, suffix, payload)
        if not updates:
            return

        cache = self._states[device_name]
        prev = {k: cache.get(k) for k in updates}
        for k, v in updates.items():
            cache.set(k, v)
        cache.last_update_ts = int(time.time())

        # Chat-event rules touching any changed field
        for rule in cls.chat_events:
            if rule.field not in updates:
                continue
            if rule.type == "on_change":
                self._fire_on_change(device, rule, prev.get(rule.field), updates[rule.field])
            elif rule.type == "threshold":
                self._fire_threshold(device, rule, updates[rule.field])

        # Scheduler idle/consumed checks (no-op if no pending jobs for this device)
        self.scheduler.tick(device_name, cache.fields)

        # Push the new snapshot to webxdc instances
        if self.bot:
            self.webxdc.push_filtered(self.bot, self.accid, self.snapshot_for)

    # --- chat-event helpers ----------------------------------------------

    def _fire_on_change(self, device: Device, rule: ChatEventRule,
                        prev: Any, new: Any) -> None:
        if prev == new:
            return
        key = self._coerce_value_key(new)
        template = rule.values.get(key)
        if not template:
            return
        ctx = {"name": device.name, "value": new, "field": rule.field}
        self._post_to_visible_chats(device, templating.render(template, ctx))

    def _fire_threshold(self, device: Device, rule: ChatEventRule, value: Any) -> None:
        if not isinstance(value, (int, float)):
            return
        # Resolve per-device thresholds (skip rule entirely if device omits them)
        limit = device.params.get(rule.limit_param)
        duration = device.params.get(rule.duration_param)
        if limit is None or duration is None:
            return
        ts_state = self._thresholds.setdefault((device.name, rule.field), _ThresholdState())
        now = int(time.time())
        if value >= limit:
            if ts_state.above_since is None:
                ts_state.above_since = now
            elif (not ts_state.active
                  and (now - ts_state.above_since) >= int(duration)):
                ts_state.active = True
                ctx = {"name": device.name, "value": float(value),
                       "seconds": now - ts_state.above_since, "field": rule.field}
                self._post_to_visible_chats(device, templating.render(rule.above, ctx))
        else:
            if ts_state.active:
                ctx = {"name": device.name, "value": float(value), "field": rule.field}
                self._post_to_visible_chats(device, templating.render(rule.below, ctx))
            ts_state.above_since = None
            ts_state.active = False

    @staticmethod
    def _coerce_value_key(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        return str(v).lower()

    # --- chat-side dispatch ----------------------------------------------

    def dispatch_command(self, chat_id: int, device_name: str, action: str,
                         *, source_msgid: int | None = None) -> tuple[bool, str]:
        """Run a direct command (on/off/toggle/status). Returns (ok, message)."""
        device = self.cfg.devices.get(device_name)
        if device is None:
            return False, f"unknown device: {device_name}"
        if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
            return False, "permission denied"
        cls = self.cfg.device_class(device)
        if action not in cls.commands:
            return False, f"unknown action: {action} (try: {', '.join(sorted(cls.commands))})"

        cmd = cls.commands[action]
        topic = f"{device.topic_prefix}/{cmd.suffix}"
        payload = templating.render(cmd.payload, {"client_id": self.client_id})
        self.mqtt.publish(topic, payload)
        log.info("dispatch %s/%s -> %s", device_name, action, topic)

        # Manual-override: cancel any pending job whose target_action == this action.
        # (A pending auto-on isn't cancelled by manual /off; only same-direction.)
        for job in self.scheduler.cancel(device_name, target_action=action):
            section = self._auto_section_for(cls, action)
            if section:
                tpl = section.trigger_messages.get("cancelled_manual")
                if tpl:
                    self._post_to_visible_chats(
                        device, templating.render(tpl, {"name": device_name})
                    )

        if source_msgid is not None and self.bot:
            try:
                self.bot.rpc.send_reaction(self.accid, source_msgid, ["🆗"])
            except Exception:
                log.exception("send_reaction failed")
        return True, ""

    def schedule(self, chat_id: int, device_name: str,
                 target_action: str, policy: sched_mod.ScheduledPolicy) -> tuple[bool, str]:
        """Add a scheduled job. Caller has already parsed the policy."""
        device = self.cfg.devices.get(device_name)
        if device is None:
            return False, f"unknown device: {device_name}"
        if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
            return False, "permission denied"
        cls = self.cfg.device_class(device)
        if target_action not in cls.commands:
            return False, f"action {target_action!r} not supported by class {cls.name}"
        job = sched_mod.ScheduledJob.from_policy(
            policy, device_name, chat_id, target_action, int(time.time())
        )
        self.scheduler.schedule(job)
        return True, self._format_schedule_ack(device_name, target_action, job)

    def cancel_schedule(self, chat_id: int, device_name: str,
                        target_action: str | None = None) -> tuple[bool, str]:
        device = self.cfg.devices.get(device_name)
        if device is None:
            return False, f"unknown device: {device_name}"
        if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
            return False, "permission denied"
        cancelled = self.scheduler.cancel(device_name, target_action=target_action)
        if not cancelled:
            return True, f"no pending schedule for {device_name}"
        return True, f"cancelled {len(cancelled)} schedule(s) for {device_name}"

    def list_devices(self, chat_id: int) -> str:
        visible = permissions.visible_devices(chat_id, self.cfg, self.allowed_chats)
        if not visible:
            return "no devices visible to this chat"
        lines: list[str] = []
        for d in visible:
            lines.append(self._format_device_line(d))
        return "\n".join(lines)

    def status_for(self, chat_id: int, device_name: str) -> str:
        device = self.cfg.devices.get(device_name)
        if device is None:
            return f"unknown device: {device_name}"
        if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
            return "permission denied"
        return self._format_device_line(device)

    def help_text(self, chat_id: int) -> str:
        visible = permissions.visible_devices(chat_id, self.cfg, self.allowed_chats)
        names = ", ".join(d.name for d in visible) or "(none — your chat has no visible devices)"
        return (
            "Commands:\n"
            "  /<device> on | off | toggle | status\n"
            "  /<device> off in 30m | off at 18h | off at 18:30 daily\n"
            "  /<device> off if idle              # power<5W for 60s (defaults)\n"
            "  /<device> off if idle 10W 120s     # power<10W for 120s\n"
            "  /<device> off if idle 10Wh in 2m   # energy<10Wh in last 2 min (rolling)\n"
            "  /<device> on for 30m               # on now + auto-off in 30 min\n"
            "  /<device> on for 1h or if idle     # on now + auto-off (timer or idle)\n"
            "  /<device> auto-on at 7h | at 7h daily\n"
            "  /<device> cancel-auto-off | cancel-auto-on | cancel-schedule\n"
            "  /list                — list devices visible to this chat\n"
            "  /apps                — (re)deliver webxdc control apps\n"
            "  /id                  — show this chat's id\n"
            "  /help                — this message\n"
            f"Devices in this chat: {names}\n"
        )

    # --- scheduler & webxdc callbacks ------------------------------------

    def on_fire(self, device_name: str, chat_id_origin: int,
                target_action: str, mode: str, ctx: dict[str, Any]) -> None:
        """Called by Scheduler when a job's policy trips."""
        device = self.cfg.devices.get(device_name)
        if device is None:
            return
        cls = self.cfg.device_class(device)
        cmd = cls.commands.get(target_action)
        if cmd is None:
            log.error("scheduler fired for unknown action %s on %s",
                      target_action, device_name)
            return
        topic = f"{device.topic_prefix}/{cmd.suffix}"
        payload = templating.render(cmd.payload, {"client_id": self.client_id})
        self.mqtt.publish(topic, payload)
        log.info("scheduler fire %s/%s mode=%s -> %s",
                 device_name, target_action, mode, topic)
        section = self._auto_section_for(cls, target_action)
        if section:
            template = section.trigger_messages.get(mode)
            if template:
                full_ctx = {"name": device_name, **ctx}
                self._post_to_visible_chats(device, templating.render(template, full_ctx))

    def snapshot_for(self, chat_id: int, class_name: str) -> dict[str, Any] | None:
        if not permissions.is_allowed(chat_id, self.allowed_chats):
            return None
        visible = [
            d for d in self.cfg.devices.values()
            if d.class_name == class_name
            and permissions.chat_can_see(chat_id, d, self.allowed_chats)
        ]
        if not visible:
            return None
        devices_payload: dict[str, Any] = {}
        for d in visible:
            st = self._states.get(d.name)
            jobs = self.scheduler.jobs_for_device(d.name)
            devices_payload[d.name] = {
                "name": d.name,
                "description": d.description,
                "fields": dict(st.fields) if st else {},
                "last_update_ts": st.last_update_ts if st else 0,
                "scheduled_jobs": [j.to_snapshot() for j in jobs],
            }
        return {
            "class": class_name,
            "devices": devices_payload,
            "server_ts": int(time.time()),
        }

    # --- webxdc inbound (called by bot.py from RawEvent hook) ------------

    def handle_webxdc_request(self, chat_id: int, msgid: int,
                              request: dict[str, Any]) -> None:
        """Process a webxdc app's update payload['request']."""
        cls_for_msg = self.webxdc.class_for_msgid(chat_id, msgid)
        if cls_for_msg is None:
            log.warning("webxdc update from unregistered msgid=%d in chat=%d "
                        "(run /apps to register)", msgid, chat_id)
            return
        device_name = str(request.get("device", "")).strip().lower()
        action = str(request.get("action", "")).strip().lower()
        if not device_name or not action:
            return

        if action == "cancel-auto-off" or action == "cancel-auto-on" or action == "cancel-schedule":
            target = None
            device = self.cfg.devices.get(device_name)
            cls = self.cfg.device_class(device) if device else None
            if cls and action == "cancel-auto-off" and cls.auto_off:
                target = cls.auto_off.command
            elif cls and action == "cancel-auto-on" and cls.auto_on:
                target = cls.auto_on.command
            ok, msg = self.cancel_schedule(chat_id, device_name, target_action=target)
            if msg and self.bot:
                self.bot.rpc.send_msg(self.accid, chat_id, MsgData(text=msg))
            return

        # Direct action with optional inline policy from the app.
        # The app may include {"auto_off": {...}} or {"auto_on": {...}} alongside the
        # action — the bot handles both as a follow-up schedule call.
        ok, msg = self.dispatch_command(chat_id, device_name, action)
        if not ok and msg and self.bot:
            self.bot.rpc.send_msg(self.accid, chat_id, MsgData(text=msg))
            return

        for key, target_action_attr in (("auto_off", "auto_off"), ("auto_on", "auto_on")):
            extra = request.get(key)
            if not isinstance(extra, dict):
                continue
            device = self.cfg.devices.get(device_name)
            cls = self.cfg.device_class(device) if device else None
            section = getattr(cls, target_action_attr, None) if cls else None
            if section is None:
                continue
            try:
                policy = self._policy_from_request(extra, section)
            except ValueError as ex:
                if self.bot:
                    self.bot.rpc.send_msg(self.accid, chat_id,
                                          MsgData(text=f"bad {key}: {ex}"))
                continue
            self.schedule(chat_id, device_name, section.command, policy)

    # --- helpers ----------------------------------------------------------

    @staticmethod
    def _auto_section_for(cls: DeviceClass, action: str):
        if cls.auto_off and cls.auto_off.command == action:
            return cls.auto_off
        if cls.auto_on and cls.auto_on.command == action:
            return cls.auto_on
        return None

    def _post_to_visible_chats(self, device: Device, text: str) -> None:
        if self.bot is None:
            log.warning("no bot ref; would post to %s: %s", device.name, text)
            return
        chats = device.allowed_chats or tuple(sorted(self.allowed_chats))
        for chat_id in chats:
            try:
                self.bot.rpc.send_msg(self.accid, chat_id, MsgData(text=text))
            except Exception:
                log.exception("post to chat %d failed", chat_id)

    def _format_device_line(self, device: Device) -> str:
        cls = self.cfg.device_class(device)
        st = self._states.get(device.name)
        f = st.fields if st else {}
        online = f.get("online")
        output = f.get("output")
        apower = f.get("apower")
        aenergy = f.get("aenergy")
        bits: list[str] = [device.name]
        bits.append("🟢" if online else "🔴" if online is False else "⚪")
        if isinstance(output, bool):
            bits.append("ON" if output else "OFF")
        elif output is None:
            bits.append("?")
        if isinstance(apower, (int, float)):
            bits.append(f"{apower:.0f}W")
        if isinstance(aenergy, (int, float)):
            bits.append(f"({aenergy / 1000.0:.2f} kWh)")
        if device.description:
            bits.append(f"— {device.description}")
        # Append pending schedules.
        jobs = self.scheduler.jobs_for_device(device.name)
        if jobs:
            for job in jobs:
                action = job.target_action
                if job.deadline_ts:
                    remaining = max(0, job.deadline_ts - int(time.time()))
                    bits.append(f"[{action} in {_fmt_secs(remaining)}]")
                elif job.has_idle():
                    bits.append(f"[{action} on idle]")
                elif job.has_consumed():
                    bits.append(f"[{action} on used<Wh]")
        # Suppress class line for single-class deployments? Keep it; useful when mixed.
        if len(self.cfg.classes) > 1:
            bits.append(f"[{cls.name}]")
        return " ".join(bits)

    def _format_schedule_ack(self, device_name: str, target_action: str,
                             job: sched_mod.ScheduledJob) -> str:
        bits = [f"scheduled {device_name} {target_action}"]
        if job.deadline_ts:
            remaining = max(0, job.deadline_ts - int(time.time()))
            if job._time_mode == "tod" and job.time_of_day:
                h, m = job.time_of_day
                suffix = " daily" if job.recurring_tod else ""
                bits.append(f"at {h:02d}:{m:02d}{suffix} (in {_fmt_secs(remaining)})")
            else:
                bits.append(f"in {_fmt_secs(remaining)}")
        if job.has_idle():
            bits.append(f"or when {job.idle_field}<{job.idle_threshold:g}W "
                        f"for {_fmt_secs(job.idle_duration_s)}")
        if job.has_consumed():
            bits.append(f"or when used<{job.consumed_threshold_wh:g}Wh "
                        f"in {_fmt_secs(job.consumed_window_s)}")
        return " ".join(bits)

    def _policy_from_request(self, raw: dict, section) -> sched_mod.ScheduledPolicy:
        """Build a ScheduledPolicy from a webxdc request payload subobject.

        Keys we accept (any subset, OR-combined):
          {"timer_seconds": 1800}
          {"time_of_day": [18, 30], "recurring_tod": false}
          {"idle": {"field":"apower","threshold":5,"duration_s":60}}
          {"consumed": {"field":"apower","threshold_wh":5,"window_s":600}}
        """
        defaults = _defaults_from_section(section)
        policy = sched_mod.ScheduledPolicy()
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
        if policy.is_empty():
            raise ValueError("no policies supplied")
        return policy


# --- module-level helpers -------------------------------------------------

def _fmt_secs(s: int) -> str:
    if s < 60:
        return f"{s}s"
    if s < 3600:
        m, ss = divmod(s, 60)
        return f"{m}m{ss}s" if ss else f"{m}m"
    h, rem = divmod(s, 3600)
    m = rem // 60
    return f"{h}h{m}m" if m else f"{h}h"


def _defaults_from_section(section: AutoOffConfig | AutoOnConfig | None) -> sched_mod.PolicyDefaults:
    if isinstance(section, AutoOffConfig):
        return sched_mod.PolicyDefaults(
            idle_field=section.default_idle_field,
            idle_threshold=section.default_idle_threshold,
            idle_duration_s=section.default_idle_duration,
            consumed_field=section.default_consumed_field,
            consumed_threshold_wh=section.default_consumed_threshold_wh,
            consumed_window_s=section.default_consumed_window_s,
        )
    return sched_mod.PolicyDefaults()


def policy_defaults_for_section(section) -> sched_mod.PolicyDefaults:
    """Public helper for bot.py to build PolicyDefaults from a class section."""
    return _defaults_from_section(section)
