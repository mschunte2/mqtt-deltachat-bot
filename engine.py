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

    # Optional free-text intro prepended to /help (from HELP_MESSAGE env var).
    help_prefix: str = ""

    # Optional History instance for SQLite-backed time series.
    history: Any = None

    # Path to devices.json — used by the "Save" button on threshold tuning
    # to persist param overrides across bot restarts. Optional; if None,
    # the persist flag on set_param is silently ignored.
    instances_path: Any = None  # str | Path | None

    # Filled in by set_bot() during on_start
    bot: Any = None
    accid: int = 0

    # State (engine-owned)
    _states: dict[str, state_mod.DeviceState] = field(default_factory=dict)
    _thresholds: dict[tuple[str, str], _ThresholdState] = field(default_factory=dict)
    _topic_lookup: dict[str, tuple[str, str]] = field(default_factory=dict)
    # In-memory overrides for device.params (e.g. power_threshold_watts);
    # surface in /apps snapshot, set via webxdc {action:"set_param"}. Lost
    # on bot restart by design — persistence is via devices.json.
    _param_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)

    # --- lifecycle --------------------------------------------------------

    def __post_init__(self) -> None:
        for name in self.cfg.devices:
            self._states[name] = state_mod.DeviceState()
        self._build_topic_lookup()

    def set_bot(self, bot, accid: int) -> None:
        self.bot = bot
        self.accid = accid

    def rehydrate_rules_from_history(self) -> None:
        """After scheduler.load_persisted(), backfill consumed-rule sample
        buffers and idle-rule below-since timestamps from the SQLite
        history. Without this, every bot restart effectively reset rule
        timers — a 'off when used <5Wh in 10m' rule wouldn't fire for
        another 10 minutes after a restart, even if the actual last 10
        minutes had no power draw at all.

        Called from bot._on_start between scheduler.load_persisted() and
        scheduler.start(). The scheduler thread isn't running yet, so it's
        safe to mutate job state directly.
        """
        if self.history is None:
            return
        now = int(time.time())
        for job in self.scheduler.all_jobs():
            if job.has_consumed() and job.consumed_field == "apower":
                since = now - job.consumed_window_s
                rows = self.history.query_power_raw(job.device_name, since, now)
                if not rows:
                    continue
                for ts, apower, _output, _count in rows:
                    job._samples.append((ts, apower))
                # Mark the window as having been "tracked since" the
                # earliest sample we just loaded so integrate_wh evaluates
                # immediately on the next tick.
                job._consumed_started_at = rows[0][0]
                log.info("rehydrated consumed rule %s/%s with %d samples",
                         job.device_name, job.rule_id, len(rows))
            if job.has_idle() and job.idle_field == "apower":
                since = now - job.idle_duration_s
                rows = self.history.query_power_raw(job.device_name, since, now)
                if rows and all(r[1] < job.idle_threshold for r in rows):
                    job._below_since = rows[0][0]
                    log.info("rehydrated idle rule %s/%s — power has been "
                             "below %.1fW since %d (continuous in history)",
                             job.device_name, job.rule_id,
                             job.idle_threshold, rows[0][0])

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

        # Persist time series + plug events for /history queries.
        if self.history is not None:
            if "apower" in updates or "aenergy" in updates:
                self.history.write_sample(
                    device_name,
                    cache.last_update_ts,
                    cache.fields.get("apower"),
                    cache.fields.get("aenergy"),
                    cache.fields.get("output"),
                )
            # Lossless raw capture + by_minute extraction for status updates.
            if suffix == "status/switch:0":
                try:
                    payload_obj = json.loads(
                        payload.decode("utf-8", errors="replace")
                        if isinstance(payload, bytes) else str(payload)
                    )
                    if isinstance(payload_obj, dict):
                        self.history.record_status(
                            device_name, cache.last_update_ts, payload_obj,
                        )
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            if suffix == "events/rpc":
                payload_text = (payload.decode("utf-8", errors="replace")
                                if isinstance(payload, bytes) else str(payload))
                self.history.write_event(
                    device_name, cache.last_update_ts, suffix, payload_text
                )

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
        log.debug("on_change %s.%s prev=%r new=%r key=%s template=%s",
                  device.name, rule.field, prev, new, key, bool(template))
        if not template:
            return
        ctx = {"name": device.name, "value": new, "field": rule.field}
        self._post_to_visible_chats(device, templating.render(template, ctx))

    def _fire_threshold(self, device: Device, rule: ChatEventRule, value: Any) -> None:
        if not isinstance(value, (int, float)):
            return
        # Resolve per-device thresholds, with in-memory override taking
        # priority over devices.json. Skip rule entirely if neither set.
        overrides = self._param_overrides.get(device.name, {})
        limit = overrides.get(rule.limit_param, device.params.get(rule.limit_param))
        duration = overrides.get(rule.duration_param, device.params.get(rule.duration_param))
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
                        device,
                        templating.render(
                            tpl,
                            {"name": device_name, "action": action,
                             "action_verb": _action_verb(action)},
                        ),
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
                        target_action: str | None = None,
                        rule_id: str | None = None) -> tuple[bool, str]:
        device = self.cfg.devices.get(device_name)
        if device is None:
            return False, f"unknown device: {device_name}"
        if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
            return False, "permission denied"
        cancelled = self.scheduler.cancel(
            device_name, target_action=target_action, rule_id=rule_id
        )
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

    def list_rules(self, chat_id: int, device_name: str | None = None) -> str:
        """Pretty-print pending auto-on / auto-off rules.

        device_name=None  → every device visible to this chat.
        device_name=<x>   → just that device (with permission check).
        """
        if device_name is not None:
            device = self.cfg.devices.get(device_name)
            if device is None:
                return f"unknown device: {device_name}"
            if not permissions.chat_can_see(chat_id, device, self.allowed_chats):
                return "permission denied"
            devices = [device]
        else:
            devices = permissions.visible_devices(chat_id, self.cfg, self.allowed_chats)
        if not devices:
            return "no devices visible to this chat"
        lines: list[str] = []
        total = 0
        for d in devices:
            jobs = self.scheduler.jobs_for_device(d.name)
            if not jobs:
                continue
            lines.append(f"{d.name}:")
            for j in jobs:
                for line in self._format_rule_lines(j):
                    lines.append("  " + line)
                total += 1
        if not total:
            return "no rules pending"
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
        base = (
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
            "  /<device> export 7d                — CSV of power + energy history\n"
            "  /<device> rules                    — list this device's pending rules\n"
            "  /rules               — list pending rules for every visible device\n"
            "  /list                — list devices visible to this chat\n"
            "  /apps                — (re)deliver webxdc control apps\n"
            "  /id                  — show this chat's id\n"
            "  /help                — this message\n"
            f"Devices in this chat: {names}\n"
        )
        if self.help_prefix:
            return f"{self.help_prefix}\n\n{base}"
        return base

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

        # NOTE: we deliberately DO NOT suppress fires whose target state
        # already matches the cached output. In an OR-combined model the
        # user wants every rule that trips to be visible in chat (even if
        # redundant) — it confirms that rules are doing what they were
        # configured to do. Republishing an already-true command is a
        # cheap no-op on the plug.

        topic = f"{device.topic_prefix}/{cmd.suffix}"
        payload = templating.render(cmd.payload, {"client_id": self.client_id})
        self.mqtt.publish(topic, payload)
        log.info("scheduler fire %s/%s mode=%s -> %s",
                 device_name, target_action, mode, topic)
        section = self._auto_section_for(cls, target_action)
        if section:
            template = section.trigger_messages.get(mode)
            if template:
                full_ctx = {
                    "name": device_name,
                    "action": target_action,
                    "action_verb": _action_verb(target_action),
                    **ctx,
                }
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
            payload = {
                "name": d.name,
                "description": d.description,
                "fields": dict(st.fields) if st else {},
                "last_update_ts": st.last_update_ts if st else 0,
                "scheduled_jobs": [j.to_snapshot() for j in jobs],
            }
            if self.history is not None:
                summary = self._energy_summary(d.name, payload["fields"].get("aenergy"))
                if summary is not None:
                    payload["energy"] = summary
                # Last-30-days bar chart data.
                payload["daily_energy_wh"] = self.history.daily_energy_kwh(
                    d.name, _local_midnight(int(time.time())), days=30,
                )
                # Surface device-level params (incl. any in-memory overrides)
                # so the app can show + edit thresholds.
                params = dict(d.params)
                params.update(self._param_overrides.get(d.name, {}))
                payload["params"] = params
            devices_payload[d.name] = payload
        return {
            "class": class_name,
            "devices": devices_payload,
            "server_ts": int(time.time()),
        }

    def _energy_summary(self, device_name: str,
                        current_wh: float | None) -> dict[str, Any] | None:
        """kWh consumed in standard intervals, integrated from minute samples.

        Each interval is reported as a {kwh, partial_since_ts | null}
        pair. partial_since_ts indicates the earliest minute sample we
        actually have within the interval — if it's significantly later
        than the requested start, the kWh is for a shorter span than the
        label suggests (e.g. on first deploy, "last 24h" actually covers
        only since-deploy). The app surfaces this as a "*" suffix.

        Returns None if there's no current reading.
        """
        if self.history is None:
            return None
        now = int(time.time())
        intervals = (
            ("kwh_last_hour",  now - 3600),
            ("kwh_last_24h",   now - 86400),
            ("kwh_last_7d",    now - 7 * 86400),
            ("kwh_last_30d",   now - 30 * 86400),
            ("kwh_today",      _local_midnight(now)),
            ("kwh_this_week",  _local_week_start(now)),
            ("kwh_this_month", _local_month_start(now)),
        )
        out: dict[str, Any] = {
            "current_total_wh":
                float(current_wh) if current_wh is not None else None,
        }
        # Threshold for marking a value "partial": we tolerate up to 90s of
        # gap between requested start and earliest sample (one or two
        # missed minute boundaries on bot startup).
        PARTIAL_GAP = 90
        for key, since in intervals:
            wh, earliest = self.history.energy_consumed_in(
                device_name, since, now,
            )
            partial_since = (
                earliest if (earliest is not None and earliest - since > PARTIAL_GAP)
                else None
            )
            out[key] = {
                "kwh": wh / 1000.0,
                "partial_since_ts": partial_since,
            }
        return out

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

        # History query: read-only; reply only to the requesting msgid.
        if action == "history":
            self._handle_history_request(chat_id, msgid, device_name, request)
            return
        if action == "events":
            self._handle_events_request(chat_id, msgid, device_name, request)
            return
        if action == "set_param":
            self._handle_set_param_request(chat_id, device_name, request)
            return

        if action == "cancel-auto-off" or action == "cancel-auto-on" or action == "cancel-schedule":
            target = None
            device = self.cfg.devices.get(device_name)
            cls = self.cfg.device_class(device) if device else None
            if cls and action == "cancel-auto-off" and cls.auto_off:
                target = cls.auto_off.command
            elif cls and action == "cancel-auto-on" and cls.auto_on:
                target = cls.auto_on.command
            # The app may include a rule_id to delete one specific rule
            # (rather than every rule of that direction).
            rid = request.get("rule_id")
            rule_id = str(rid) if isinstance(rid, str) and rid else None
            ok, msg = self.cancel_schedule(
                chat_id, device_name, target_action=target, rule_id=rule_id
            )
            if msg and self.bot:
                self.bot.rpc.send_msg(self.accid, chat_id, MsgData(text=msg))
            return

        # Pure scheduling actions: action="auto-off" / "auto-on" with the
        # policy in a sibling key. Don't try to dispatch these as class
        # commands — they're not in cls.commands.
        if action in ("auto-off", "auto-on"):
            self._schedule_from_request(chat_id, device_name, action, request)
            return

        # Direct action with optional inline policy from the app.
        # The app may include {"auto_off": {...}} or {"auto_on": {...}} alongside
        # the action — the bot handles both as a follow-up schedule call.
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

    def _schedule_from_request(self, chat_id: int, device_name: str,
                                action: str, request: dict[str, Any]) -> None:
        """action ∈ {auto-off, auto-on}: pull the policy from the matching
        sibling key in the request body and schedule it. No dispatch."""
        direction = "off" if action == "auto-off" else "on"
        key = "auto_off" if action == "auto-off" else "auto_on"
        extra = request.get(key)
        if not isinstance(extra, dict):
            if self.bot:
                self.bot.rpc.send_msg(self.accid, chat_id,
                                      MsgData(text=f"missing {key} body"))
            return
        device = self.cfg.devices.get(device_name)
        if device is None:
            return
        cls = self.cfg.device_class(device)
        section = cls.auto_off if direction == "off" else cls.auto_on
        if section is None:
            if self.bot:
                self.bot.rpc.send_msg(self.accid, chat_id,
                                      MsgData(text=f"{action} not supported"))
            return
        try:
            policy = self._policy_from_request(extra, section)
        except ValueError as ex:
            if self.bot:
                self.bot.rpc.send_msg(self.accid, chat_id,
                                      MsgData(text=f"bad {key}: {ex}"))
            return
        self.schedule(chat_id, device_name, section.command, policy)

    def _handle_history_request(self, chat_id: int, msgid: int,
                                 device_name: str, request: dict[str, Any]) -> None:
        """Reply to a webxdc history query with downsampled time series."""
        if self.history is None or self.bot is None:
            return
        device = self.cfg.devices.get(device_name)
        if device is None or not permissions.chat_can_see(
                chat_id, device, self.allowed_chats):
            return
        try:
            window_seconds = int(request.get("window_seconds", 21600))
        except (TypeError, ValueError):
            window_seconds = 21600
        # Cap to ~31 days max.
        window_seconds = max(60, min(window_seconds, 31 * 86400))
        until_ts = int(time.time())
        since_ts = until_ts - window_seconds

        bucket_s, points = self.history.query_power(
            device_name, since_ts, until_ts, max_points=200,
        )
        energy = self.history.query_energy(device_name, since_ts, until_ts)
        # Authoritative total — hybrid (energy_minute first, power_minute
        # fallback). Doesn't depend on having ≥2 hourly snapshots in the
        # window, so it's correct even for 1-hour and shorter views.
        total_wh, _earliest = self.history.energy_consumed_in(
            device_name, since_ts, until_ts,
        )

        body = {
            "history": {
                "device": device_name,
                "window_seconds": window_seconds,
                "since_ts": since_ts,
                "until_ts": until_ts,
                "bucket_seconds": bucket_s,
                "power_points": points,
                "energy_points": energy,
                "total_wh": total_wh,
            }
        }
        self.webxdc.push_to_msgid(self.bot, self.accid, msgid, body)

    def _handle_events_request(self, chat_id: int, msgid: int,
                                device_name: str, request: dict[str, Any]) -> None:
        if self.history is None or self.bot is None:
            return
        device = self.cfg.devices.get(device_name)
        if device is None or not permissions.chat_can_see(
                chat_id, device, self.allowed_chats):
            return
        try:
            limit = max(1, min(int(request.get("limit", 50)), 200))
        except (TypeError, ValueError):
            limit = 50
        try:
            window_seconds = int(request.get("window_seconds", 7 * 86400))
        except (TypeError, ValueError):
            window_seconds = 7 * 86400
        until_ts = int(time.time())
        since_ts = until_ts - max(60, min(window_seconds, 31 * 86400))
        rows = self.history.query_events(device_name, since_ts, until_ts, limit=limit)
        body = {
            "events": {
                "device": device_name,
                "since_ts": since_ts,
                "until_ts": until_ts,
                "rows": [
                    {"ts": ts, "suffix": suffix, "kind": kind, "payload": payload[:512]}
                    for ts, suffix, kind, payload in rows
                ],
            }
        }
        self.webxdc.push_to_msgid(self.bot, self.accid, msgid, body)

    def _handle_set_param_request(self, chat_id: int, device_name: str,
                                   request: dict[str, Any]) -> None:
        device = self.cfg.devices.get(device_name)
        if device is None or not permissions.chat_can_see(
                chat_id, device, self.allowed_chats):
            return
        # Whitelist of params the app may tune at runtime.
        param = str(request.get("param", "")).strip()
        if param not in {"power_threshold_watts", "power_threshold_duration_s"}:
            log.warning("rejecting set_param for %r (not in whitelist)", param)
            return
        raw = request.get("value")
        try:
            # Coerce to int/float depending on canonical type from devices.json.
            cur = device.params.get(param)
            if isinstance(cur, int) or param.endswith("_s"):
                value = int(raw)
            else:
                value = float(raw)
            if value < 0:
                raise ValueError("negative")
        except (TypeError, ValueError):
            log.warning("rejecting set_param %s=%r (bad value)", param, raw)
            return
        self._param_overrides.setdefault(device_name, {})[param] = value
        # Optional persistence: write devices.json atomically so the value
        # survives a bot restart. Validation re-runs config.load on the
        # rendered file before we commit; if it fails, we keep the
        # in-memory override but don't promote.
        persisted = False
        if request.get("persist") is True:
            try:
                _persist_device_param(self._instances_path, device_name, param, value)
                persisted = True
                log.info("param %s.%s=%r persisted to %s",
                         device_name, param, value, self._instances_path)
            except Exception as ex:
                log.exception("persist set_param failed: %s", ex)
        log.info("param override %s.%s=%r (in-memory%s)",
                 device_name, param, value,
                 "; persisted" if persisted else "")
        # Reset any active threshold latch so the new limit takes effect cleanly.
        for key in [k for k in self._thresholds if k[0] == device_name]:
            self._thresholds[key] = _ThresholdState()
        # Push a fresh snapshot so the app sees the updated params field.
        if self.bot:
            self.webxdc.push_filtered(self.bot, self.accid, self.snapshot_for)

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
                log.info("posted to chat=%d device=%s: %s",
                         chat_id, device.name, text[:80])
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

    def _rule_clauses(self, job: sched_mod.ScheduledJob) -> list[str]:
        """Each enabled policy as a clean clause string — NO 'or' prefixes.
        Callers join them however they like (single-line via 'or', or
        multi-line indented)."""
        out: list[str] = []
        if job.deadline_ts:
            remaining = max(0, job.deadline_ts - int(time.time()))
            if job._time_mode == "tod" and job.time_of_day:
                h, m = job.time_of_day
                suffix = " daily" if job.recurring_tod else ""
                out.append(f"at {h:02d}:{m:02d}{suffix} (in {_fmt_secs(remaining)})")
            else:
                out.append(f"in {_fmt_secs(remaining)}")
        if job.has_idle():
            out.append(f"when {job.idle_field}<{job.idle_threshold:g}W "
                       f"for {_fmt_secs(job.idle_duration_s)}")
        if job.has_consumed():
            out.append(f"when used<{job.consumed_threshold_wh:g}Wh "
                       f"in {_fmt_secs(job.consumed_window_s)}")
        return out

    def _format_rule_lines(self, job: sched_mod.ScheduledJob) -> list[str]:
        """List-of-lines description used by /rules. Single-clause rules
        stay inline; multi-clause (OR-combined) rules get an indented
        bullet list under the action header."""
        clauses = self._rule_clauses(job)
        suffix = " (once)" if job.once else ""
        if not clauses:
            return [job.target_action + suffix]
        if len(clauses) == 1:
            return [f"{job.target_action} {clauses[0]}{suffix}"]
        return [f"{job.target_action}:{suffix}"] + [f"  - {c}" for c in clauses]

    def _format_schedule_ack(self, device_name: str, target_action: str,
                             job: sched_mod.ScheduledJob) -> str:
        clauses = self._rule_clauses(job)
        head = f"scheduled {device_name} {target_action}"
        if not clauses:
            return head
        return head + " " + " or ".join(clauses)

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
        # The "only once" checkbox arrives as `once: true` in the policy.
        if raw.get("once") is True:
            policy.once = True
        if policy.is_empty():
            raise ValueError("no policies supplied")
        return policy


# --- module-level helpers -------------------------------------------------

def _persist_device_param(instances_path, device_name: str,
                            param: str, value) -> None:
    """Atomically rewrite devices.json with a single param updated for
    one device. Validates the result still loads via config.load before
    committing — on validation failure the original file is untouched.
    """
    if instances_path is None:
        raise RuntimeError("no instances_path configured")
    from pathlib import Path
    p = Path(instances_path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    devices = raw.get("devices") or []
    found = False
    for d in devices:
        if d.get("name") == device_name:
            d[param] = value
            found = True
            break
    if not found:
        raise KeyError(f"device {device_name!r} not in {p}")
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    # Validate parses cleanly via config.load before swapping.
    from config import load as _cfg_load
    try:
        _cfg_load(devices_dir=p.parent / "devices", instances_file=tmp)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    import os
    os.replace(tmp, p)


def _action_verb(action: str) -> str:
    """Human-readable verb for trigger_messages. Used as {action_verb} in templates."""
    return {
        "off": "switching off",
        "on": "switching on",
        "toggle": "toggling",
    }.get(action, f"running {action}")


def _local_midnight(now_ts: int) -> int:
    lt = time.localtime(now_ts)
    return int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1)))


def _local_week_start(now_ts: int) -> int:
    """Local-time midnight of the most recent Monday (ISO week)."""
    midnight = _local_midnight(now_ts)
    lt = time.localtime(midnight)
    return midnight - lt.tm_wday * 86400


def _local_month_start(now_ts: int) -> int:
    lt = time.localtime(now_ts)
    return int(time.mktime((lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1)))


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
