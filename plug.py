"""Digital twin of a physical plug.

One PlugTwin per device. Owns ALL of that plug's ground truth:
fields, last_update_ts, scheduled rules, threshold latches. All
per-device behaviour lives here as methods — MQTT-driven state
extraction, chat-event firing, rule evaluation (state-based and
time-based), action dispatch, snapshot assembly.

Dependencies are injected as plain callables via `TwinDeps` so the
twin doesn't import bot / mqtt / webxdc / publisher. Keeps the
dependency graph a clean DAG and makes the class trivially
unit-testable: stub the callables, drive the twin, assert on calls.

Threading: a single per-twin lock guards `fields`, `rules`, and
`threshold_latches`. The same twin is touched by:
  - MQTT thread (on_mqtt)
  - Delta Chat handler thread (dispatch / schedule / cancel)
  - Rules sweeper thread (tick_time)
All three acquire the lock for short critical sections; long-running
work (mqtt_publish, post_to_chats, broadcast, history writes,
save_rules) happens outside the lock.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import durations
import permissions
import rules as rules_mod
import state as state_mod
import templating
from config import (AutoOffConfig, ChatEventRule, Device, DeviceClass)

log = logging.getLogger("mqtt_bot.plug")


# --- per-(device, field) threshold latch ---------------------------------

@dataclass
class ThresholdLatch:
    above_since: int | None = None
    active: bool = False


# --- injected dependencies ----------------------------------------------

@dataclass(frozen=True)
class TwinDeps:
    """All side-effecting collaborators a PlugTwin needs, as callables.

    Keeping these as plain callables (not class refs) means the twin
    has zero static imports of bot/mqtt/publisher/webxdc — those
    dependencies are wired up in bot.py at construction time.
    """
    mqtt_publish:  Callable[[str, str], None]                 # (topic, payload)
    post_to_chats: Callable[["Device", str], None]            # text → device.allowed_chats
    broadcast:     Callable[[str], None]                      # device_name → publisher
    save_rules:    Callable[[], None]                         # rules.json atomic save
    react:         Callable[[int, str], None]                 # (msgid, emoji) for dispatch ack
    history:       "Any"   # History | None — read inside snapshot, write inside on_mqtt
    client_id:     str


# --- the digital twin ----------------------------------------------------

class PlugTwin:
    def __init__(self, cls: DeviceClass, cfg: Device, deps: TwinDeps) -> None:
        self.cls = cls
        self.cfg = cfg
        self.deps = deps
        self.name = cfg.name
        self.fields: dict[str, Any] = {}
        self.last_update_ts: int = 0
        self.rules: list[rules_mod.ScheduledJob] = []
        self.threshold_latches: dict[str, ThresholdLatch] = {}
        # In-memory param overrides — currently unused (set_param dropped
        # in v0.2), but we keep the attribute so the snapshot has a
        # stable shape.
        self.param_overrides: dict[str, Any] = {}
        self._lock = threading.Lock()

    # --- inbound from MQTT ----------------------------------------------

    def on_mqtt(self, suffix: str, payload: bytes) -> None:
        """Apply one inbound MQTT message: extract → update fields →
        fire chat events → tick state rules → write history → broadcast
        if any state edge.
        """
        updates = state_mod.extract(self.cls, suffix, payload)
        if not updates:
            return

        now = int(time.time())
        prev: dict[str, Any] = {}
        with self._lock:
            for k in updates:
                prev[k] = self.fields.get(k)
            self.fields.update(updates)
            self.last_update_ts = now
            current_fields = dict(self.fields)

        # Chat-event evaluation (touches threshold latches under the lock,
        # but emits messages outside it).
        edge_fired = self._evaluate_chat_events(updates, prev, now)

        # State-based rule evaluation (idle / consumed). Dormant rules
        # (target state already met) are reset to their starting condition
        # so they don't accumulate while the plug sits in target.
        rule_fired = self._tick_state_rules(current_fields, now)

        # History writes — outside the lock; History has its own lock.
        self._write_history(suffix, payload, current_fields, now)

        # Broadcast on any state edge OR rule fire. Don't broadcast for
        # plain power-metric noise (apower wiggling without crossing
        # anything significant).
        if edge_fired or rule_fired:
            self.deps.broadcast(self.name)

    # --- chat-side actions ---------------------------------------------

    def dispatch(self, action: str,
                 source_msgid: int | None = None) -> tuple[bool, str]:
        """Validate + publish + cancel same-direction rules + react +
        broadcast. Returns (ok, message)."""
        if action not in self.cls.commands:
            return False, (f"unknown action: {action} "
                           f"(try: {', '.join(sorted(self.cls.commands))})")
        cmd = self.cls.commands[action]
        topic = f"{self.cfg.topic_prefix}/{cmd.suffix}"
        payload = templating.render(cmd.payload,
                                    {"client_id": self.deps.client_id})
        self.deps.mqtt_publish(topic, payload)
        log.info("dispatch %s/%s → %s", self.name, action, topic)

        # Manual-override: cancel any pending rule whose target_action ==
        # this action. Same-direction only (a manual /off should NOT
        # clear a pending auto-on).
        cancelled = self._remove_rules(target_action=action)
        section = self._auto_section_for(action)
        if cancelled and section:
            tpl = section.trigger_messages.get("cancelled_manual")
            if tpl:
                text = templating.render(tpl, {
                    "name": self.name,
                    "action": action,
                    "action_verb": _action_verb(action),
                })
                self.deps.post_to_chats(self.cfg, text)
        if cancelled:
            self.deps.save_rules()

        if source_msgid is not None:
            self.deps.react(source_msgid, "🆗")

        self.deps.broadcast(self.name)
        return True, ""

    def schedule(self, target_action: str,
                 policy: rules_mod.ScheduledPolicy,
                 chat_id_origin: int) -> tuple[bool, str]:
        """Add a scheduled rule. If a rule with the same rule_id already
        exists for this target_action, it's replaced in place."""
        if target_action not in self.cls.commands:
            return False, (f"action {target_action!r} not supported "
                           f"by class {self.cls.name}")
        now = int(time.time())
        job = rules_mod.ScheduledJob.from_policy(
            policy, self.name, chat_id_origin, target_action, now,
        )
        with self._lock:
            # Replace any existing rule with the same (target_action, rule_id).
            self.rules = [r for r in self.rules
                          if not (r.target_action == job.target_action
                                  and r.rule_id == job.rule_id)]
            self.rules.append(job)
        self.deps.save_rules()
        self.deps.broadcast(self.name)
        return True, _format_schedule_ack(self.name, target_action, job)

    def cancel(self, *, target_action: str | None = None,
               rule_id: str | None = None) -> list[rules_mod.ScheduledJob]:
        cancelled = self._remove_rules(target_action=target_action,
                                       rule_id=rule_id)
        if cancelled:
            self.deps.save_rules()
            self.deps.broadcast(self.name)
        return cancelled

    # --- time-based rule tick (called by sweeper thread) ---------------

    def next_deadline(self) -> int | None:
        with self._lock:
            return min((r.deadline_ts for r in self.rules
                        if r.deadline_ts is not None),
                       default=None)

    def tick_time(self, now: int) -> None:
        """Fire any rule whose deadline has elapsed. Re-arm or drop per
        policy. Skip-but-re-arm (no fire) when device is already in the
        rule's target state."""
        fires: list[tuple[rules_mod.ScheduledJob, str, dict[str, Any]]] = []
        mutated = False
        survivors: list[rules_mod.ScheduledJob] = []
        with self._lock:
            output = self.fields.get("output")
            for job in self.rules:
                if job.deadline_ts is None or job.deadline_ts > now:
                    survivors.append(job)
                    continue
                # Time-based rule has reached its deadline.
                mutated = True
                dormant = rules_mod._job_dormant(job, output)
                mode = job._time_mode or "timer"
                elapsed = max(0, now - job.deadline_ts)
                ctx: dict[str, Any] = {
                    "value": 0, "seconds": elapsed, "field": "",
                    "duration_human": (durations.format(elapsed)
                                       if elapsed else ""),
                }
                if job.time_of_day:
                    h, m = job.time_of_day
                    ctx["hh"] = f"{h:02d}"
                    ctx["mm"] = f"{m:02d}"

                if job.once:
                    pass  # drop, do not re-arm
                elif job.time_of_day:
                    h, m = job.time_of_day
                    job.deadline_ts = rules_mod.next_tod_deadline(h, m, now)
                    survivors.append(job)
                elif job.timer_seconds:
                    job.deadline_ts = now + job.timer_seconds
                    survivors.append(job)
                # else: drop defensively

                if not dormant:
                    fires.append((job, mode, ctx))
            self.rules = survivors

        # Outside the lock: do the side effects.
        for job, mode, ctx in fires:
            self._fire_rule(job, mode, ctx)
        if mutated:
            self.deps.save_rules()
        if fires:
            self.deps.broadcast(self.name)

    # --- read-only ------------------------------------------------------

    def can_chat_see(self, chat_id: int, allowed_chats: set[int]) -> bool:
        return permissions.chat_can_see(chat_id, self.cfg, allowed_chats)

    def jobs_snapshot(self) -> list[rules_mod.ScheduledJob]:
        with self._lock:
            return list(self.rules)

    def to_dict(self) -> dict[str, Any]:
        """The per-device payload included in the outbound snapshot."""
        with self._lock:
            fields = dict(self.fields)
            last_update_ts = self.last_update_ts
            scheduled = [j.to_snapshot() for j in self.rules]
        params = dict(self.cfg.params)
        params.update(self.param_overrides)
        payload: dict[str, Any] = {
            "name": self.name,
            "description": self.cfg.description,
            "fields": fields,
            "last_update_ts": last_update_ts,
            "scheduled_jobs": scheduled,
            "params": params,
        }
        if self.deps.history is not None:
            from snapshot import (_daily_energy_wh, _energy_summary,
                                  _power_history)
            payload["energy"] = _energy_summary(
                self.deps.history, self.name, fields.get("aenergy"),
            )
            payload["daily_energy_wh"] = _daily_energy_wh(
                self.deps.history, self.name,
            )
            payload["power_history"] = _power_history(
                self.deps.history, self.name,
            )
        return payload

    # --- internals ------------------------------------------------------

    def _evaluate_chat_events(self, updates: dict[str, Any],
                              prev: dict[str, Any], now: int) -> bool:
        """Fire on_change / threshold rules for any field that changed.
        Returns True if any event fired (caller broadcasts then)."""
        any_fired = False
        for rule in self.cls.chat_events:
            if rule.field not in updates:
                continue
            new_value = updates[rule.field]
            if rule.type == "on_change":
                if prev.get(rule.field) != new_value:
                    if self._fire_on_change(rule, new_value):
                        any_fired = True
            elif rule.type == "threshold":
                if self._fire_threshold(rule, new_value, now):
                    any_fired = True
        return any_fired

    def _fire_on_change(self, rule: ChatEventRule, new_value: Any) -> bool:
        key = _coerce_value_key(new_value)
        template = rule.values.get(key)
        if not template:
            return False
        text = templating.render(template, {
            "name": self.name, "value": new_value, "field": rule.field,
        })
        self.deps.post_to_chats(self.cfg, text)
        return True

    def _fire_threshold(self, rule: ChatEventRule, value: Any, now: int) -> bool:
        if not isinstance(value, (int, float)):
            return False
        limit = self.cfg.params.get(rule.limit_param)
        duration = self.cfg.params.get(rule.duration_param)
        if limit is None or duration is None:
            return False
        with self._lock:
            latch = self.threshold_latches.setdefault(rule.field,
                                                     ThresholdLatch())
        fired = False
        if value >= limit:
            if latch.above_since is None:
                latch.above_since = now
            elif (not latch.active
                  and (now - latch.above_since) >= int(duration)):
                latch.active = True
                self.deps.post_to_chats(self.cfg, templating.render(
                    rule.above, {"name": self.name, "value": float(value),
                                 "seconds": now - latch.above_since,
                                 "field": rule.field},
                ))
                fired = True
        else:
            if latch.active:
                self.deps.post_to_chats(self.cfg, templating.render(
                    rule.below, {"name": self.name, "value": float(value),
                                 "field": rule.field},
                ))
                fired = True
            latch.above_since = None
            latch.active = False
        return fired

    def _tick_state_rules(self, fields: dict[str, Any], now: int) -> bool:
        """Evaluate idle and consumed rules after every state update.
        Mirrors what scheduler.tick used to do. Mutates job's transient
        bookkeeping (`_below_since`, `_samples`, `_consumed_started_at`)
        and the rules list (one-shot rules that fire are dropped).

        Locking invariant
        -----------------
        Transient per-job state — `_below_since`, `_samples`,
        `_consumed_started_at` — is owned by `self._lock`. Both
        `_eval_idle` and `_eval_consumed` MUST be called while
        holding it, and `integrate_wh` MUST stay inside the lock
        even though it's a pure function: another thread (DC
        handler, sweeper) could otherwise prepend to `_samples`
        between the cutoff trim and the integral, producing a
        wrong Wh number. If you're tempted to release the lock
        for a "perf win" — don't.
        """
        fires: list[tuple[rules_mod.ScheduledJob, str, dict[str, Any]]] = []
        mutated = False
        survivors: list[rules_mod.ScheduledJob] = []
        with self._lock:
            output = fields.get("output")
            for job in self.rules:
                # State-aware dormancy: reset transient counters and skip
                # while the device is already in the rule's target state.
                if rules_mod._job_dormant(job, output):
                    job._below_since = None
                    if job._samples:
                        job._samples.clear()
                    job._consumed_started_at = now
                    survivors.append(job)
                    continue
                fired = False
                if job.has_idle():
                    fired = self._eval_idle(job, fields, now, fires)
                if not fired and job.has_consumed():
                    fired = self._eval_consumed(job, fields, now, fires)
                if fired and job.once:
                    mutated = True
                    continue  # drop one-shot fired job
                survivors.append(job)
            if len(survivors) != len(self.rules):
                self.rules = survivors

        for job, mode, ctx in fires:
            self._fire_rule(job, mode, ctx)
        if fires and not mutated:
            mutated = True
        if mutated:
            self.deps.save_rules()
        return bool(fires)

    def _eval_idle(self, job, fields, now, fires) -> bool:
        v = fields.get(job.idle_field)
        if not isinstance(v, (int, float)):
            return False
        if v < job.idle_threshold:
            if job._below_since is None:
                job._below_since = now
                return False
            if now - job._below_since >= job.idle_duration_s:
                duration = now - job._below_since
                fires.append((job, "idle", {
                    "value": float(v),
                    "threshold": float(job.idle_threshold),
                    "seconds": duration,
                    "duration_human": durations.format(duration),
                    "field": job.idle_field,
                }))
                job._below_since = None  # re-arm
                return True
        else:
            job._below_since = None
        return False

    def _eval_consumed(self, job, fields, now, fires) -> bool:
        v = fields.get(job.consumed_field)
        if isinstance(v, (int, float)):
            job._samples.append((now, float(v)))
        cutoff = now - job.consumed_window_s
        while job._samples and job._samples[0][0] < cutoff:
            job._samples.popleft()
        if now - job._consumed_started_at < job.consumed_window_s:
            return False
        wh = rules_mod.integrate_wh(job._samples, cutoff, now)
        if wh < job.consumed_threshold_wh:
            fires.append((job, "consumed", {
                "value": float(wh),
                "threshold": float(job.consumed_threshold_wh),
                "seconds": job.consumed_window_s,
                "window_human": durations.format(job.consumed_window_s),
                "field": job.consumed_field,
            }))
            job._samples.clear()
            job._consumed_started_at = now
            return True
        return False

    def _fire_rule(self, job: rules_mod.ScheduledJob, mode: str,
                   ctx: dict[str, Any]) -> None:
        """Publish the action's MQTT command and post the trigger
        message. Called outside the lock."""
        cmd = self.cls.commands.get(job.target_action)
        if cmd is None:
            log.error("rule fired for unknown action %s on %s",
                      job.target_action, self.name)
            return
        topic = f"{self.cfg.topic_prefix}/{cmd.suffix}"
        payload = templating.render(cmd.payload,
                                    {"client_id": self.deps.client_id})
        self.deps.mqtt_publish(topic, payload)
        log.info("rule fire %s/%s mode=%s → %s",
                 self.name, job.target_action, mode, topic)
        section = self._auto_section_for(job.target_action)
        if section is None:
            return
        template = section.trigger_messages.get(mode)
        if not template:
            return
        full_ctx = {
            "name": self.name,
            "action": job.target_action,
            "action_verb": _action_verb(job.target_action),
            **ctx,
        }
        self.deps.post_to_chats(self.cfg, templating.render(template, full_ctx))

    def _remove_rules(self, *, target_action: str | None = None,
                      rule_id: str | None = None
                      ) -> list[rules_mod.ScheduledJob]:
        cancelled: list[rules_mod.ScheduledJob] = []
        with self._lock:
            survivors: list[rules_mod.ScheduledJob] = []
            for r in self.rules:
                if target_action is not None and r.target_action != target_action:
                    survivors.append(r); continue
                if rule_id is not None and r.rule_id != rule_id:
                    survivors.append(r); continue
                cancelled.append(r)
            self.rules = survivors
        return cancelled

    def _auto_section_for(self, action: str) -> AutoOffConfig | None:
        if self.cls.auto_off and self.cls.auto_off.command == action:
            return self.cls.auto_off
        if self.cls.auto_on and self.cls.auto_on.command == action:
            return self.cls.auto_on
        return None

    def _write_history(self, suffix: str, payload: bytes,
                       fields: dict[str, Any], now: int) -> None:
        h = self.deps.history
        if h is None:
            return
        if "apower" in fields or "aenergy" in fields:
            h.write_sample(self.name, now,
                           fields.get("apower"), fields.get("aenergy"),
                           fields.get("output"))
        if suffix == "status/switch:0":
            try:
                payload_obj = json.loads(
                    payload.decode("utf-8", errors="replace")
                    if isinstance(payload, bytes) else str(payload)
                )
                if isinstance(payload_obj, dict):
                    h.record_status(self.name, now, payload_obj)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        # events/rpc handling deliberately removed in v0.2 — events
        # table no longer maintained.

    # --- restoration helper (used by rules.py during load) -------------

    def add_persisted_rule(self, job: rules_mod.ScheduledJob) -> None:
        """Insert a rule loaded from rules.json. No save, no broadcast —
        bot.py orchestrates startup."""
        with self._lock:
            self.rules.append(job)


# --- pure helpers (module-level) -----------------------------------------

def _coerce_value_key(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v).lower()


def _action_verb(action: str) -> str:
    return {
        "off": "switching off",
        "on": "switching on",
        "toggle": "toggling",
    }.get(action, f"running {action}")


def _format_schedule_ack(device_name: str, target_action: str,
                         job: rules_mod.ScheduledJob) -> str:
    """Single-line ack used as the chat reply when a rule is scheduled."""
    clauses = _rule_clauses(job)
    head = f"scheduled {device_name} {target_action}"
    if not clauses:
        return head
    return head + " " + " or ".join(clauses)


def _rule_clauses(job: rules_mod.ScheduledJob) -> list[str]:
    """Each enabled policy as a clean clause string (no 'or' prefixes)."""
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
