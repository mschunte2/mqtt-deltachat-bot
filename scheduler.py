"""Action scheduler — fires a target_action on a device when any policy trips.

Pending rules are persisted to a JSON file (`rules.json` under the bot's
config dir) so a `systemctl restart` doesn't silently drop them. On
load:
  - one-shot rules whose deadline elapsed during downtime are dropped
    (firing them retroactively would surprise the user),
  - recurring time-of-day rules re-arm to their next occurrence.
Transient state (idle's _below_since, consumed's _samples,
consumed_started_at) is reset on load — at worst an idle/consumed rule
takes one extra cycle to fire after restart.

Four trigger policies, any subset of which may be active on a single job
(OR semantics — whichever fires first wins):

  - timer:       at deadline_ts (now + N seconds), one-shot
  - time_of_day: at next HH:MM in local time; optionally recurring daily
  - idle:        when state[field] < threshold for >= duration_s
  - consumed:    when integral(field·dt) over last window_s < threshold_wh

Auto-off and auto-on are both built on this. They differ only in:
  - target_action  ("off" vs "on")
  - which policies they enable (auto-on currently only supports timer + tod)
  - which trigger_messages dict the engine pulls templates from

Jobs are keyed by (device_name, target_action), so a single device can
hold a pending auto-on AND a pending auto-off concurrently.

In-memory only; bot restart drops pending jobs by design.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import durations

log = logging.getLogger("mqtt_bot.scheduler")


# --- Defaults & data shapes ----------------------------------------------

@dataclass(frozen=True)
class PolicyDefaults:
    """Fall-back values used when the user omits explicit policy params.

    Constructed by the engine from the device class's auto_off / auto_on
    config block, so device classes that omit certain knobs simply leave
    these as the dataclass defaults (and refuse the corresponding clause
    via the `allowed` set passed to parse_policy).
    """
    idle_field: str = "apower"
    idle_threshold: float = 5.0
    idle_duration_s: int = 60
    consumed_field: str = "apower"
    consumed_threshold_wh: float = 5.0
    consumed_window_s: int = 600


@dataclass
class ScheduledPolicy:
    timer_seconds: int | None = None
    time_of_day: tuple[int, int] | None = None     # (hour, minute)
    recurring_tod: bool = False

    idle_field: str | None = None
    idle_threshold: float | None = None
    idle_duration_s: int | None = None

    consumed_field: str | None = None
    consumed_threshold_wh: float | None = None
    consumed_window_s: int | None = None

    # If True, the rule is deleted after its first fire. If False (the
    # default since v1.5) the rule re-arms and fires every time the
    # condition is met again, until explicitly cancelled.
    once: bool = False

    def is_empty(self) -> bool:
        return (self.timer_seconds is None
                and self.time_of_day is None
                and self.idle_field is None
                and self.consumed_field is None)


@dataclass
class ScheduledJob:
    device_name: str
    chat_id_origin: int
    target_action: str

    # Stable identifier within (device, target_action). Derived from the
    # job's policy contents so re-adding an identical rule replaces in
    # place; differing rules get distinct ids and run in parallel.
    rule_id: str = ""

    # time-based (mutually exclusive within ONE rule: either timer or tod;
    # multiple rules can each have their own time-based policy)
    deadline_ts: int | None = None
    time_of_day: tuple[int, int] | None = None
    recurring_tod: bool = False    # legacy flag, retained for back-compat
    _time_mode: str | None = None    # "timer" | "tod" — drives template selection
    # The original timer duration, kept so the rule can re-arm to
    # `now + timer_seconds` after firing (recurring timers).
    timer_seconds: int | None = None

    # If True, the rule self-deletes on first fire. Default False in v1.5+.
    once: bool = False

    # idle policy
    idle_field: str | None = None
    idle_threshold: float | None = None
    idle_duration_s: int | None = None
    _below_since: int | None = None

    # consumed policy
    consumed_field: str | None = None
    consumed_threshold_wh: float | None = None
    consumed_window_s: int | None = None
    _samples: deque = field(default_factory=lambda: deque(maxlen=2000))
    _consumed_started_at: int = 0

    @classmethod
    def from_policy(
        cls,
        policy: ScheduledPolicy,
        device_name: str,
        chat_id_origin: int,
        target_action: str,
        now: int,
    ) -> "ScheduledJob":
        deadline_ts: int | None = None
        time_mode: str | None = None
        if policy.timer_seconds is not None:
            deadline_ts = now + policy.timer_seconds
            time_mode = "timer"
        elif policy.time_of_day is not None:
            h, m = policy.time_of_day
            deadline_ts = next_tod_deadline(h, m, now)
            time_mode = "tod"
        return cls(
            device_name=device_name,
            chat_id_origin=chat_id_origin,
            target_action=target_action,
            rule_id=derive_rule_id(policy),
            deadline_ts=deadline_ts,
            time_of_day=policy.time_of_day,
            recurring_tod=policy.recurring_tod,
            _time_mode=time_mode,
            timer_seconds=policy.timer_seconds,
            once=policy.once,
            idle_field=policy.idle_field,
            idle_threshold=policy.idle_threshold,
            idle_duration_s=policy.idle_duration_s,
            consumed_field=policy.consumed_field,
            consumed_threshold_wh=policy.consumed_threshold_wh,
            consumed_window_s=policy.consumed_window_s,
            _consumed_started_at=now if policy.consumed_field else 0,
        )

    def has_time(self) -> bool:
        return self.deadline_ts is not None

    def has_idle(self) -> bool:
        return self.idle_field is not None

    def has_consumed(self) -> bool:
        return self.consumed_field is not None

    def to_dict(self) -> dict[str, Any]:
        """Persistence shape — every non-transient field. Reverse of
        from_dict. Transient state (_below_since, _samples,
        _consumed_started_at) is intentionally omitted; rules re-acquire
        idle/consumed state organically after load."""
        return {
            "device_name": self.device_name,
            "chat_id_origin": self.chat_id_origin,
            "target_action": self.target_action,
            "rule_id": self.rule_id,
            "deadline_ts": self.deadline_ts,
            "time_of_day": list(self.time_of_day) if self.time_of_day else None,
            "recurring_tod": self.recurring_tod,
            "_time_mode": self._time_mode,
            "timer_seconds": self.timer_seconds,
            "once": self.once,
            "idle_field": self.idle_field,
            "idle_threshold": self.idle_threshold,
            "idle_duration_s": self.idle_duration_s,
            "consumed_field": self.consumed_field,
            "consumed_threshold_wh": self.consumed_threshold_wh,
            "consumed_window_s": self.consumed_window_s,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ScheduledJob":
        tod = d.get("time_of_day")
        # Migration for rules persisted before v1.5 (no `once` field):
        # preserve the previous one-shot-by-default behaviour by treating
        # any non-recurring rule as once=True. Recurring TODs were
        # already persistent so they stay persistent.
        if "once" in d:
            once = bool(d["once"])
        else:
            once = not bool(d.get("recurring_tod", False))
        return cls(
            device_name=str(d["device_name"]),
            chat_id_origin=int(d["chat_id_origin"]),
            target_action=str(d["target_action"]),
            rule_id=str(d.get("rule_id") or "default"),
            deadline_ts=d.get("deadline_ts"),
            time_of_day=(int(tod[0]), int(tod[1])) if tod else None,
            recurring_tod=bool(d.get("recurring_tod", False)),
            _time_mode=d.get("_time_mode"),
            timer_seconds=d.get("timer_seconds"),
            once=once,
            idle_field=d.get("idle_field"),
            idle_threshold=d.get("idle_threshold"),
            idle_duration_s=d.get("idle_duration_s"),
            consumed_field=d.get("consumed_field"),
            consumed_threshold_wh=d.get("consumed_threshold_wh"),
            consumed_window_s=d.get("consumed_window_s"),
            _consumed_started_at=int(time.time()) if d.get("consumed_field") else 0,
        )

    def to_snapshot(self) -> dict[str, Any]:
        """JSON-safe snapshot for the webxdc app (countdowns, indicators)."""
        return {
            "rule_id": self.rule_id,
            "target_action": self.target_action,
            "deadline_ts": self.deadline_ts,
            "time_of_day": list(self.time_of_day) if self.time_of_day else None,
            "recurring_tod": self.recurring_tod,
            "timer_seconds": self.timer_seconds,
            "once": self.once,
            "idle": ({"field": self.idle_field, "threshold": self.idle_threshold,
                      "duration_s": self.idle_duration_s} if self.has_idle() else None),
            "consumed": ({"field": self.consumed_field,
                          "threshold_wh": self.consumed_threshold_wh,
                          "window_s": self.consumed_window_s} if self.has_consumed() else None),
        }


def _job_dormant(job: "ScheduledJob", output: Any) -> bool:
    """A rule is dormant when the device is already in its target state.
    Off-rule on a device that is already off → dormant. Same for on-rule
    on an already-on device. None / unknown output → not dormant
    (better to fire-and-suppress later than to miss a real condition).
    """
    if job.target_action == "off" and output is False:
        return True
    if job.target_action == "on" and output is True:
        return True
    return False


def derive_rule_id(policy: ScheduledPolicy) -> str:
    """Stable, human-readable id derived from the policy's contents.

    Same policy contents → same rule_id → schedule() replaces in place.
    Different contents → different rule_id → both rules coexist and fire
    independently. Hand-readable so the app can log/show it.
    """
    parts: list[str] = []
    if policy.timer_seconds is not None:
        parts.append(f"timer:{policy.timer_seconds}")
    if policy.time_of_day is not None:
        h, m = policy.time_of_day
        suffix = "d" if policy.recurring_tod else ""
        parts.append(f"tod:{h:02d}{m:02d}{suffix}")
    if policy.idle_field is not None:
        parts.append(f"idle:{policy.idle_threshold:g}W:{policy.idle_duration_s}s")
    if policy.consumed_field is not None:
        parts.append(f"consumed:{policy.consumed_threshold_wh:g}Wh:"
                     f"{policy.consumed_window_s}s")
    return ",".join(parts) or "empty"


# --- Helpers --------------------------------------------------------------

def next_tod_deadline(h: int, m: int, now: int) -> int:
    """Unix seconds for the next occurrence of HH:MM in local time.

    Uses mktime with isdst=-1 so DST transitions are handled correctly:
    a "07:00" scheduled the day before a spring-forward gets resolved
    against tomorrow's calendar date, not "now + 86400" arithmetic.
    """
    lt = time.localtime(now)
    target = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, h, m, 0, 0, 0, -1))
    if target <= now:
        tom = time.localtime(now + 86400)
        target = time.mktime((tom.tm_year, tom.tm_mon, tom.tm_mday, h, m, 0, 0, 0, -1))
    return int(target)


def integrate_wh(samples, window_start: int, now: int) -> float:
    """Watt-hours consumed in [window_start, now] via trapezoidal rule.

    `samples` is an iterable of (ts_seconds, watts) ordered by ts. Samples
    older than window_start are ignored. The trailing sample is extended
    to `now` at constant power. Returns 0 for empty input.
    """
    pts = [(t, p) for t, p in samples if t >= window_start]
    if not pts:
        return 0.0
    if len(pts) == 1:
        return pts[0][1] * (now - window_start) / 3600.0
    ws = 0.0
    pt, pp = pts[0]
    for t, p in pts[1:]:
        ws += (pp + p) / 2.0 * (t - pt)
        pt, pp = t, p
    ws += pp * (now - pt)
    return ws / 3600.0


# --- Parser ---------------------------------------------------------------

ALL_POLICY_KINDS = frozenset({"timer", "tod", "idle", "consumed"})

_TOD_RE = re.compile(
    r"^at\s+(\d{1,2})(?:h(\d{2})?|:(\d{2}))?\s*(daily)?$", re.IGNORECASE
)

# Timer / idle accept multi-token durations like "30 min", "1 hour 30 min".
# Capture the rest of the line and let durations.parse handle it.
_TIMER_RE = re.compile(r"^(?:for|in)\s+(.+?)\s*$", re.IGNORECASE)

# Unified idle clause: power threshold (W) OR rolling-window energy (Wh/kWh).
# Value is a number with optional unit, possibly with a space between
# (e.g. "200W", "200 W", "200Wh", "200 Wh"). Duration is the rest of the
# line, so "30min", "30 min", "1h 30m", "1 hour" all parse.
_IDLE_RE = re.compile(
    r"^(?:if|until)\s+idle"
    r"(?:\s+(\d+(?:\.\d+)?\s*[a-zA-Z]*)"   # value with optional unit
    r"(?:\s+in)?"                           # optional "in" keyword
    r"\s+(.+?))?"                            # duration (rest)
    r"\s*$",
    re.IGNORECASE,
)
_NUM_UNIT_RE = re.compile(r"^([0-9]*\.?[0-9]+)\s*([a-zA-Z]+)?$")


def parse_policy(
    text: str,
    defaults: PolicyDefaults,
    allowed: frozenset[str] | set[str] = ALL_POLICY_KINDS,
) -> ScheduledPolicy:
    """Parse a chat-side scheduling clause.

    Forms (case-insensitive, whitespace-tolerant):
      timer:    "for 30m" | "in 30m"
      tod:      "at 7h" | "at 7:30" | "at 18h30" | "at 7h daily"
      idle:     "until idle" | "until idle 10W 120s"
      consumed: "until used <5Wh in 10m" | "until used 1.5kWh in 1h"

    Multiple parts joined by " or " combine OR-wise (whichever fires first
    wins). Only one time-based policy per clause is allowed (timer XOR tod).
    """
    s = text.strip()
    parts = [p.strip() for p in re.split(r"\s+or\s+", s, flags=re.IGNORECASE) if p.strip()]
    if not parts:
        raise ValueError("empty schedule clause")
    policy = ScheduledPolicy()
    # Strip a "once" keyword from anywhere in any part. If any was
    # present, mark the policy as one-shot; otherwise the default
    # (recurring) applies.
    cleaned_parts: list[str] = []
    for part in parts:
        tokens = part.split()
        filtered = [t for t in tokens if t.lower() != "once"]
        if len(filtered) != len(tokens):
            policy.once = True
        joined = " ".join(filtered).strip()
        if joined:
            cleaned_parts.append(joined)
    parts = cleaned_parts
    if not parts:
        raise ValueError("only 'once' specified — needs an actual condition")
    for part in parts:
        _apply(part, policy, defaults, allowed)
    if policy.is_empty():
        raise ValueError("no policies parsed from clause")
    return policy


def _apply(part: str, policy: ScheduledPolicy, d: PolicyDefaults, allowed) -> None:
    if (m := _TIMER_RE.match(part)):
        if "timer" not in allowed:
            raise ValueError(f"timer not allowed: {part!r}")
        if policy.timer_seconds is not None or policy.time_of_day is not None:
            raise ValueError("only one time-based policy per clause")
        policy.timer_seconds = durations.parse(m.group(1))
        return
    if (m := _TOD_RE.match(part)):
        if "tod" not in allowed:
            raise ValueError(f"time-of-day not allowed: {part!r}")
        if policy.timer_seconds is not None or policy.time_of_day is not None:
            raise ValueError("only one time-based policy per clause")
        h = int(m.group(1))
        # Group 2 is "minutes after h" (e.g. 7h30); group 3 is "minutes after :" (e.g. 7:30).
        # Either may be absent → minute defaults to 0.
        mm = int(m.group(2) or m.group(3) or 0)
        daily = bool(m.group(4))
        if not (0 <= h <= 23 and 0 <= mm <= 59):
            raise ValueError(f"invalid HH:MM: {part!r}")
        policy.time_of_day = (h, mm)
        policy.recurring_tod = daily
        return
    if (m := _IDLE_RE.match(part)):
        # Bare "if idle" / "until idle" → power-threshold policy, defaults.
        if m.group(1) is None:
            if "idle" not in allowed:
                raise ValueError(f"idle not allowed: {part!r}")
            if policy.idle_field is not None:
                raise ValueError("idle policy specified twice")
            policy.idle_field = d.idle_field
            policy.idle_threshold = d.idle_threshold
            policy.idle_duration_s = d.idle_duration_s
            return
        # With value+duration: unit on the value picks instantaneous (W) vs rolling (Wh/kWh).
        val, unit = _value_unit_split(m.group(1))
        duration = durations.parse(m.group(2))
        if unit in {"", "w"}:
            if "idle" not in allowed:
                raise ValueError(f"idle (power) not allowed: {part!r}")
            if policy.idle_field is not None:
                raise ValueError("idle policy specified twice")
            policy.idle_field = d.idle_field
            policy.idle_threshold = val
            policy.idle_duration_s = duration
        elif unit in {"wh", "kwh"}:
            if "consumed" not in allowed:
                raise ValueError(f"idle (energy) not allowed: {part!r}")
            if policy.consumed_field is not None:
                raise ValueError("consumed policy specified twice")
            policy.consumed_field = d.consumed_field
            policy.consumed_threshold_wh = val if unit == "wh" else val * 1000.0
            policy.consumed_window_s = duration
        else:
            raise ValueError(f"expected W/Wh/kWh, got {unit!r}")
        return
    raise ValueError(f"unrecognised schedule part: {part!r}")


def _value_unit_split(s: str) -> tuple[float, str]:
    """Return (numeric value, lowercase unit suffix or '')."""
    m = _NUM_UNIT_RE.match(s.strip())
    if not m:
        raise ValueError(f"expected number with optional unit: {s!r}")
    return float(m.group(1)), (m.group(2) or "").lower()


def _parse_value(s: str, expected: str) -> float:
    m = _NUM_UNIT_RE.match(s.strip())
    if not m:
        raise ValueError(f"expected number with unit {expected}: {s!r}")
    n = float(m.group(1))
    unit = (m.group(2) or "").lower()
    if expected == "Wh":
        if unit in {"", "wh"}:
            return n
        if unit == "kwh":
            return n * 1000.0
        raise ValueError(f"expected Wh/kWh, got {unit!r}")
    if expected == "W":
        if unit in {"", "w"}:
            return n
        raise ValueError(f"expected W, got {unit!r}")
    return n


# --- Scheduler ------------------------------------------------------------

# on_fire(device_name, chat_id_origin, target_action, mode, ctx)
#   mode in {"timer", "tod", "idle", "consumed"}
FireCallback = Callable[[str, int, str, str, dict[str, Any]], None]


class Scheduler:
    def __init__(self, on_fire: FireCallback,
                 persist_path: Path | str | None = None) -> None:
        self._on_fire = on_fire
        # Keyed by (device, target_action, rule_id) so multiple rules with
        # the same target action can coexist and fire independently.
        self._jobs: dict[tuple[str, str, str], ScheduledJob] = {}
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._persist_path = Path(persist_path) if persist_path else None
        # Engine registers a callback so the timer thread can ask the
        # current device output state — used to skip evaluating rules
        # whose target state is already met (an off-rule when the plug
        # is already off, an on-rule when it's already on).
        self._state_provider: Callable[[str], dict[str, Any]] | None = None

    def set_state_provider(self, provider: Callable[[str], dict[str, Any]]) -> None:
        self._state_provider = provider

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True, name="scheduler")
        self._thread.start()

    # --- public API -------------------------------------------------------

    def schedule(self, job: ScheduledJob) -> None:
        if not job.rule_id:
            # Backward-compat: a job without a rule_id is a single rule.
            job.rule_id = "default"
        with self._lock:
            self._jobs[(job.device_name, job.target_action, job.rule_id)] = job
        self._wake.set()
        self._persist()

    def cancel(self, device_name: str,
               target_action: str | None = None,
               rule_id: str | None = None) -> list[ScheduledJob]:
        cancelled: list[ScheduledJob] = []
        with self._lock:
            for key in list(self._jobs.keys()):
                d, a, rid = key
                if d != device_name:
                    continue
                if target_action is not None and a != target_action:
                    continue
                if rule_id is not None and rid != rule_id:
                    continue
                cancelled.append(self._jobs.pop(key))
        if cancelled:
            self._wake.set()
            self._persist()
        return cancelled

    def get(self, device_name: str, target_action: str,
            rule_id: str = "default") -> ScheduledJob | None:
        with self._lock:
            return self._jobs.get((device_name, target_action, rule_id))

    def jobs_for_device(self, device_name: str) -> list[ScheduledJob]:
        with self._lock:
            return [j for k, j in self._jobs.items() if k[0] == device_name]

    def all_jobs(self) -> list[ScheduledJob]:
        with self._lock:
            return list(self._jobs.values())

    def tick(self, device_name: str, state: dict[str, Any], now: int | None = None) -> None:
        """Evaluate idle and consumed policies for the device after a state update."""
        now = now if now is not None else int(time.time())
        fires: list[tuple[ScheduledJob, str, dict[str, Any]]] = []
        output = state.get("output")
        with self._lock:
            for key in list(self._jobs.keys()):
                d, _action, _rid = key
                if d != device_name:
                    continue
                job = self._jobs[key]
                # State-aware dormancy: an off-rule does nothing while
                # the device is already off; an on-rule does nothing while
                # the device is already on. Reset transient counters so
                # the rule starts fresh when the device flips back.
                if _job_dormant(job, output):
                    job._below_since = None
                    if job._samples:
                        job._samples.clear()
                    job._consumed_started_at = now
                    continue
                fired = False
                if job.has_idle():
                    v = state.get(job.idle_field)
                    if isinstance(v, (int, float)):
                        if v < job.idle_threshold:
                            if job._below_since is None:
                                job._below_since = now
                            elif now - job._below_since >= job.idle_duration_s:
                                duration = now - job._below_since
                                fires.append((job, "idle",
                                              {"value": float(v),
                                               "threshold": float(job.idle_threshold),
                                               "seconds": duration,
                                               "duration_human": durations.format(duration),
                                               "field": job.idle_field}))
                                fired = True
                                if job.once:
                                    del self._jobs[key]
                                else:
                                    # Re-arm: clear so a fresh below-since
                                    # tracking begins next time we evaluate.
                                    job._below_since = None
                        else:
                            job._below_since = None
                if not fired and job.has_consumed():
                    v = state.get(job.consumed_field)
                    if isinstance(v, (int, float)):
                        job._samples.append((now, float(v)))
                    cutoff = now - job.consumed_window_s
                    while job._samples and job._samples[0][0] < cutoff:
                        job._samples.popleft()
                    if now - job._consumed_started_at >= job.consumed_window_s:
                        wh = integrate_wh(job._samples, cutoff, now)
                        if wh < job.consumed_threshold_wh:
                            fires.append((job, "consumed",
                                          {"value": float(wh),
                                           "threshold": float(job.consumed_threshold_wh),
                                           "seconds": job.consumed_window_s,
                                           "window_human": durations.format(job.consumed_window_s),
                                           "field": job.consumed_field}))
                            if job.once:
                                del self._jobs[key]
                            else:
                                # Re-arm: clear samples; a new window
                                # begins at the next non-dormant tick.
                                job._samples.clear()
                                job._consumed_started_at = now
        for j, mode, ctx in fires:
            self._safe_fire(j, mode, ctx)
        if fires:
            self._persist()

    # --- timer thread -----------------------------------------------------

    def _loop(self) -> None:
        while True:
            now = int(time.time())
            fires: list[tuple[ScheduledJob, str, dict[str, Any]]] = []
            next_deadline: int | None = None
            with self._lock:
                # Pass 1: detect fires + re-arm.
                for key in list(self._jobs.keys()):
                    job = self._jobs[key]
                    if not job.has_time() or job.deadline_ts > now:
                        continue
                    # State-aware skip: if the device is already in the
                    # rule's target state, re-arm without firing — no
                    # publish, no chat post.
                    output = None
                    if self._state_provider is not None:
                        st = self._state_provider(job.device_name) or {}
                        output = st.get("output")
                    dormant = _job_dormant(job, output)

                    mode = job._time_mode or "timer"
                    elapsed = max(0, now - (job.deadline_ts or now))
                    ctx: dict[str, Any] = {
                        "value": 0, "seconds": elapsed, "field": "",
                        "duration_human": durations.format(elapsed) if elapsed else "",
                    }
                    if job.time_of_day:
                        h, m = job.time_of_day
                        ctx["hh"] = f"{h:02d}"
                        ctx["mm"] = f"{m:02d}"

                    # Re-arm decision (TOD vs timer vs once).
                    if job.once:
                        del self._jobs[key]
                    elif job.time_of_day:
                        h, m = job.time_of_day
                        job.deadline_ts = next_tod_deadline(h, m, now)
                    elif job.timer_seconds:
                        job.deadline_ts = now + job.timer_seconds
                    else:
                        # No re-arm value (legacy data); drop defensively.
                        del self._jobs[key]

                    if not dormant:
                        fires.append((job, mode, ctx))
                # Pass 2: collect next deadline from surviving jobs.
                for j in self._jobs.values():
                    if j.has_time() and (next_deadline is None or j.deadline_ts < next_deadline):
                        next_deadline = j.deadline_ts
            for j, mode, ctx in fires:
                self._safe_fire(j, mode, ctx)
            if fires:
                self._persist()
            if next_deadline is None:
                self._wake.wait()
            else:
                self._wake.wait(timeout=max(0.5, next_deadline - int(time.time())))
            self._wake.clear()

    # --- internals --------------------------------------------------------

    # --- persistence ------------------------------------------------------

    def load_persisted(self) -> int:
        """Read rules.json into _jobs. Drop expired one-shots; re-arm
        recurring TODs whose deadline elapsed during downtime. Idempotent
        — safe to call before .start(). Returns count of rules loaded.
        """
        if self._persist_path is None or not self._persist_path.exists():
            return 0
        try:
            data = json.loads(self._persist_path.read_text())
        except (OSError, json.JSONDecodeError):
            log.exception("failed to read %s; skipping load", self._persist_path)
            return 0
        now = int(time.time())
        loaded = 0
        with self._lock:
            for d in data.get("jobs", []):
                try:
                    job = ScheduledJob.from_dict(d)
                except (KeyError, TypeError, ValueError):
                    continue
                # Re-arm recurring TODs whose deadline passed during downtime.
                if job.deadline_ts and job.deadline_ts < now:
                    if job.recurring_tod and job.time_of_day:
                        h, m = job.time_of_day
                        job.deadline_ts = next_tod_deadline(h, m, now)
                    else:
                        # One-shot timer/tod expired during downtime — skip.
                        continue
                self._jobs[(job.device_name, job.target_action, job.rule_id)] = job
                loaded += 1
        log.info("loaded %d persisted rules from %s",
                 loaded, self._persist_path)
        # If we re-armed any recurring TODs the on-disk state diverged
        # from in-memory — sync.
        self._persist()
        return loaded

    def _persist(self) -> None:
        """Write _jobs to disk atomically. No-op if no persist_path was
        configured. Caller MUST NOT hold self._lock — we acquire it."""
        if self._persist_path is None:
            return
        with self._lock:
            data = {"jobs": [j.to_dict() for j in self._jobs.values()]}
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._persist_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2))
            os.replace(tmp, self._persist_path)
        except Exception:
            log.exception("persist scheduler state to %s failed",
                          self._persist_path)

    def _safe_fire(self, job: ScheduledJob, mode: str, ctx: dict[str, Any]) -> None:
        try:
            self._on_fire(job.device_name, job.chat_id_origin, job.target_action, mode, ctx)
        except Exception:
            log.exception("on_fire raised for %s/%s mode=%s",
                          job.device_name, job.target_action, mode)
