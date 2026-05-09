"""Scheduled rules subsystem — types, parser, persistence, sweeper.

Rules live on PlugTwin instances (one rule list per device). This
module owns:

  - the dataclasses (ScheduledJob, ScheduledPolicy, PolicyDefaults)
    and helpers (derive_rule_id, next_tod_deadline, integrate_wh,
    _job_dormant)
  - parse_policy (chat-text clause → ScheduledPolicy)
  - Persistence: save_all(registry, path) writes a flat JSON list,
    load_into(registry, path) restores it onto the right twins
  - RulesSweeper: a daemon thread that wakes at min deadline across
    every twin and calls twin.tick_time(now)

State-based rule evaluation (idle / consumed) does NOT live here in
v0.2 — it moved into PlugTwin.on_mqtt because it needs to react to
every state update inline. The sweeper only handles time-based
rules (timer / tod).
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..util import durations

log = logging.getLogger("mqtt_bot.rules")


# Rules loaded from rules.json on startup don't fire for this many
# seconds. Without it, a rule like "off when used <300Wh in 1h"
# whose condition is *already* satisfied by historical data fires
# the instant rehydrate-from-history pre-fills its sample buffer —
# i.e. before the user has had a chance to see the rule in the app
# or change their mind. The grace period gives them a minute to
# react. Runtime-scheduled rules (`/dev off if idle` from chat or
# the app) leave `_loaded_at = 0` and are NOT subject to it.
GRACE_PERIOD_S = 60


# --- Defaults & data shapes ----------------------------------------------

@dataclass(frozen=True)
class PolicyDefaults:
    """Fall-back values used when the user omits explicit policy params.

    Constructed by bot.py from the device class's auto_off / auto_on
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

    # If True, the rule self-deletes on first fire. If False the rule
    # re-arms and keeps firing until cancelled (default since v0.1.5).
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

    # Stable id within (device, target_action). Derived from policy
    # contents so re-adding an identical rule replaces in place;
    # different rules get distinct ids and run in parallel.
    rule_id: str = ""

    # time-based (timer XOR tod within ONE rule)
    deadline_ts: int | None = None
    time_of_day: tuple[int, int] | None = None
    recurring_tod: bool = False
    _time_mode: str | None = None       # "timer" | "tod"
    timer_seconds: int | None = None    # original duration for re-arm

    once: bool = False                  # one-shot when True

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

    # When the rule's idle/consumed observation window started (or
    # was last reset by manual-toggle). Used by snapshot to render
    # the actual elapsed time next to live observed values, capped
    # at the rule's configured window. 0 = legacy/loaded; display
    # falls back to the full window. Transient — not persisted.
    _observation_started_at: int = 0

    # Set by load_into to int(time.time()) on every rule restored from
    # rules.json. 0 for runtime-added rules. The twin's tick / state
    # eval skips firing while now - _loaded_at < GRACE_PERIOD_S.
    # Transient — NOT persisted via to_dict / from_dict.
    _loaded_at: int = 0

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
            _observation_started_at=(
                now if (policy.idle_field or policy.consumed_field) else 0
            ),
        )

    def has_time(self) -> bool:
        return self.deadline_ts is not None

    def in_grace(self, now: int, grace_s: int = GRACE_PERIOD_S) -> bool:
        """True for rules just loaded from rules.json that haven't
        served their grace period yet. Runtime-added rules return
        False (their _loaded_at stays 0)."""
        return self._loaded_at != 0 and (now - self._loaded_at) < grace_s

    def has_idle(self) -> bool:
        return self.idle_field is not None

    def has_consumed(self) -> bool:
        return self.consumed_field is not None

    def to_dict(self) -> dict[str, Any]:
        """Persistence shape — every non-transient field. Reverse of
        from_dict. Transient state (_below_since, _samples,
        _consumed_started_at) is intentionally omitted."""
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
        # Pre-v0.1.5 migration: missing `once` → infer from recurring_tod.
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
    """Off-rule on an already-off device → dormant. Same for on/on.
    Unknown output → not dormant (better to fire-and-suppress later)."""
    if job.target_action == "off" and output is False:
        return True
    if job.target_action == "on" and output is True:
        return True
    return False


def derive_rule_id(policy: ScheduledPolicy) -> str:
    """Stable, human-readable id derived from policy contents."""
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


# --- Time + integration helpers ------------------------------------------

def next_tod_deadline(h: int, m: int, now: int) -> int:
    """Unix seconds for the next occurrence of HH:MM in local time.
    Uses mktime with isdst=-1 for DST safety."""
    lt = time.localtime(now)
    target = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, h, m, 0, 0, 0, -1))
    if target <= now:
        tom = time.localtime(now + 86400)
        target = time.mktime((tom.tm_year, tom.tm_mon, tom.tm_mday, h, m, 0, 0, 0, -1))
    return int(target)


def integrate_wh(samples, window_start: int, now: int) -> float:
    """Watt-hours over [window_start, now] via trapezoidal rule.
    `samples` = iterable of (ts_seconds, watts) ordered by ts. Trailing
    sample is extended to `now` at constant power. 0 for empty input."""
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


# --- Parser --------------------------------------------------------------

ALL_POLICY_KINDS = frozenset({"timer", "tod", "idle", "consumed"})

_TOD_RE = re.compile(
    r"^at\s+(\d{1,2})(?:h(\d{2})?|:(\d{2}))?\s*(daily)?$", re.IGNORECASE
)
_TIMER_RE = re.compile(r"^(?:for|in)\s+(.+?)\s*$", re.IGNORECASE)
_IDLE_RE = re.compile(
    r"^(?:if|until)\s+idle"
    r"(?:\s+(\d+(?:\.\d+)?\s*[a-zA-Z]*)"   # value with optional unit
    r"(?:\s+in)?"                           # optional "in"
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
    """Parse a chat-side scheduling clause. See README for syntax."""
    s = text.strip()
    parts = [p.strip() for p in re.split(r"\s+or\s+", s, flags=re.IGNORECASE)
             if p.strip()]
    if not parts:
        raise ValueError("empty schedule clause")
    policy = ScheduledPolicy()
    cleaned: list[str] = []
    for part in parts:
        tokens = part.split()
        filtered = [t for t in tokens if t.lower() != "once"]
        if len(filtered) != len(tokens):
            policy.once = True
        joined = " ".join(filtered).strip()
        if joined:
            cleaned.append(joined)
    parts = cleaned
    if not parts:
        raise ValueError("only 'once' specified — needs an actual condition")
    for part in parts:
        _apply(part, policy, defaults, allowed)
    if policy.is_empty():
        raise ValueError("no policies parsed from clause")
    return policy


def _apply(part: str, policy: ScheduledPolicy,
           d: PolicyDefaults, allowed) -> None:
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
        mm = int(m.group(2) or m.group(3) or 0)
        daily = bool(m.group(4))
        if not (0 <= h <= 23 and 0 <= mm <= 59):
            raise ValueError(f"invalid HH:MM: {part!r}")
        policy.time_of_day = (h, mm)
        policy.recurring_tod = daily
        return
    if (m := _IDLE_RE.match(part)):
        if m.group(1) is None:
            if "idle" not in allowed:
                raise ValueError(f"idle not allowed: {part!r}")
            if policy.idle_field is not None:
                raise ValueError("idle policy specified twice")
            policy.idle_field = d.idle_field
            policy.idle_threshold = d.idle_threshold
            policy.idle_duration_s = d.idle_duration_s
            return
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
    m = _NUM_UNIT_RE.match(s.strip())
    if not m:
        raise ValueError(f"expected number with optional unit: {s!r}")
    return float(m.group(1)), (m.group(2) or "").lower()


# --- Persistence (whole-registry) ----------------------------------------

def save_all(registry, path: Path | str) -> None:
    """Write every twin's rules to a single rules.json. Atomic.
    `registry` is a TwinRegistry."""
    p = Path(path)
    jobs: list[dict] = []
    for twin in registry.all():
        jobs.extend(j.to_dict() for j in twin.jobs_snapshot())
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps({"jobs": jobs}, indent=2))
        os.replace(tmp, p)
    except Exception:
        log.exception("persist rules to %s failed", p)


def load_into(registry, path: Path | str) -> int:
    """Read rules.json into the matching twins' rule lists. Drops
    one-shot rules whose deadline elapsed during downtime; re-arms
    recurring TODs. Returns count loaded.
    """
    p = Path(path)
    if not p.exists():
        return 0
    try:
        data = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        log.exception("failed to read %s; skipping load", p)
        return 0
    now = int(time.time())
    loaded = 0
    rearmed = False
    dropped_unknown: dict[str, int] = {}   # device_name → count
    dropped_expired = 0
    dropped_malformed = 0
    for d in data.get("jobs", []):
        try:
            job = ScheduledJob.from_dict(d)
        except (KeyError, TypeError, ValueError):
            dropped_malformed += 1
            continue
        twin = registry.get(job.device_name)
        if twin is None:
            dropped_unknown[job.device_name] = dropped_unknown.get(
                job.device_name, 0) + 1
            continue
        if job.deadline_ts and job.deadline_ts < now:
            if job.recurring_tod and job.time_of_day:
                h, m = job.time_of_day
                job.deadline_ts = next_tod_deadline(h, m, now)
                rearmed = True
            else:
                # one-shot timer/tod expired during downtime — drop
                dropped_expired += 1
                continue
        # Stamp loaded-at so PlugTwin's tick honours the grace period
        # before firing this rule (avoids the "rule disappears on
        # restart because rehydrate-from-history insta-satisfies its
        # condition" footgun).
        job._loaded_at = now
        twin.add_persisted_rule(job)
        loaded += 1
    log.info("loaded %d persisted rules from %s", loaded, p)
    if dropped_expired:
        log.info("  dropped %d rule(s) whose deadline elapsed during downtime",
                 dropped_expired)
    if dropped_malformed:
        log.warning("  dropped %d malformed rule(s) (rules.json schema drift?)",
                    dropped_malformed)
    if dropped_unknown:
        names = ", ".join(f"{n}×{c}" for n, c in sorted(dropped_unknown.items()))
        log.warning("  DROPPED rules for unknown device(s): %s — "
                    "rename in devices.json reverts on next save_all. "
                    "Edit %s manually to relocate the rules.",
                    names, p)
    if rearmed:
        save_all(registry, p)
    return loaded


# --- Sweeper thread ------------------------------------------------------

class RulesSweeper:
    """Single daemon thread that wakes at the earliest deadline across
    every twin and calls twin.tick_time(now) on every twin. Tick is
    cheap when no rule is due (a comparison per rule)."""

    def __init__(self, registry) -> None:
        self._registry = registry
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="rules-sweeper")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def wake(self) -> None:
        """Called when a new rule is added/cancelled, so we re-evaluate
        the earliest deadline immediately."""
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = int(time.time())
            deadlines = [d for d in (t.next_deadline()
                                     for t in self._registry.all())
                         if d is not None]
            if deadlines:
                wait_for = max(0.5, min(deadlines) - now)
            else:
                wait_for = None  # block until wake
            if wait_for is None:
                self._wake.wait()
            else:
                self._wake.wait(timeout=wait_for)
            self._wake.clear()
            if self._stop.is_set():
                return
            now = int(time.time())
            for t in self._registry.all():
                try:
                    t.tick_time(now)
                except Exception:
                    log.exception("tick_time raised for %s", t.name)
