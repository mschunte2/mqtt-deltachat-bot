"""Action scheduler — fires a target_action on a device when any policy trips.

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

import logging
import re
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
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

    # time-based (mutually exclusive: either timer or tod)
    deadline_ts: int | None = None
    time_of_day: tuple[int, int] | None = None
    recurring_tod: bool = False
    _time_mode: str | None = None    # "timer" | "tod" — drives template selection

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
            deadline_ts=deadline_ts,
            time_of_day=policy.time_of_day,
            recurring_tod=policy.recurring_tod,
            _time_mode=time_mode,
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

    def to_snapshot(self) -> dict[str, Any]:
        """JSON-safe snapshot for the webxdc app (countdowns, indicators)."""
        return {
            "target_action": self.target_action,
            "deadline_ts": self.deadline_ts,
            "time_of_day": list(self.time_of_day) if self.time_of_day else None,
            "recurring_tod": self.recurring_tod,
            "idle": ({"field": self.idle_field, "threshold": self.idle_threshold,
                      "duration_s": self.idle_duration_s} if self.has_idle() else None),
            "consumed": ({"field": self.consumed_field,
                          "threshold_wh": self.consumed_threshold_wh,
                          "window_s": self.consumed_window_s} if self.has_consumed() else None),
        }


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
_TIMER_RE = re.compile(r"^(?:for|in)\s+(\S+)$", re.IGNORECASE)
_IDLE_RE = re.compile(r"^until\s+idle(?:\s+(\S+)\s+(\S+))?$", re.IGNORECASE)
_CONSUMED_RE = re.compile(r"^until\s+used\s*<?\s*(\S+)\s+in\s+(\S+)$", re.IGNORECASE)
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
        if "idle" not in allowed:
            raise ValueError(f"idle not allowed: {part!r}")
        if policy.idle_field is not None:
            raise ValueError("idle policy specified twice")
        policy.idle_field = d.idle_field
        if m.group(1):
            policy.idle_threshold = _parse_value(m.group(1), "W")
            policy.idle_duration_s = durations.parse(m.group(2))
        else:
            policy.idle_threshold = d.idle_threshold
            policy.idle_duration_s = d.idle_duration_s
        return
    if (m := _CONSUMED_RE.match(part)):
        if "consumed" not in allowed:
            raise ValueError(f"consumed not allowed: {part!r}")
        if policy.consumed_field is not None:
            raise ValueError("consumed policy specified twice")
        policy.consumed_field = d.consumed_field
        policy.consumed_threshold_wh = _parse_value(m.group(1), "Wh")
        policy.consumed_window_s = durations.parse(m.group(2))
        return
    raise ValueError(f"unrecognised schedule part: {part!r}")


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
    def __init__(self, on_fire: FireCallback) -> None:
        self._on_fire = on_fire
        self._jobs: dict[tuple[str, str], ScheduledJob] = {}
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True, name="scheduler")
        self._thread.start()

    # --- public API -------------------------------------------------------

    def schedule(self, job: ScheduledJob) -> None:
        with self._lock:
            self._jobs[(job.device_name, job.target_action)] = job
        self._wake.set()

    def cancel(self, device_name: str, target_action: str | None = None) -> list[ScheduledJob]:
        cancelled: list[ScheduledJob] = []
        with self._lock:
            for key in list(self._jobs.keys()):
                if key[0] != device_name:
                    continue
                if target_action is not None and key[1] != target_action:
                    continue
                cancelled.append(self._jobs.pop(key))
        if cancelled:
            self._wake.set()
        return cancelled

    def get(self, device_name: str, target_action: str) -> ScheduledJob | None:
        with self._lock:
            return self._jobs.get((device_name, target_action))

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
        with self._lock:
            for key in list(self._jobs.keys()):
                if key[0] != device_name:
                    continue
                job = self._jobs[key]
                fired = False
                if job.has_idle():
                    v = state.get(job.idle_field)
                    if isinstance(v, (int, float)):
                        if v < job.idle_threshold:
                            if job._below_since is None:
                                job._below_since = now
                            elif now - job._below_since >= job.idle_duration_s:
                                del self._jobs[key]
                                fires.append((job, "idle",
                                              {"value": float(v),
                                               "seconds": now - job._below_since,
                                               "field": job.idle_field}))
                                fired = True
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
                            del self._jobs[key]
                            fires.append((job, "consumed",
                                          {"value": float(wh),
                                           "seconds": job.consumed_window_s,
                                           "field": job.consumed_field}))
        for j, mode, ctx in fires:
            self._safe_fire(j, mode, ctx)

    # --- timer thread -----------------------------------------------------

    def _loop(self) -> None:
        while True:
            now = int(time.time())
            fires: list[tuple[ScheduledJob, str, dict[str, Any]]] = []
            next_deadline: int | None = None
            with self._lock:
                # Pass 1: detect fires + re-arm recurring TODs.
                for key in list(self._jobs.keys()):
                    job = self._jobs[key]
                    if not job.has_time() or job.deadline_ts > now:
                        continue
                    mode = job._time_mode or "timer"
                    ctx: dict[str, Any] = {"value": 0, "seconds": 0, "field": ""}
                    if job.time_of_day:
                        h, m = job.time_of_day
                        ctx["hh"] = f"{h:02d}"
                        ctx["mm"] = f"{m:02d}"
                    if job.recurring_tod and job.time_of_day:
                        h, m = job.time_of_day
                        job.deadline_ts = next_tod_deadline(h, m, now)
                        # keep in _jobs; do not delete
                    else:
                        del self._jobs[key]
                    fires.append((job, mode, ctx))
                # Pass 2: collect next deadline from surviving jobs.
                for j in self._jobs.values():
                    if j.has_time() and (next_deadline is None or j.deadline_ts < next_deadline):
                        next_deadline = j.deadline_ts
            for j, mode, ctx in fires:
                self._safe_fire(j, mode, ctx)
            if next_deadline is None:
                self._wake.wait()
            else:
                self._wake.wait(timeout=max(0.5, next_deadline - int(time.time())))
            self._wake.clear()

    # --- internals --------------------------------------------------------

    def _safe_fire(self, job: ScheduledJob, mode: str, ctx: dict[str, Any]) -> None:
        try:
            self._on_fire(job.device_name, job.chat_id_origin, job.target_action, mode, ctx)
        except Exception:
            log.exception("on_fire raised for %s/%s mode=%s",
                          job.device_name, job.target_action, mode)
