"""Build a ScheduledPolicy from a webxdc app payload.

This is an untrusted-input boundary: the object comes from whatever
`.xdc` build happens to be installed in the chat, which may be months
old or simply buggy. It used to live inline in bot.py, where it was
both untestable and unvalidated:

- Callers caught only `ValueError`, but the coercions raised
  `TypeError`. `{"idle": {"threshold": null}}` -> `float(None)`,
  `{"time_of_day": [{}, 0]}` -> `int({})`, and
  `{"consumed": {"threshold_wh": [1]}}` all escaped the handler into
  the RawEvent hook, so one malformed request from one app instance was
  not contained and the user got no reply explaining it.
- Only `timer_minutes` was range-checked. `idle.duration_minutes`,
  `consumed.window_minutes` and `avg.window_minutes` accepted any
  numeric value including negatives, so
  `{"idle": {"duration_minutes": -5}}` gave `idle_duration_s = -300`
  and `now - _below_since >= -300` is true on the *second* sample below
  threshold — collapsing a 30-minute safety window to one status
  update. The chat acknowledgement rendered "for 0s" because
  `durations.format` clamps negatives, so nothing looked wrong.

Every failure here is now a `ValueError` with a message fit to show the
user, which is what the callers already expect.
"""

from __future__ import annotations

from .core import rules as rules_mod
from .util import durations

#: Upper bound for any app-supplied duration, in minutes. Matches
#: durations.MAX_SECONDS, which bounds the chat-typed path — a policy
#: is a policy regardless of which surface created it.
MAX_MINUTES = durations.MAX_SECONDS / 60


def _number(raw, field: str) -> float:
    """Coerce to float or raise ValueError (never TypeError).

    bool is rejected: `isinstance(True, (int, float))` is True in
    Python, and a client sending `true` for a threshold means a bug,
    not the value 1.
    """
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(
            f"{field} must be a number, got {type(raw).__name__}")
    value = float(raw)
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError(f"{field} must be a finite number")
    return value


def _minutes(raw, field: str, default_s: int) -> int:
    """An optional duration in minutes -> seconds, bounded.

    Missing values fall back to the device class's default. Present
    values must be a positive, finite number no longer than
    MAX_MINUTES.
    """
    if raw is None:
        return int(default_s)
    value = _number(raw, field)
    if value <= 0:
        raise ValueError(f"{field} must be positive, got {value:g}")
    if value > MAX_MINUTES:
        raise ValueError(
            f"{field} must be at most {MAX_MINUTES:g} minutes, "
            f"got {value:g}")
    return int(round(value * 60))


def _threshold(raw, field: str, default: float) -> float:
    if raw is None:
        return float(default)
    value = _number(raw, field)
    if value < 0:
        raise ValueError(f"{field} must not be negative, got {value:g}")
    return value


def _field_name(raw, default: str) -> str:
    """State-field name. Must be a plain string — accepting anything
    and calling str() on it turned a dict into a nonsense field name
    that then silently never matched."""
    if raw is None:
        return default
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(
            f"field must be a non-empty string, got {type(raw).__name__}")
    return raw.strip()


def build(raw: dict, defaults: rules_mod.PolicyDefaults
          ) -> rules_mod.ScheduledPolicy:
    """`auto_off` / `auto_on` payload subobject -> ScheduledPolicy.

    The app speaks **minutes** at this boundary; we convert to seconds
    once here and feed the engine its native unit downstream.

    Raises ValueError (only) on anything malformed.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy must be an object, got {type(raw).__name__}")

    policy = rules_mod.ScheduledPolicy()

    if "timer_minutes" in raw and raw["timer_minutes"] is not None:
        policy.timer_seconds = _minutes(raw["timer_minutes"],
                                        "timer_minutes", 0)

    tod = raw.get("time_of_day")
    if tod is not None:
        if not isinstance(tod, (list, tuple)) or len(tod) != 2:
            raise ValueError("time_of_day must be [hour, minute]")
        h = int(_number(tod[0], "time_of_day hour"))
        m = int(_number(tod[1], "time_of_day minute"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError(
                f"time_of_day out of range: {h:02d}:{m:02d}")
        policy.time_of_day = (h, m)
        policy.recurring_tod = bool(raw.get("recurring_tod", False))

    idle = raw.get("idle")
    if idle is not None:
        if not isinstance(idle, dict):
            raise ValueError("idle must be an object")
        policy.idle_field = _field_name(idle.get("field"),
                                        defaults.idle_field)
        policy.idle_threshold = _threshold(idle.get("threshold"),
                                           "idle.threshold",
                                           defaults.idle_threshold)
        policy.idle_duration_s = _minutes(idle.get("duration_minutes"),
                                          "idle.duration_minutes",
                                          defaults.idle_duration_s)

    consumed = raw.get("consumed")
    if consumed is not None:
        if not isinstance(consumed, dict):
            raise ValueError("consumed must be an object")
        policy.consumed_field = _field_name(consumed.get("field"),
                                            defaults.consumed_field)
        policy.consumed_threshold_wh = _threshold(
            consumed.get("threshold_wh"), "consumed.threshold_wh",
            defaults.consumed_threshold_wh)
        policy.consumed_window_s = _minutes(
            consumed.get("window_minutes"), "consumed.window_minutes",
            defaults.consumed_window_s)

    avg = raw.get("avg")
    if avg is not None:
        if not isinstance(avg, dict):
            raise ValueError("avg must be an object")
        policy.avg_field = _field_name(avg.get("field"), defaults.avg_field)
        policy.avg_threshold_w = _threshold(avg.get("threshold_w"),
                                            "avg.threshold_w",
                                            defaults.avg_threshold_w)
        policy.avg_window_s = _minutes(avg.get("window_minutes"),
                                       "avg.window_minutes",
                                       defaults.avg_window_s)

    if raw.get("once") is True:
        policy.once = True

    if policy.is_empty():
        raise ValueError("no policies supplied")
    return policy
