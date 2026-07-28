"""Parse and format short duration strings used in chat commands.

Accepted forms (case-insensitive, whitespace-tolerant):
    "30s", "5m", "1h", "1h30m", "90s", "2h15m30s"

Returns integer seconds. Raises ValueError on anything else, including
an empty string. Zero is rejected — a zero-duration auto-off makes no
sense and is almost always a typo. So is anything above MAX_SECONDS:
the result feeds `ScheduledJob.deadline_ts`, and the rules sweeper
waits on `deadline - now`, which raises OverflowError above
`threading.TIMEOUT_MAX`. A year is far more than any plug timer needs
and leaves seven orders of magnitude of headroom under that limit.
"""

import re

_UNIT = (
    r"(d(?:ay)?s?"            # d, day, days
    r"|h(?:ours?|rs?)?"       # h, hr, hrs, hour, hours
    r"|m(?:in(?:ute)?s?)?"    # m, min, mins, minute, minutes
    r"|s(?:ec(?:ond)?s?)?)"   # s, sec, secs, second, seconds
)
_TOKEN_RE = re.compile(r"(\d+)\s*" + _UNIT, re.IGNORECASE)
_FULL_RE = re.compile(r"^(?:\s*\d+\s*" + _UNIT + r"\s*)+$", re.IGNORECASE)

_MULT = {"d": 86400, "h": 3600, "m": 60, "s": 1}

#: Upper bound on any parsed duration (365 days).
MAX_SECONDS = 365 * 86400


def parse(text: str) -> int:
    if not text or not _FULL_RE.match(text):
        raise ValueError(f"not a duration: {text!r}")
    total = 0
    for n, unit in _TOKEN_RE.findall(text):
        # Verbose units (min, hours, etc.) all start with one of d/h/m/s,
        # which is the canonical key in _MULT.
        total += int(n) * _MULT[unit[0].lower()]
    if total <= 0:
        raise ValueError(f"duration must be positive: {text!r}")
    if total > MAX_SECONDS:
        raise ValueError(
            f"duration too long (max {MAX_SECONDS // 86400}d): {text!r}")
    return total


def format(seconds: int) -> str:
    if seconds < 0:
        return "0s"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts: list[str] = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s or not parts:
        parts.append(f"{s}s")
    return "".join(parts)
