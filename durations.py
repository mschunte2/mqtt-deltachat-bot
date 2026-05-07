"""Parse and format short duration strings used in chat commands.

Accepted forms (case-insensitive, whitespace-tolerant):
    "30s", "5m", "1h", "1h30m", "90s", "2h15m30s"

Returns integer seconds. Raises ValueError on anything else, including
an empty string. Zero is rejected — a zero-duration auto-off makes no
sense and is almost always a typo.
"""

import re

_TOKEN_RE = re.compile(r"(\d+)\s*([hms])", re.IGNORECASE)
_FULL_RE = re.compile(r"^(?:\s*\d+\s*[hms]\s*)+$", re.IGNORECASE)

_MULT = {"h": 3600, "m": 60, "s": 1}


def parse(text: str) -> int:
    if not text or not _FULL_RE.match(text):
        raise ValueError(f"not a duration: {text!r}")
    total = 0
    for n, unit in _TOKEN_RE.findall(text):
        total += int(n) * _MULT[unit.lower()]
    if total <= 0:
        raise ValueError(f"duration must be positive: {text!r}")
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
