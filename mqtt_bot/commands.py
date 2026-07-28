"""Pure parser for chat-side ``/<device> <verb> [tail]`` commands.

No module-level state, no I/O. Tests can drive it directly with a
string and assert the parsed tuple.

Also holds the **replay-protection windows** as named constants:

- ``MAX_AGE_SECONDS`` — typed text command must be no older than
  this when it reaches the bot. Sized to absorb a single MQTT
  broker reconnect + retry without losing user-typed ``/status``.
- ``MAX_APP_AGE_SECONDS`` — webxdc button taps. Tighter because
  the app shows pending state and stale taps are usually
  unintended.
- ``MAX_CLOCK_SKEW_SECONDS`` — accept future-dated messages
  within this skew (NTP not yet settled, sender clock ahead).
"""

from __future__ import annotations

import re

# Verbs that don't need a device prefix: `/help`, `/list`, etc.
GLOBAL_VERBS = {"id", "list", "apps", "help", "rules", "refresh",
                "diag", "version"}
# Per-device verbs that fire an immediate dispatch.
DIRECT_VERBS = {"on", "off", "toggle", "status"}
# Per-device verbs that cancel a previously-scheduled rule.
CANCEL_VERBS = {"cancel-auto-off", "cancel-auto-on", "cancel-schedule"}
# Per-device verbs that create a scheduled rule.
SCHEDULE_VERBS = {"auto-off", "auto-on"}

_CMD_RE = re.compile(r"^/(\S+)(?:\s+(.*))?$", re.DOTALL)
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")

MAX_AGE_SECONDS = 200
MAX_APP_AGE_SECONDS = 45
MAX_CLOCK_SKEW_SECONDS = 30

# `/<device> export <window>` bounds. The handler fetchall()s both
# tables into RAM before writing the CSV, and the deployment target is
# a Pi with 416 MB total / ~275 MB available whose samples_raw table is
# already past 300k rows — roughly 61k rows per device per month at the
# recommended 15 s cadence. A year would have materialised ~1.5M tuples
# at once and taken the bot down with the OOM killer. 31 days is the
# longest window that stays comfortably inside that budget; the row cap
# is a second belt for a device polled far faster than recommended.
EXPORT_MAX_WINDOW_S = 31 * 86400
EXPORT_MAX_ROWS = 100_000


#: Every webxdc action the bot will act on. `refresh` and `telemetry`
#: are members rather than short-circuiting above the check in
#: handle_webxdc_request, so no action reaches real work without passing
#: the whitelist. Lives here, not in bot.py, so a lock-step test can
#: compare it against what the shipped app actually emits — bot.py is
#: import-hostile and cannot be asserted against.
KNOWN_APP_ACTIONS = frozenset({
    "on", "off", "toggle", "status",
    "auto-off", "auto-on",
    "cancel-auto-off", "cancel-auto-on", "cancel-schedule",
    "reset-counter", "refresh", "telemetry",
})


def check_freshness(ts, now: int, max_age: int,
                    skew: int = MAX_CLOCK_SKEW_SECONDS) -> tuple[bool, str]:
    """Is a request bearing timestamp `ts` fresh enough to act on?

    Returns (ok, reason); `reason` is empty when ok.

    A missing or non-numeric `ts` is a REJECTION, not a skip. The
    webxdc path used to read

        ts = req.get("ts")
        if isinstance(ts, (int, float)):
            ...check age...
        handle_webxdc_request(...)      # ran regardless

    so a request with no `ts` at all — or with `ts` as a JSON string,
    null, or bool — bypassed replay protection entirely, with no log
    line, and switched a mains relay. Both SECURITY.md and CLAUDE.md
    documented the opposite ("the `ts` field is required ... apps
    without a `ts` field are rejected"), which is the worst kind of
    gap: the operator believes the control exists.

    bool is excluded explicitly because `isinstance(True, int)` is
    True in Python, so `{"ts": true}` would otherwise read as ts=1 —
    an age of ~56 years, which the window would reject, but for the
    wrong reason and only by luck.
    """
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return False, f"missing or non-numeric ts ({type(ts).__name__})"
    age = int(now) - int(ts)
    if age > max_age:
        return False, f"stale by {age - max_age}s (age={age}s)"
    if age < -skew:
        return False, f"future-dated by {-age}s beyond the {skew}s skew"
    return True, ""


def sanitize(value, fallback: str = "?", max_len: int = 64) -> str:
    """Strip control characters, trim whitespace, cap length. Used
    when echoing user-supplied strings (device names, actions) into
    chat replies — defends against control-character injection."""
    if not isinstance(value, str):
        value = str(value) if value is not None else ""
    cleaned = _CTRL_RE.sub(" ", value).strip()
    return cleaned[:max_len] if cleaned else fallback


def parse_text_command(text: str) -> tuple[str, str, str] | None:
    """Parse a chat message into ``(device, verb, rest)``.

    - ``/<verb> [rest]`` for global verbs (``device == ""``).
    - ``/<device> <verb> [rest]`` for per-device verbs.
    - Returns ``None`` if the text doesn't start with ``/`` or
      doesn't have a verb.
    """
    m = _CMD_RE.match(text.strip())
    if not m:
        return None
    head = m.group(1).lower()
    tail = (m.group(2) or "").strip()
    if head in GLOBAL_VERBS:
        return ("", head, tail)
    if not tail:
        return None
    parts = tail.split(maxsplit=1)
    verb = parts[0].lower()
    rest = parts[1] if len(parts) > 1 else ""
    return (head, verb, rest)
