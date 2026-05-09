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
GLOBAL_VERBS = {"id", "list", "apps", "help", "rules", "refresh"}
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
