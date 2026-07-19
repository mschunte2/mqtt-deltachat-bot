"""Delta Chat account configuration applied at startup.

Currently just message retention: `delete_device_after` bounds how long
the bot keeps local messages, which is what stops dc.db from growing
without limit as the Publisher pushes webxdc status-update carriers.

Kept out of bot.py — which can't be imported in tests (module-level
construction touches the filesystem) — so the days->seconds mapping and
the disable sentinel are unit-testable against a fake rpc. `_on_start`
calls apply_retention() with the live rpc.
"""

from __future__ import annotations

_SECONDS_PER_DAY = 86400


def ensure_bot_mode(rpc, accid: int) -> None:
    """Put the account in Delta Chat bot mode (`bot=1`), idempotently.

    deltabot-cli sets this only on its `init`/configure path — an account
    imported from a backup does NOT get it, so unknown contacts' first
    messages are parked as contact requests and group-membership events
    never arrive, making the bot look dead to new users. Setting it when
    already set is a harmless no-op."""
    rpc.set_config(accid, "bot", "1")


def delete_device_after_seconds(days: int) -> int:
    """Convert a retention window in days to the seconds value Delta Chat
    expects. 0 (and any nonsense negative) means "never delete"."""
    if days <= 0:
        return 0
    return days * _SECONDS_PER_DAY


def apply_retention(rpc, accid: int, days: int) -> None:
    """Set `delete_device_after` on the account. `days=0` writes "0"
    (Delta Chat's never-delete sentinel), so the knob can be turned off
    by config rather than leaving a stale value in place.

    Caveat: this is a *local* policy — Delta Chat deletes EVERY message on
    the account older than the window, including the webxdc app-container
    message an install points at. An install left untouched past the
    window ages out and stops receiving pushes until a /apps re-seed."""
    seconds = delete_device_after_seconds(days)
    rpc.set_config(accid, "delete_device_after", str(seconds))
