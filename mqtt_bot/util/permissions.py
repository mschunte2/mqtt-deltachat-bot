"""Allow-list checks. Pure functions over Config + a global chat set."""

from __future__ import annotations

from .config import Config, Device


def is_allowed(chat_id: int, allowed_chats: set[int]) -> bool:
    """True iff chat may talk to the bot at all (the global gate)."""
    return chat_id in allowed_chats


def chat_can_see(chat_id: int, device: Device, fallback_chats: set[int]) -> bool:
    """True iff chat may see/operate this device.

    If the device declares its own allowed_chats, that wins.
    Otherwise it falls back to the global allow-list — sensible for
    single-chat setups where every authorised chat sees every device.
    """
    if device.allowed_chats:
        return chat_id in device.allowed_chats
    return chat_id in fallback_chats


def visible_devices(chat_id: int, cfg: Config, fallback_chats: set[int]) -> list[Device]:
    return [d for d in cfg.devices.values()
            if chat_can_see(chat_id, d, fallback_chats)]


def chats_for_device(device: Device, fallback_chats: set[int]) -> tuple[int, ...]:
    """Every chat that may receive output about this device.

    The inverse of `chat_can_see`, and derived from it rather than
    re-deriving the fallback rule. `bot.py._post_to_visible_chats` used
    to compute `device.allowed_chats or ALLOWED_CHATS` inline — a second
    copy of the predicate on the highest-volume outbound surface
    (threshold alerts, offline notices, counter-reset alerts, every
    rule-fire message). The two agreed, but SECURITY.md claimed
    `chat_can_see` was "the single function called from every routing
    site ... there is no bypass path", and the copy would have silently
    diverged the moment anyone added a deny-list, a per-device
    `enabled` flag, or a mute.
    """
    candidates = device.allowed_chats or tuple(sorted(fallback_chats))
    return tuple(c for c in candidates
                 if chat_can_see(c, device, fallback_chats))
