"""Allow-list checks. Pure functions over Config + a global chat set."""

from __future__ import annotations

from config import Config, Device


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
