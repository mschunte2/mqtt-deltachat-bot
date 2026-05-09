"""Registry of PlugTwin instances + reverse topic lookup.

Owns:
  - dict[device_name, PlugTwin]
  - dict[topic_str, (twin, suffix)] for O(1) MQTT routing

No business logic. The bot.py routing functions and the rules sweeper
both ask the registry to resolve a topic / name / chat to twin(s).

Constructed once at startup; immutable afterwards (the device set is
fixed at config-load time, no live device add/remove in v0.2).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from .twin import PlugTwin


class TwinRegistry:
    def __init__(self, twins: Iterable[PlugTwin]) -> None:
        self._by_name: dict[str, PlugTwin] = {t.name: t for t in twins}
        self._topic_lookup: dict[str, tuple[PlugTwin, str]] = {}
        for t in self._by_name.values():
            for sub in t.cls.subscribe:
                self._topic_lookup[f"{t.cfg.topic_prefix}/{sub.suffix}"] = (t, sub.suffix)

    def get(self, name: str) -> PlugTwin | None:
        return self._by_name.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def all(self) -> Iterator[PlugTwin]:
        return iter(self._by_name.values())

    def find_by_topic(self, topic: str) -> tuple[PlugTwin, str] | None:
        return self._topic_lookup.get(topic)

    def visible_to(self, chat_id: int,
                   allowed_chats: set[int]) -> list[PlugTwin]:
        return [t for t in self._by_name.values()
                if t.can_chat_see(chat_id, allowed_chats)]

    def visible_classes_for(self, chat_id: int,
                            allowed_chats: set[int]) -> set[str]:
        return {t.cls.name for t in self.visible_to(chat_id, allowed_chats)}

    def subscriptions(self) -> list[str]:
        return sorted(self._topic_lookup)
