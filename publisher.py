"""The single outbound pipeline from bot to apps.

Decides WHEN to push the ground-truth snapshot. Doesn't know about
Delta Chat or webxdc internals — it works through three callables
injected at construction:

  build(chat_id, class_name) -> dict | None     # snapshot.build_for_chat
  msgids() -> dict[chat_id, dict[class, msgid]] # WebxdcIO map snapshot
  send(chat_id, msgid, payload) -> bool          # WebxdcIO.push_to_msgid

Triggers:
  - broadcast(device_name=None) — twin/router calls this on every
    state edge (chat-emitted event, rule fire, schedule, cancel, etc.)
  - push_unicast(chat_id, msgid, class_name) — for the refresh button
  - daemon thread — fires broadcast() every PUBLISH_INTERVAL_S seconds

Threading: the three callables must be safe to call from the MQTT
thread, the Delta Chat handler thread, and the publisher's own
daemon. SnapshotBuilder is read-only against twin state (each twin
locks its own access); WebxdcIO holds a registry whose mutation
points (send_apps) are confined to the DC handler thread.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

log = logging.getLogger("mqtt_bot.publisher")


class Publisher:
    def __init__(
        self,
        build: Callable[[int, str], dict | None],
        msgids: Callable[[], dict[int, dict[str, int]]],
        send: Callable[[int, int, dict], bool],
        interval_s: int,
    ) -> None:
        self._build = build
        self._msgids = msgids
        self._send = send
        self._interval = max(60, min(int(interval_s), 900))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="publisher")
        self._thread.start()
        log.info("publisher up; periodic interval %ds", self._interval)

    def stop(self) -> None:
        self._stop.set()

    # --- triggers --------------------------------------------------------

    def broadcast(self, device_name: str | None = None) -> int:
        """Build + push to every registered (chat, class). `device_name`
        is informational; the snapshot always contains every device the
        chat can see in that class."""
        pushed = 0
        for chat_id, by_class in self._msgids().items():
            for class_name, msgid in by_class.items():
                payload = self._build(chat_id, class_name)
                if payload is None:
                    continue
                if self._send(chat_id, msgid, payload):
                    pushed += 1
        log.debug("broadcast(trigger=%s) → %d push(es)", device_name, pushed)
        return pushed

    def push_unicast(self, chat_id: int, msgid: int,
                     class_name: str) -> bool:
        payload = self._build(chat_id, class_name)
        if payload is None:
            return False
        return self._send(chat_id, msgid, payload)

    # --- daemon ----------------------------------------------------------

    def _loop(self) -> None:
        # wait(interval) returns True when stop() is called
        while not self._stop.wait(self._interval):
            try:
                self.broadcast()
            except Exception:
                log.exception("periodic broadcast failed")
