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

import json
import logging
import threading
from collections.abc import Callable

log = logging.getLogger("mqtt_bot.publisher")


def _content_hash(payload: dict) -> int:
    """Hash the snapshot's content excluding the always-changing
    server_ts. Two snapshots taken seconds apart on an offline device
    will hash equal — the periodic skip uses this to avoid re-pushing
    identical payloads."""
    body = {k: v for k, v in payload.items() if k != "server_ts"}
    return hash(json.dumps(body, sort_keys=True, default=str))


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
        # Last successfully-pushed content hash per (chat_id, msgid).
        # Used to skip identical periodic pushes; force-pushed by
        # push_unicast (refresh button) and by edge broadcasts.
        self._last_hash: dict[tuple[int, int], int] = {}

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

    def broadcast(self, device_name: str | None = None,
                  *, only_class: str | None = None,
                  force: bool = False) -> int:
        """Build + push to every registered (chat, class). With
        `only_class`, restrict the fan-out to that class — used by
        twin-driven edge broadcasts so a Tasmota toggle doesn't churn
        unrelated Shelly app instances. With `force`, push regardless
        of the content hash (state edges); without it, skip pushes
        whose hash matches the last successful one for that
        (chat, msgid)."""
        pushed = 0
        skipped = 0
        for chat_id, by_class in self._msgids().items():
            for class_name, msgid in by_class.items():
                if only_class is not None and class_name != only_class:
                    continue
                payload = self._build(chat_id, class_name)
                if payload is None:
                    continue
                key = (chat_id, msgid)
                h = _content_hash(payload)
                if not force and self._last_hash.get(key) == h:
                    skipped += 1
                    continue
                if self._send(chat_id, msgid, payload):
                    self._last_hash[key] = h
                    pushed += 1
        if device_name or skipped:
            log.debug("broadcast(trigger=%s class=%s force=%s) → %d push(es), %d skipped",
                      device_name, only_class, force, pushed, skipped)
        return pushed

    def push_unicast(self, chat_id: int, msgid: int,
                     class_name: str) -> bool:
        """Always pushes (force-true) — used for the refresh button and
        /apps onboarding."""
        payload = self._build(chat_id, class_name)
        if payload is None:
            return False
        if self._send(chat_id, msgid, payload):
            self._last_hash[(chat_id, msgid)] = _content_hash(payload)
            return True
        return False

    # --- daemon ----------------------------------------------------------

    def _loop(self) -> None:
        # wait(interval) returns True when stop() is called
        while not self._stop.wait(self._interval):
            try:
                self.broadcast()
            except Exception:
                log.exception("periodic broadcast failed")
