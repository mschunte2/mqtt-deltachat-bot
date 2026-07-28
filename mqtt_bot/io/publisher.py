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
import time
from collections.abc import Callable

log = logging.getLogger("mqtt_bot.publisher")


def _content_hash(payload: dict) -> int:
    """Hash the snapshot's content excluding the always-changing
    server_ts. Two snapshots taken seconds apart on an offline device
    will hash equal — the non-forced /apps onboarding fan-out uses this
    to avoid re-pushing identical payloads to instances that already
    have the current state. The periodic heartbeat forces past it so the
    app's freshness indicator stays current."""
    body = {k: v for k, v in payload.items() if k != "server_ts"}
    return hash(json.dumps(body, sort_keys=True, default=str))


class Publisher:
    #: How long to let a burst of state edges accumulate before pushing.
    #: Long enough to collapse the on/off/settle flurry a switch
    #: produces, short enough that the app still feels immediate.
    COALESCE_S = 2.0

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
        # Clamp to [60s, 24h]. The heartbeat is an idle-freshness tick, not
        # the live path (state edges + refresh push immediately), so a cadence
        # of hours is fine — the 24h ceiling lets PUBLISH_INTERVAL_S=10800 (3h)
        # through, keeping dc.db growth from frequent full-snapshot carriers low.
        self._interval = max(60, min(int(interval_s), 86400))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Pending edge broadcasts, coalesced by the daemon. Twins used to
        # build and push inline on the MQTT callback thread; see
        # request_broadcast.
        self._pending_lock = threading.Lock()
        self._pending_classes: set[str | None] = set()
        self._pending_devices: set[str] = set()
        self._wake = threading.Event()
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
        # Wake the loop too: it blocks in _wake.wait(timeout=<up to the
        # heartbeat interval>), so setting only _stop would leave
        # shutdown waiting up to PUBLISH_INTERVAL_S — three hours by
        # default — before the thread noticed.
        self._stop.set()
        self._wake.set()

    def is_alive(self) -> bool:
        """Whether the heartbeat thread is still running — surfaced by
        /diag, since a dead publisher means apps silently stop updating
        while everything else looks fine."""
        return self._thread is not None and self._thread.is_alive()

    # --- triggers --------------------------------------------------------

    def request_broadcast(self, device_name: str | None = None,
                          *, only_class: str | None = None) -> None:
        """Queue an edge broadcast; the daemon performs it.

        Twins call this on every state edge. It used to be a synchronous
        `broadcast(force=True)` running on paho's network callback
        thread, which meant every edge rebuilt the full payload inline: ~750
        SQL queries per device (daily_energy alone is 366 aenergy_at
        lookups), each taking and releasing History's lock against the
        writer, plus two JSON serialisations of ~139 KB. With
        MQTT_KEEPALIVE=30 a flapping device could stall the loop past
        the PINGREQ deadline, causing a disconnect, causing another edge.

        Queuing instead does three things: gets the work off the network
        thread, collapses a burst of edges into one push, and lets the
        daemon send the compact payload rather than the full one.
        """
        with self._pending_lock:
            self._pending_classes.add(only_class)
            if device_name:
                self._pending_devices.add(device_name)
        self._wake.set()

    def _drain_pending(self) -> tuple[set[str | None], set[str]]:
        with self._pending_lock:
            classes = self._pending_classes
            devices = self._pending_devices
            self._pending_classes = set()
            self._pending_devices = set()
            return classes, devices

    def broadcast(self, device_name: str | None = None,
                  *, only_class: str | None = None,
                  force: bool = False,
                  include_history: bool = True) -> int:
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
                payload = self._build(chat_id, class_name,
                                      include_history)
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
        # Refresh and /apps always send the full payload: this is the
        # user explicitly asking for current data, and it is what
        # repopulates an app whose charts have only had compact edge
        # updates since the last heartbeat.
        payload = self._build(chat_id, class_name, True)
        if payload is None:
            return False
        if self._send(chat_id, msgid, payload):
            self._last_hash[(chat_id, msgid)] = _content_hash(payload)
            return True
        return False

    # --- daemon ----------------------------------------------------------

    def _loop(self) -> None:
        """Serve two schedules from one thread.

        - Edge broadcasts, coalesced: after the first request we wait
          COALESCE_S for more, then send ONE compact push. A cycling
          appliance produced several full pushes a minute; each left a
          permanently-retained carrier in dc.db, which is what took it
          to 464 MB.
        - The heartbeat, every `interval`: a full payload with
          force=True, so a quiet device's app refreshes server_ts (and
          its charts) rather than showing an ever-growing "data N ago"
          while the bot is perfectly healthy.
        """
        next_heartbeat = time.monotonic() + self._interval
        while not self._stop.is_set():
            timeout = max(0.0, next_heartbeat - time.monotonic())
            woken = self._wake.wait(timeout=timeout)
            if self._stop.is_set():
                break

            if woken:
                # Let a burst settle before building anything.
                self._stop.wait(self.COALESCE_S)
                if self._stop.is_set():
                    break
                self._wake.clear()
                classes, devices = self._drain_pending()
                try:
                    trigger = ",".join(sorted(devices)) or None
                    for cls in classes:
                        self.broadcast(trigger, only_class=cls, force=True,
                                       include_history=False)
                except Exception:
                    log.exception("edge broadcast failed")

            if time.monotonic() >= next_heartbeat:
                next_heartbeat = time.monotonic() + self._interval
                try:
                    self.broadcast(force=True, include_history=True)
                except Exception:
                    log.exception("periodic broadcast failed")
        log.info("publisher stopped")
