"""Thin wrapper around paho-mqtt.

The engine is the only consumer. It supplies:
  - subscriptions_for() -> list[str]   called on every (re)connect
  - on_message(topic, payload)         dispatches inbound traffic

Side-effecting; runs paho's network loop in a daemon thread so the
deltabot-cli main loop is unaffected.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

import paho.mqtt.client as mqtt

log = logging.getLogger("mqtt_bot.mqtt")


class MqttClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        client_id: str,
        keepalive: int,
        subscriptions_for: Callable[[], list[str]],
        on_message: Callable[[str, bytes], None],
    ) -> None:
        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._subscriptions_for = subscriptions_for
        self._on_message = on_message
        # paho-mqtt 2.x requires callback_api_version explicitly; 1.x doesn't
        # know the keyword. Use VERSION1 callbacks (3-arg on_connect etc.)
        # because that's what our handlers below are written for.
        try:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                client_id=client_id, clean_session=True,
            )
        except (AttributeError, TypeError):
            self._client = mqtt.Client(client_id=client_id, clean_session=True)
        if username:
            self._client.username_pw_set(username, password)
        self._client.on_connect = self._handle_connect
        self._client.on_disconnect = self._handle_disconnect
        self._client.on_message = self._handle_message
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_message_ts: float = 0.0

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._client.connect_async(self._host, self._port,
                                   keepalive=self._keepalive)
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="mqtt-loop")
        self._thread.start()
        log.info("MQTT client thread started -> %s:%d", self._host, self._port)

    def stop(self) -> None:
        """Ask the network loop to finish. Symmetric with Publisher and
        RulesSweeper, which both had one."""
        self._stop.set()
        try:
            self._client.disconnect()
        except Exception:
            pass

    def _run(self) -> None:
        """Supervise paho's network loop.

        The thread target used to be a bare `loop_forever`. Disconnects
        are handled well (paho auto-reconnects and _handle_disconnect
        logs), but if loop_forever ever *returns* or raises, the bot
        stops receiving MQTT entirely: twins freeze at their last known
        values, nothing monitors thread liveness, and — before the
        staleness work — /list still rendered a cheerful ON/42W. Restart
        it, with a backoff so a hard failure doesn't spin.
        """
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._client.loop_forever(retry_first_connection=True)
                if self._stop.is_set():
                    break
                log.error("MQTT loop_forever returned unexpectedly; "
                          "restarting in %.0fs", backoff)
            except Exception:
                if self._stop.is_set():
                    break
                log.exception("MQTT loop_forever raised; restarting in %.0fs",
                              backoff)
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, 60.0)
            try:
                self._client.reconnect()
            except Exception:
                log.warning("MQTT reconnect attempt failed; will retry")
        log.info("MQTT client thread stopped")

    def is_alive(self) -> bool:
        """Whether the network loop thread is still running. Surfaced by
        /diag so 'the bot stopped reacting' has an answer from chat."""
        return self._thread is not None and self._thread.is_alive()

    def is_connected(self) -> bool:
        try:
            return bool(self._client.is_connected())
        except Exception:
            return False

    # --- publish (thread-safe) -------------------------------------------

    def publish(self, topic: str, payload: str, qos: int = 1,
                retain: bool = False) -> bool:
        """Publish one command. Returns whether paho accepted it.

        The result matters: callers switch mains relays and then tell
        the user what happened. Discarding a non-success rc (typically
        MQTT_ERR_NO_CONN while paho reconnects) meant the bot reacted
        🆗 and posted "switching off" for commands that never left the
        process.

        qos defaults to 1 so a command survives a reconnect rather than
        being dropped on the floor — these are one-shot actuations, not
        a telemetry stream, and a duplicate on/off is harmless because
        the twin only trusts the device's own echo.
        """
        try:
            info = self._client.publish(topic, payload, qos=qos, retain=retain)
        except Exception:
            log.exception("publish to %s raised", topic)
            return False
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("publish to %s FAILED rc=%s — the device did not get "
                      "this command", topic, info.rc)
            return False
        return True

    # --- callbacks --------------------------------------------------------

    def _handle_connect(self, _client, _userdata, _flags, rc):
        if rc != 0:
            log.error("MQTT connect failed rc=%s", rc)
            return
        topics = self._subscriptions_for()
        for t in topics:
            self._client.subscribe(t, qos=0)
        log.info("MQTT connected; subscribed to %d topics", len(topics))

    def _handle_disconnect(self, _client, _userdata, rc):
        log.warning("MQTT disconnected rc=%s; paho will auto-reconnect", rc)

    def _handle_message(self, _client, _userdata, msg):
        self._last_message_ts = time.time()
        try:
            self._on_message(msg.topic, msg.payload)
        except Exception:
            log.exception("on_message handler raised for topic=%s", msg.topic)

    def last_message_age_s(self) -> float | None:
        """Seconds since any inbound MQTT message, or None if we have
        never received one. The single most useful number when asking
        'is this bot still hearing from the plugs?'."""
        if not self._last_message_ts:
            return None
        return time.time() - self._last_message_ts
