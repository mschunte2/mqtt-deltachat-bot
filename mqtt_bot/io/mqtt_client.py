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

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._client.connect_async(self._host, self._port, keepalive=self._keepalive)
        self._thread = threading.Thread(
            target=self._client.loop_forever,
            kwargs={"retry_first_connection": True},
            daemon=True,
            name="mqtt-loop",
        )
        self._thread.start()
        log.info("MQTT client thread started -> %s:%d", self._host, self._port)

    # --- publish (thread-safe) -------------------------------------------

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> None:
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.warning("publish to %s rc=%s", topic, info.rc)

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
        try:
            self._on_message(msg.topic, msg.payload)
        except Exception:
            log.exception("on_message handler raised for topic=%s", msg.topic)
