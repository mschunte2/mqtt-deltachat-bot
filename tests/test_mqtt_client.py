"""Tests for the paho wrapper.

mqtt_client.py had no test file. Two behaviours matter enough to pin:

- `publish` must report failure. It used to return None unconditionally
  and log a non-success rc at WARNING, so the twin acked 🆗 for commands
  that never left the process.
- The network loop must not die silently. The thread target was a bare
  `loop_forever`; if it returned or raised, the bot stopped receiving
  MQTT forever with nothing monitoring liveness.

`paho.mqtt.client` is stubbed in tests/__init__.py alongside deltachat2,
so these run without the package installed.
"""

import threading
import time
import unittest

from mqtt_bot.io import mqtt_client as mc


class _FakeInfo:
    def __init__(self, rc):
        self.rc = rc


class _FakeClient:
    """Stands in for paho's Client. `loop_script` drives what successive
    loop_forever calls do."""

    def __init__(self, loop_script=None, publish_rc=0):
        self.subscribed = []
        self.published = []
        self.publish_rc = publish_rc
        self.connected = False
        self.disconnect_calls = 0
        self.reconnects = 0
        self._loop_script = list(loop_script or [])
        self.loop_calls = 0
        self.loop_entered = threading.Event()
        self._release = threading.Event()

    # -- lifecycle paho calls --
    def username_pw_set(self, *a):
        pass

    def connect_async(self, *a, **k):
        self.connected = True

    def disconnect(self):
        self.disconnect_calls += 1
        self._release.set()

    def reconnect(self):
        self.reconnects += 1

    def is_connected(self):
        return self.connected

    def subscribe(self, topic, qos=0):
        self.subscribed.append(topic)

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        if isinstance(self.publish_rc, Exception):
            raise self.publish_rc
        return _FakeInfo(self.publish_rc)

    def loop_forever(self, retry_first_connection=False):
        self.loop_calls += 1
        self.loop_entered.set()
        action = (self._loop_script.pop(0) if self._loop_script else "block")
        if action == "return":
            return
        if action == "raise":
            raise OSError("socket died")
        self._release.wait(timeout=10)      # "block"


def _client(fake, **kw):
    c = mc.MqttClient(
        host="localhost", port=1883, username="u", password="p",
        client_id="test", keepalive=30,
        subscriptions_for=kw.get("subscriptions_for", lambda: ["a/b"]),
        on_message=kw.get("on_message", lambda t, p: None),
    )
    c._client = fake
    return c


class TestPublishReportsResult(unittest.TestCase):
    def test_success_returns_true(self):
        fake = _FakeClient(publish_rc=0)
        self.assertTrue(_client(fake).publish("t", "payload"))

    def test_failure_rc_returns_false(self):
        fake = _FakeClient(publish_rc=4)          # MQTT_ERR_NO_CONN
        self.assertFalse(_client(fake).publish("t", "payload"))

    def test_raising_publish_returns_false(self):
        fake = _FakeClient(publish_rc=RuntimeError("broker gone"))
        self.assertFalse(_client(fake).publish("t", "payload"))

    def test_commands_go_out_at_qos_1(self):
        """One-shot actuations should survive a reconnect. A duplicate is
        harmless — the twin only trusts the device's own echo."""
        fake = _FakeClient()
        _client(fake).publish("t", "payload")
        self.assertEqual(fake.published[0][2], 1)


class TestLoopSupervision(unittest.TestCase):
    def _run(self, script):
        fake = _FakeClient(loop_script=script)
        c = _client(fake)
        self.addCleanup(c.stop)
        c.start()
        return c, fake

    def test_loop_returning_is_restarted(self):
        c, fake = self._run(["return", "block"])
        deadline = time.monotonic() + 5
        while fake.loop_calls < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertGreaterEqual(fake.loop_calls, 2,
                                "loop_forever returned and was never restarted")
        self.assertTrue(c.is_alive())

    def test_loop_raising_is_restarted(self):
        c, fake = self._run(["raise", "block"])
        deadline = time.monotonic() + 5
        while fake.loop_calls < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertGreaterEqual(fake.loop_calls, 2,
                                "loop_forever raised and killed the thread")
        self.assertTrue(c.is_alive())

    def test_stop_ends_the_thread(self):
        c, fake = self._run(["block"])
        self.assertTrue(fake.loop_entered.wait(timeout=5))
        c.stop()
        c._thread.join(timeout=5)
        self.assertFalse(c.is_alive())

    def test_is_alive_false_before_start(self):
        self.assertFalse(_client(_FakeClient()).is_alive())


class TestObservabilityHooks(unittest.TestCase):
    def test_last_message_age_is_none_before_any_message(self):
        self.assertIsNone(_client(_FakeClient()).last_message_age_s())

    def test_last_message_age_tracks_inbound_traffic(self):
        c = _client(_FakeClient())
        msg = type("Msg", (), {"topic": "a/b", "payload": b"{}"})()
        c._handle_message(None, None, msg)
        age = c.last_message_age_s()
        self.assertIsNotNone(age)
        self.assertLess(age, 5)

    def test_handler_exception_does_not_escape_the_callback(self):
        """An exception here would propagate into paho's network loop."""
        def boom(topic, payload):
            raise ValueError("bad payload")

        c = _client(_FakeClient(), on_message=boom)
        msg = type("Msg", (), {"topic": "a/b", "payload": b"{"})()
        c._handle_message(None, None, msg)      # must not raise

    def test_subscriptions_applied_on_connect(self):
        fake = _FakeClient()
        c = _client(fake, subscriptions_for=lambda: ["x/1", "x/2"])
        c._handle_connect(None, None, None, 0)
        self.assertEqual(fake.subscribed, ["x/1", "x/2"])

    def test_failed_connect_does_not_subscribe(self):
        fake = _FakeClient()
        c = _client(fake, subscriptions_for=lambda: ["x/1"])
        c._handle_connect(None, None, None, 5)   # not authorised
        self.assertEqual(fake.subscribed, [])


if __name__ == "__main__":
    unittest.main()
