"""Tests for chat-side display.

formatters.py had no test file, despite being what the user actually
reads. The staleness tests below cover the gap that mattered most: a
device line rendered identically whether the reading was four seconds
or six hours old, so a dead MQTT thread was invisible from chat.
"""

import threading
import time
import unittest

from mqtt_bot import formatters
from mqtt_bot.core import rules as sched

from tests._fixtures import _build_twin


NOW = 1_714_000_000


class TestStalenessIsVisible(unittest.TestCase):
    def _line(self, age_s, **fields):
        twin, _, _ = _build_twin()
        twin.fields.update({"online": True, "output": True, "apower": 42.0,
                            **fields})
        twin.last_update_ts = NOW - age_s if age_s is not None else 0
        return formatters.format_device_line(twin, now=NOW)

    def test_fresh_line_has_no_staleness_marker(self):
        line = self._line(5)
        self.assertNotIn("stale", line)
        self.assertNotIn("no data", line)

    def test_stale_line_is_marked_with_its_age(self):
        line = self._line(6 * 3600)
        self.assertIn("stale", line)
        self.assertIn("6h", line)

    def test_never_seen_device_says_so(self):
        self.assertIn("no data", self._line(None))

    def test_a_dead_feed_does_not_look_healthy(self):
        """The concrete scenario: MQTT thread dies at 03:00, user runs
        /list at 09:00. The line used to be byte-identical to a healthy
        one because `online` only flips via the plug's LWT, which needs
        the very connection under suspicion."""
        fresh = self._line(4)
        dead = self._line(6 * 3600)
        self.assertNotEqual(fresh, dead)
        self.assertIn("🟢", dead)          # still "online" per the last LWT
        self.assertIn("stale", dead)

    def test_boundary_is_not_flagged(self):
        self.assertNotIn("stale", self._line(formatters.STALE_AFTER_S))
        self.assertIn("stale", self._line(formatters.STALE_AFTER_S + 1))

    def test_normal_status_bits_survive(self):
        line = self._line(5)
        self.assertIn("ON", line)
        self.assertIn("42W", line)


class TestDeviceLineBasics(unittest.TestCase):
    def _twin(self, **fields):
        twin, _, _ = _build_twin()
        twin.last_update_ts = int(time.time())
        twin.fields.update(fields)
        return twin

    def test_offline_marker(self):
        line = formatters.format_device_line(self._twin(online=False))
        self.assertIn("🔴", line)

    def test_unknown_state_marker(self):
        line = formatters.format_device_line(self._twin())
        self.assertIn("⚪", line)

    def test_multi_class_suffix(self):
        twin = self._twin(online=True)
        self.assertIn(f"[{twin.cls.name}]",
                      formatters.format_device_line(twin, multi_class=True))
        self.assertNotIn(f"[{twin.cls.name}]",
                         formatters.format_device_line(twin))

    def test_pending_rule_is_listed(self):
        twin = self._twin(online=True, output=True)
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        self.assertIn("[off in", formatters.format_device_line(twin))


class TestFieldsReadIsLocked(unittest.TestCase):
    def test_rendering_while_fields_churn_does_not_raise(self):
        """A bare dict(twin.fields) raises "dictionary changed size
        during iteration" if the MQTT thread is mid-update."""
        twin, _, _ = _build_twin()
        twin.last_update_ts = int(time.time())
        errors = []
        stop = threading.Event()

        def churn():
            i = 0
            while not stop.is_set():
                i += 1
                with twin._lock:
                    twin.fields.update({f"k{i % 50}": i, "apower": float(i)})
                    if i % 3 == 0:
                        twin.fields.pop(f"k{(i - 1) % 50}", None)

        t = threading.Thread(target=churn, daemon=True)
        t.start()
        try:
            for _ in range(400):
                try:
                    formatters.format_device_line(twin)
                except Exception as ex:
                    errors.append(ex)
                    break
        finally:
            stop.set()
            t.join(timeout=5)

        self.assertEqual([repr(e) for e in errors], [])



class TestDiag(unittest.TestCase):
    """Everything /diag reports was already in memory and unreachable
    from chat: 'the bot stopped reacting' had no answer short of SSH."""

    def _diag(self, **over):
        twin, _, _ = _build_twin()
        twin.last_update_ts = NOW - 30
        kw = dict(
            version="v0.2.1-43-g8bee740", mqtt_alive=True,
            mqtt_connected=True, mqtt_last_message_age_s=30.0,
            sweeper_alive=True, publisher_alive=True, twins=[twin],
            registered_msgids={"shelly_plug": 4242}, allowed_chats={12},
            now=NOW,
        )
        kw.update(over)
        return formatters.format_diag(**kw)

    def test_healthy_report_names_the_build(self):
        self.assertIn("v0.2.1-43-g8bee740", self._diag())

    def test_dead_mqtt_thread_is_obvious(self):
        out = self._diag(mqtt_alive=False)
        self.assertIn("DEAD", out)
        self.assertIn("❌", out)

    def test_dead_sweeper_explains_the_consequence(self):
        """'sweeper: dead' means nothing to a user; 'timed rules will
        not fire' is the fact they need."""
        self.assertIn("timed rules will not fire",
                      self._diag(sweeper_alive=False))

    def test_dead_publisher_explains_the_consequence(self):
        self.assertIn("apps will not update", self._diag(publisher_alive=False))

    def test_disconnected_broker_is_reported(self):
        self.assertIn("DISCONNECTED", self._diag(mqtt_connected=False))

    def test_never_received_a_message(self):
        self.assertIn("never", self._diag(mqtt_last_message_age_s=None))

    def test_last_message_age_is_human_readable(self):
        self.assertIn("ago", self._diag(mqtt_last_message_age_s=3600.0))

    def test_missing_app_registration_points_at_apps(self):
        self.assertIn("/apps", self._diag(registered_msgids={}))

    def test_empty_allowed_chats_is_called_out(self):
        self.assertIn("none set", self._diag(allowed_chats=set()))

    def test_devices_report_their_age_and_rule_count(self):
        twin, _, _ = _build_twin()
        twin.last_update_ts = NOW - 7200
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        out = self._diag(twins=[twin])
        self.assertIn("2h", out)
        self.assertIn("1 rule", out)

    def test_no_visible_devices(self):
        self.assertIn("none visible", self._diag(twins=[]))

    def test_device_with_no_data_says_so(self):
        twin, _, _ = _build_twin()
        twin.last_update_ts = 0
        self.assertIn("no data yet", self._diag(twins=[twin]))


if __name__ == "__main__":
    unittest.main()
