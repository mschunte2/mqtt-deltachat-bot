"""Tests for `mqtt_bot.rehydrate.rehydrate_rules_from_history`.

Covers idle and avg rule rehydration on bot startup. Consumed rules
don't need rehydration (their evaluator reads
``history.energy_consumed_in`` directly).
"""

import time
import unittest

from mqtt_bot import rehydrate as rehy
from mqtt_bot.core import rules as sched
from mqtt_bot.core.twins import TwinRegistry

from tests._fixtures import _FakeHistory, _build_twin


class TestRehydrateIdle(unittest.TestCase):
    def test_idle_rehydrated_when_history_continuously_below(self):
        h = _FakeHistory()
        now = int(time.time())
        since = now - 60
        h.samples_raw_rows = [
            (since + i, 1.0, 230.0, 0.0, 50.0, 0.0, 1, 25.0)
            for i in range(0, 60, 5)
        ]
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower",
                                  idle_threshold=5.0,
                                  idle_duration_s=60),
            12,
        )
        twin.rules[0]._below_since = None
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), h)
        self.assertIsNotNone(twin.rules[0]._below_since)

    def test_idle_not_rehydrated_when_history_has_spike(self):
        h = _FakeHistory()
        now = int(time.time())
        since = now - 60
        # One sample above threshold → idle rule must NOT be rehydrated.
        h.samples_raw_rows = [
            (since + i, (10.0 if i == 30 else 1.0),
             230.0, 0.0, 50.0, 0.0, 1, 25.0)
            for i in range(0, 60, 5)
        ]
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower",
                                  idle_threshold=5.0,
                                  idle_duration_s=60),
            12,
        )
        twin.rules[0]._below_since = None
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), h)
        self.assertIsNone(twin.rules[0]._below_since)


class TestRehydrateAvg(unittest.TestCase):
    def test_avg_rehydrated_when_mean_below_and_coverage_full(self):
        h = _FakeHistory()
        now = int(time.time())
        since = now - 600
        # Cycling load: 1, 1, 1, 12 → mean = 3.75 W < 5 W threshold.
        # Coverage: first sample at exactly `since`.
        vals = [1.0, 1.0, 1.0, 12.0]
        h.samples_raw_rows = [
            (since + (i * 600) // (len(vals) - 1),
             v, 230.0, 0.0, 50.0, 0.0, 1, 25.0)
            for i, v in enumerate(vals)
        ]
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=5.0,
                                  avg_window_s=600),
            12,
        )
        # Wipe the just-stamped _avg_started_at so we can verify
        # the rehydration stamps it back.
        twin.rules[0]._avg_started_at = 0
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), h)
        self.assertNotEqual(twin.rules[0]._avg_started_at, 0)
        # Stamp anchors the warmup gate at the window start so the
        # rule is immediately eligible on the next state update.
        self.assertLessEqual(twin.rules[0]._avg_started_at, since)

    def test_avg_not_rehydrated_when_mean_above_threshold(self):
        h = _FakeHistory()
        now = int(time.time())
        since = now - 600
        h.samples_raw_rows = [
            (since + i, 10.0, 230.0, 0.0, 50.0, 0.0, 1, 25.0)
            for i in range(0, 600, 30)
        ]
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=5.0,
                                  avg_window_s=600),
            12,
        )
        twin.rules[0]._avg_started_at = 0
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), h)
        self.assertEqual(twin.rules[0]._avg_started_at, 0)

    def test_avg_not_rehydrated_without_full_coverage(self):
        h = _FakeHistory()
        now = int(time.time())
        since = now - 600
        # First sample arrives 200 s after `since` → partial window,
        # no rehydration even if the mean would qualify.
        h.samples_raw_rows = [
            (since + 200 + i, 1.0, 230.0, 0.0, 50.0, 0.0, 1, 25.0)
            for i in range(0, 400, 30)
        ]
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=5.0,
                                  avg_window_s=600),
            12,
        )
        twin.rules[0]._avg_started_at = 0
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), h)
        self.assertEqual(twin.rules[0]._avg_started_at, 0)


class TestRehydrateNoOp(unittest.TestCase):
    def test_no_history_is_noop(self):
        twin, _, _ = _build_twin(history=None)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower",
                                  idle_threshold=5.0,
                                  idle_duration_s=60),
            12,
        )
        rehy.rehydrate_rules_from_history(TwinRegistry([twin]), None)
        self.assertIsNone(twin.rules[0]._below_since)


if __name__ == "__main__":
    unittest.main()
