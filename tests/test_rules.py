"""Tests for the rules subsystem: parse_policy,
next_tod_deadline, save_all/load_into round-trip, and the grace
period for rules just rehydrated from rules.json."""

import tempfile
import time
import unittest
from pathlib import Path

from mqtt_bot.core import rules as sched
from mqtt_bot.core.twins import TwinRegistry

from tests._fixtures import _build_twin


class TestParsePolicy(unittest.TestCase):
    def setUp(self):
        self.d = sched.PolicyDefaults(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
            consumed_field="apower", consumed_threshold_wh=5.0, consumed_window_s=600,
            avg_field="apower", avg_threshold_w=5.0, avg_window_s=600,
        )

    def test_timer(self):
        p = sched.parse_policy("for 30m", self.d)
        self.assertEqual(p.timer_seconds, 1800)
        self.assertIsNone(p.time_of_day)

    def test_in_alias(self):
        self.assertEqual(sched.parse_policy("in 30m", self.d).timer_seconds, 1800)

    def test_tod(self):
        p = sched.parse_policy("at 18h", self.d)
        self.assertEqual(p.time_of_day, (18, 0))
        self.assertFalse(p.recurring_tod)
        p = sched.parse_policy("at 18:30", self.d)
        self.assertEqual(p.time_of_day, (18, 30))

    def test_tod_daily(self):
        p = sched.parse_policy("at 7h daily", self.d)
        self.assertEqual(p.time_of_day, (7, 0))
        self.assertTrue(p.recurring_tod)

    def test_tod_invalid(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("at 25h", self.d)

    def test_idle_defaults_if(self):
        p = sched.parse_policy("if idle", self.d)
        self.assertEqual(p.idle_threshold, 5.0)
        self.assertEqual(p.idle_duration_s, 60)

    def test_idle_defaults_until_synonym(self):
        # `until` is a synonym for `if` (preserved for older syntax)
        p = sched.parse_policy("until idle", self.d)
        self.assertEqual(p.idle_threshold, 5.0)

    def test_idle_power_overrides(self):
        p = sched.parse_policy("if idle 10W 120s", self.d)
        self.assertEqual(p.idle_threshold, 10.0)
        self.assertEqual(p.idle_duration_s, 120)
        # No consumed policy on a W-unit clause.
        self.assertIsNone(p.consumed_field)

    def test_idle_energy_via_wh(self):
        # Wh unit on the value -> rolling-window energy policy ("consumed")
        p = sched.parse_policy("if idle 10Wh in 2m", self.d)
        self.assertEqual(p.consumed_threshold_wh, 10.0)
        self.assertEqual(p.consumed_window_s, 120)
        # No idle-power policy on a Wh clause.
        self.assertIsNone(p.idle_field)

    def test_idle_energy_via_kwh(self):
        p = sched.parse_policy("if idle 1.5kWh in 1h", self.d)
        self.assertEqual(p.consumed_threshold_wh, 1500.0)
        self.assertEqual(p.consumed_window_s, 3600)

    def test_idle_in_optional_for_power(self):
        # `in` keyword is accepted on either form
        p = sched.parse_policy("if idle 10W in 120s", self.d)
        self.assertEqual(p.idle_threshold, 10.0)
        self.assertEqual(p.idle_duration_s, 120)

    def test_idle_with_spaces_in_value_and_duration(self):
        # User reported "200Wh in 30min" works but "200 Wh in 30 min" failed.
        # Both should now parse identically.
        a = sched.parse_policy("if idle 200Wh in 30min", self.d)
        b = sched.parse_policy("if idle 200 Wh in 30 min", self.d)
        self.assertEqual(a.consumed_threshold_wh, 200.0)
        self.assertEqual(a.consumed_window_s, 1800)
        self.assertEqual(b.consumed_threshold_wh, 200.0)
        self.assertEqual(b.consumed_window_s, 1800)

    def test_idle_power_with_spaces(self):
        a = sched.parse_policy("if idle 5W 60s", self.d)
        b = sched.parse_policy("if idle 5 W 60 s", self.d)
        c = sched.parse_policy("if idle 5 W in 60 sec", self.d)
        for p in (a, b, c):
            self.assertEqual(p.idle_threshold, 5.0)
            self.assertEqual(p.idle_duration_s, 60)

    def test_timer_with_spaces(self):
        # "for 30 min" used to fail because (\S+) captured only "30".
        self.assertEqual(sched.parse_policy("for 30 min", self.d).timer_seconds, 1800)
        self.assertEqual(sched.parse_policy("in 1 hour", self.d).timer_seconds, 3600)
        self.assertEqual(sched.parse_policy("for 1 hr 30 min", self.d).timer_seconds, 5400)

    def test_idle_unknown_unit_rejected(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle 10J 60s", self.d)

    def test_or_combination(self):
        p = sched.parse_policy("for 1h or if idle", self.d)
        self.assertEqual(p.timer_seconds, 3600)
        self.assertIsNotNone(p.idle_field)

    def test_avg_clause(self):
        p = sched.parse_policy("avg 5W in 30m", self.d)
        self.assertEqual(p.avg_field, "apower")
        self.assertEqual(p.avg_threshold_w, 5.0)
        self.assertEqual(p.avg_window_s, 1800)
        # `avg` is distinct from `idle` — no idle policy set.
        self.assertIsNone(p.idle_field)

    def test_avg_clause_for_synonym(self):
        p = sched.parse_policy("avg 7.5W for 1h", self.d)
        self.assertEqual(p.avg_threshold_w, 7.5)
        self.assertEqual(p.avg_window_s, 3600)

    def test_avg_clause_if_prefix(self):
        p = sched.parse_policy("if avg 5W in 10m", self.d)
        self.assertEqual(p.avg_threshold_w, 5.0)
        self.assertEqual(p.avg_window_s, 600)

    def test_avg_unit_must_be_watts(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("avg 5Wh in 10m", self.d)

    def test_avg_rule_id_format(self):
        p = sched.parse_policy("avg 5W in 30m", self.d)
        # Minutes-formatted suffix, post-Part-3.
        self.assertEqual(sched.derive_rule_id(p), "avg:5W:30m")

    def test_idle_rule_id_uses_minutes(self):
        p = sched.parse_policy("if idle 5W in 60s", self.d)
        # 60s formats as "1m".
        self.assertEqual(sched.derive_rule_id(p), "idle:5W:1m")

    def test_consumed_rule_id_uses_minutes(self):
        p = sched.parse_policy("if idle 5Wh in 10m", self.d)
        self.assertEqual(sched.derive_rule_id(p), "consumed:5Wh:10m")

    def test_avg_with_idle_or_combination(self):
        # idle + avg can be OR-combined in a single rule.
        p = sched.parse_policy("if idle or avg 3W in 5m", self.d)
        self.assertIsNotNone(p.idle_field)
        self.assertEqual(p.avg_threshold_w, 3.0)
        self.assertEqual(p.avg_window_s, 300)

    def test_two_time_policies_rejected(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("for 1h or at 18h", self.d)

    def test_restricted_kinds(self):
        # auto-on only allows timer + tod
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle", self.d, allowed=frozenset({"timer", "tod"}))
        # consumed (Wh form) also rejected for auto-on
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle 5Wh in 1m", self.d,
                               allowed=frozenset({"timer", "tod"}))
        # but tod alone is fine
        p = sched.parse_policy("at 7h", self.d, allowed=frozenset({"timer", "tod"}))
        self.assertEqual(p.time_of_day, (7, 0))


class TestTodDeadline(unittest.TestCase):
    def test_future_today(self):
        # pick a target far enough in the future to not be ambiguous
        now = int(time.time())
        lt = time.localtime(now)
        h = (lt.tm_hour + 2) % 24
        target = sched.next_tod_deadline(h, 0, now)
        self.assertGreater(target, now)
        # within 25 hours
        self.assertLess(target - now, 25 * 3600)

    def test_past_rolls_to_tomorrow(self):
        now = int(time.time())
        lt = time.localtime(now)
        h_past = (lt.tm_hour - 1) % 24
        target = sched.next_tod_deadline(h_past, lt.tm_min, now)
        # must be in the future
        self.assertGreater(target, now)
        # within ~25 hours
        self.assertLess(target - now, 25 * 3600)


class TestRulesPersistence(unittest.TestCase):
    def setUp(self):
        twin, _, _ = _build_twin()
        # Schedule three rules of mixed shapes.
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        twin.schedule("off", sched.ScheduledPolicy(
            consumed_field="apower", consumed_threshold_wh=5.0,
            consumed_window_s=600), 12)
        twin.schedule("on", sched.ScheduledPolicy(
            time_of_day=(7, 30), recurring_tod=True), 12)
        self.registry = TwinRegistry([twin])
        self.path = Path(tempfile.mkdtemp()) / "rules.json"

    def test_save_load_round_trip(self):
        sched.save_all(self.registry, self.path)
        before = sorted(j.rule_id for j in self.registry.get("kitchen").rules)

        # Wipe the in-memory rules and reload from disk.
        self.registry.get("kitchen").rules.clear()
        n = sched.load_into(self.registry, self.path)
        self.assertEqual(n, 3)
        after = sorted(j.rule_id for j in self.registry.get("kitchen").rules)
        self.assertEqual(before, after)

    def test_load_into_drops_rules_for_unknown_device(self):
        # Save rules from a registry with `kitchen`, then load into a
        # registry that no longer has that twin.
        sched.save_all(self.registry, self.path)
        empty = TwinRegistry([])
        with self.assertLogs("mqtt_bot.rules", level="WARNING") as cap:
            n = sched.load_into(empty, self.path)
        self.assertEqual(n, 0)
        # The summary log line should mention `kitchen`.
        self.assertTrue(any("kitchen" in m for m in cap.output))


class TestGracePeriod(unittest.TestCase):
    """Rules just loaded from rules.json honour a short grace
    period before they're allowed to fire — guards against the
    "rule fires the moment the bot finishes booting" footgun."""

    def test_loaded_rule_does_not_fire_during_grace(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True   # not dormant for off-rule
        # Simulate a rule restored from rules.json: deadline already
        # elapsed AND _loaded_at set to "now".
        now = int(time.time())
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=now - 5,            # would fire immediately
            _time_mode="timer",
            timer_seconds=600,
            once=False,
            _loaded_at=now,                  # within grace period
        )
        twin.add_persisted_rule(job)
        twin.tick_time(now)
        # No fire, rule survives.
        self.assertEqual(calls["published"], [])
        self.assertEqual(len(twin.rules), 1)

    def test_loaded_rule_fires_after_grace(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True
        now = int(time.time())
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=now - 5, _time_mode="timer", timer_seconds=600,
            once=False, _loaded_at=now,
        )
        twin.add_persisted_rule(job)
        # Past the grace period.
        twin.tick_time(now + sched.GRACE_PERIOD_S + 1)
        self.assertEqual(len(calls["published"]), 1)

    def test_runtime_scheduled_rule_fires_immediately(self):
        # Counter-test: rules added via twin.schedule (not load_into)
        # have _loaded_at == 0 and skip the grace gate entirely.
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1), 12)
        # Fast-forward past the deadline.
        now = int(time.time()) + 5
        twin.tick_time(now)
        self.assertEqual(len(calls["published"]), 1)

    def test_load_into_stamps_loaded_at(self):
        # The persistence path sets _loaded_at; round-trip via save_all
        # then load_into into a fresh twin and assert.
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        registry = TwinRegistry([twin])
        path = Path(tempfile.mkdtemp()) / "rules.json"
        sched.save_all(registry, path)
        # Wipe and reload.
        twin.rules.clear()
        sched.load_into(registry, path)
        self.assertEqual(len(twin.rules), 1)
        self.assertGreater(twin.rules[0]._loaded_at, 0)


if __name__ == "__main__":
    unittest.main()
