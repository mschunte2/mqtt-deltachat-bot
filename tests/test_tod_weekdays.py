"""Weekday filter on time-of-day rules ("on at 07:00, Mo–Fr only").

`weekdays` uses date.weekday() numbering (0 = Mon … 6 = Sun). None
means every day — the behaviour before the filter existed — so legacy
rules.json entries and rule ids are unchanged.
"""

import unittest

from mqtt_bot.core import rules as sched
from tests.test_tod_dst import _FALL_BACK, _at, _tz

WORKDAYS = (0, 1, 2, 3, 4)


class TestNextDeadlineWithWeekdays(unittest.TestCase):
    # 2026-11-06 is a Friday.

    def test_friday_after_target_skips_the_weekend(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 11, 6, 8, 0)
            got = sched.next_tod_deadline(7, 0, now, WORKDAYS)
            self.assertEqual(got, _at(2026, 11, 9, 7, 0))   # Monday

    def test_same_day_before_target(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 11, 6, 6, 0)
            got = sched.next_tod_deadline(7, 0, now, WORKDAYS)
            self.assertEqual(got, _at(2026, 11, 6, 7, 0))

    def test_single_day_rolls_a_full_week(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 11, 6, 8, 0)
            got = sched.next_tod_deadline(7, 0, now, (4,))   # Fridays
            self.assertEqual(got, _at(2026, 11, 13, 7, 0))

    def test_none_and_all_days_match_unfiltered(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 11, 6, 8, 0)
            plain = sched.next_tod_deadline(7, 0, now)
            self.assertEqual(sched.next_tod_deadline(7, 0, now, None), plain)
            self.assertEqual(
                sched.next_tod_deadline(7, 0, now, tuple(range(7))), plain)

    def test_fall_back_day_with_filter(self):
        # 2026-10-25 (25h day) is a Sunday; a Sunday-only 00:30 rule
        # evaluated at 00:45 must go a week out, not into the past.
        with _tz("Europe/Berlin"):
            now = _at(*_FALL_BACK, 0, 45)
            got = sched.next_tod_deadline(0, 30, now, (6,))
            self.assertGreater(got, now)
            self.assertEqual(got, _at(2026, 11, 1, 0, 30))


class TestParseWeekdays(unittest.TestCase):
    def test_forms(self):
        cases = {
            "mon-fri": WORKDAYS,
            "Mo-Fr": WORKDAYS,
            "weekdays": WORKDAYS,
            "weekend": (5, 6),
            "weekends": (5, 6),
            "sa,su": (5, 6),
            "mo, we, fr": (0, 2, 4),
            "Monday,Wednesday": (0, 2),
            "fri-mon": (0, 4, 5, 6),
            "tue,tue": (1,),
            "daily": None,
        }
        for spec, want in cases.items():
            with self.subTest(spec=spec):
                self.assertEqual(sched.parse_weekdays(spec), want)

    def test_bad_specs(self):
        for spec in ("", "xx", "mo-", "mo-xx", "mo,,fr", "funday"):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    sched.parse_weekdays(spec)


class TestFormatWeekdays(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(sched.format_weekdays(WORKDAYS), "Mo–Fr")
        self.assertEqual(sched.format_weekdays((5, 6)), "Sa,Su")
        self.assertEqual(sched.format_weekdays((0, 2, 4)), "Mo,We,Fr")
        self.assertEqual(sched.format_weekdays((0, 1, 2, 4)), "Mo–We,Fr")
        self.assertEqual(sched.format_weekdays(None), "")


class TestParsePolicyWithWeekdays(unittest.TestCase):
    def setUp(self):
        self.d = sched.PolicyDefaults()

    def test_chat_clause(self):
        p = sched.parse_policy("at 7:00 mon-fri", self.d)
        self.assertEqual(p.time_of_day, (7, 0))
        self.assertEqual(p.weekdays, WORKDAYS)

    def test_with_daily_and_once(self):
        p = sched.parse_policy("at 7h daily sa,su", self.d)
        self.assertTrue(p.recurring_tod)
        self.assertEqual(p.weekdays, (5, 6))
        p = sched.parse_policy("at 7 weekdays once", self.d)
        self.assertTrue(p.once)
        self.assertEqual(p.weekdays, WORKDAYS)

    def test_unfiltered_unchanged(self):
        p = sched.parse_policy("at 7h daily", self.d)
        self.assertIsNone(p.weekdays)

    def test_garbage_after_time_rejected(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("at 7:00 tomorrow", self.d)


class TestJobCarriesWeekdays(unittest.TestCase):
    def _policy(self, weekdays):
        return sched.ScheduledPolicy(time_of_day=(7, 0), recurring_tod=True,
                                     weekdays=weekdays)

    def test_rule_id(self):
        self.assertEqual(sched.derive_rule_id(self._policy(None)),
                         "tod:0700d")
        self.assertEqual(sched.derive_rule_id(self._policy(WORKDAYS)),
                         "tod:0700d@01234")

    def test_from_policy_and_rearm_honour_filter(self):
        with _tz("Europe/Berlin"):
            fri_8 = _at(2026, 11, 6, 8, 0)
            job = sched.ScheduledJob.from_policy(
                self._policy(WORKDAYS), "kaffeete", 1, "on", fri_8)
            self.assertEqual(job.deadline_ts, _at(2026, 11, 9, 7, 0))
            self.assertTrue(job.rearm(_at(2026, 11, 9, 7, 0)))
            self.assertEqual(job.deadline_ts, _at(2026, 11, 10, 7, 0))

    def test_round_trip_and_snapshot(self):
        job = sched.ScheduledJob.from_policy(
            self._policy(WORKDAYS), "kaffeete", 1, "on", 1_700_000_000)
        d = job.to_dict()
        self.assertEqual(d["weekdays"], [0, 1, 2, 3, 4])
        self.assertEqual(sched.ScheduledJob.from_dict(d).weekdays, WORKDAYS)
        self.assertEqual(job.to_snapshot()["weekdays"], [0, 1, 2, 3, 4])

    def test_legacy_dict_has_no_filter(self):
        job = sched.ScheduledJob.from_policy(
            self._policy(None), "kaffeete", 1, "on", 1_700_000_000)
        d = job.to_dict()
        del d["weekdays"]
        self.assertIsNone(sched.ScheduledJob.from_dict(d).weekdays)
        self.assertIsNone(job.to_snapshot()["weekdays"])


if __name__ == "__main__":
    unittest.main()
