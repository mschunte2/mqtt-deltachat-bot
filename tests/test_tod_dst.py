"""next_tod_deadline across DST boundaries.

`next_tod_deadline` advanced to "tomorrow" with `now + 86400`. On the
25-hour fall-back day that lands on the *same* calendar date when now
is between 00:00 and 00:59 local, so the function returned a timestamp
in the PAST.

Consequence: tick_time sees deadline <= now, fires, re-arms to the same
past timestamp, and the sweeper's wait clamps to 0.5s — repeating twice
a second for up to 30 minutes, rewriting rules.json each time (~3600
writes). For on/off targets _job_dormant eventually suppresses the chat
post once the device reaches the target state, but the write storm and
CPU spin continue. For a `toggle` target _job_dormant always returns
False, so the plug is toggled and the chat spammed at 2 Hz for half an
hour.

The existing test only asserted "in the future and within 25h" against
the live wall clock, so it passed 364 days a year. These pin TZ and the
date instead.
"""

import os
import time
import unittest
from contextlib import contextmanager

from mqtt_bot.core import rules as sched


@contextmanager
def _tz(name: str):
    """Pin the process timezone for the duration of the block."""
    prev = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prev
        time.tzset()


def _at(y, mo, d, h, mi):
    """Local wall-clock time -> unix seconds, under the ambient TZ."""
    return int(time.mktime((y, mo, d, h, mi, 0, 0, 0, -1)))


# Europe/Berlin: clocks go back 03:00 -> 02:00 on 2026-10-25 (25h day),
# and forward 02:00 -> 03:00 on 2026-03-29 (23h day).
_FALL_BACK = (2026, 10, 25)
_SPRING_FWD = (2026, 3, 29)


class TestFallBackDay(unittest.TestCase):
    def test_deadline_is_never_in_the_past(self):
        with _tz("Europe/Berlin"):
            now = _at(*_FALL_BACK, 0, 30)
            for h, m in ((0, 0), (0, 15), (0, 29)):
                with self.subTest(target=f"{h:02d}:{m:02d}"):
                    got = sched.next_tod_deadline(h, m, now)
                    self.assertGreater(
                        got, now,
                        f"{h:02d}:{m:02d} from 00:30 on the fall-back day "
                        f"resolved to {time.strftime('%F %T %Z', time.localtime(got))}")

    def test_deadline_lands_on_the_requested_wall_clock_time(self):
        with _tz("Europe/Berlin"):
            now = _at(*_FALL_BACK, 0, 30)
            got = sched.next_tod_deadline(0, 15, now)
            lt = time.localtime(got)
            self.assertEqual((lt.tm_hour, lt.tm_min), (0, 15))
            self.assertEqual(lt.tm_mday, 26, "should be the NEXT day")

    def test_later_target_same_day_still_works(self):
        with _tz("Europe/Berlin"):
            now = _at(*_FALL_BACK, 0, 30)
            got = sched.next_tod_deadline(18, 30, now)
            lt = time.localtime(got)
            self.assertEqual((lt.tm_hour, lt.tm_min), (18, 30))
            self.assertEqual(lt.tm_mday, 25, "18:30 is still ahead today")

    def test_rearm_after_firing_moves_forward(self):
        """The storm is the re-arm, not the first fire: a rule that
        re-arms to a past timestamp fires again immediately."""
        with _tz("Europe/Berlin"):
            now = _at(*_FALL_BACK, 0, 30)
            job = sched.ScheduledJob(
                device_name="kaffeete", chat_id_origin=12,
                target_action="toggle", rule_id="r",
                deadline_ts=now - 60, time_of_day=(0, 15),
                _time_mode="tod", once=False,
            )
            self.assertTrue(job.rearm(now))
            self.assertGreater(job.deadline_ts, now,
                               "re-armed into the past — this is the 2 Hz "
                               "fire/re-arm storm")


class TestSpringForwardDay(unittest.TestCase):
    def test_non_existent_local_time_resolves_forward(self):
        """02:30 does not exist on the spring-forward day; mktime with
        isdst=-1 resolves it to 03:30 CEST rather than failing."""
        with _tz("Europe/Berlin"):
            now = _at(*_SPRING_FWD, 0, 30)
            got = sched.next_tod_deadline(2, 30, now)
            self.assertGreater(got, now)
            self.assertLess(got - now, 26 * 3600)

    def test_deadline_is_never_in_the_past_on_short_day(self):
        with _tz("Europe/Berlin"):
            now = _at(*_SPRING_FWD, 23, 30)
            for h, m in ((0, 0), (7, 0), (23, 0)):
                with self.subTest(target=f"{h:02d}:{m:02d}"):
                    self.assertGreater(sched.next_tod_deadline(h, m, now), now)


class TestOrdinaryDays(unittest.TestCase):
    def test_target_later_today(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 6, 15, 9, 0)
            got = sched.next_tod_deadline(18, 30, now)
            self.assertEqual(got, _at(2026, 6, 15, 18, 30))

    def test_target_already_passed_rolls_to_tomorrow(self):
        with _tz("Europe/Berlin"):
            now = _at(2026, 6, 15, 19, 0)
            got = sched.next_tod_deadline(18, 30, now)
            self.assertEqual(got, _at(2026, 6, 16, 18, 30))

    def test_month_and_year_rollover(self):
        with _tz("Europe/Berlin"):
            got = sched.next_tod_deadline(7, 0, _at(2026, 12, 31, 23, 30))
            lt = time.localtime(got)
            self.assertEqual((lt.tm_year, lt.tm_mon, lt.tm_mday,
                              lt.tm_hour, lt.tm_min), (2027, 1, 1, 7, 0))

    def test_utc_deployment_unaffected(self):
        with _tz("UTC"):
            now = _at(2026, 10, 25, 0, 30)
            self.assertGreater(sched.next_tod_deadline(0, 15, now), now)



class TestEnergyBucketBoundaries(unittest.TestCase):
    """Daily and weekly boundaries used fixed 86400-second steps. A DST
    change makes one local day 23 or 25 hours long, so every boundary
    after the transition drifted to 23:00 or 01:00 and the daily-energy
    bars stopped lining up with local midnight."""

    def test_daily_boundaries_stay_at_local_midnight_across_dst(self):
        from mqtt_bot.io.history import _local_midnights_back_from

        with _tz("Europe/Berlin"):
            # Ends the day after the autumn transition, reaching back
            # across it.
            end = _at(2026, 10, 27, 0, 0)
            starts = _local_midnights_back_from(end, 5)
            self.assertEqual(len(starts), 5)
            for ts in starts:
                lt = time.localtime(ts)
                self.assertEqual((lt.tm_hour, lt.tm_min), (0, 0),
                                 f"{time.strftime('%F %T %Z', lt)} is not "
                                 f"local midnight")

    def test_one_day_really_is_25_hours_on_the_fall_back_day(self):
        from mqtt_bot.io.history import _local_midnights_back_from

        with _tz("Europe/Berlin"):
            starts = _local_midnights_back_from(_at(2026, 10, 27, 0, 0), 5)
            gaps = [b - a for a, b in zip(starts, starts[1:])]
            self.assertIn(25 * 3600, gaps,
                          "the fall-back day should be 25 hours long")

    def test_daily_boundaries_across_spring_forward(self):
        from mqtt_bot.io.history import _local_midnights_back_from

        with _tz("Europe/Berlin"):
            starts = _local_midnights_back_from(_at(2026, 3, 31, 0, 0), 5)
            gaps = [b - a for a, b in zip(starts, starts[1:])]
            self.assertIn(23 * 3600, gaps,
                          "the spring-forward day should be 23 hours long")
            for ts in starts:
                lt = time.localtime(ts)
                self.assertEqual((lt.tm_hour, lt.tm_min), (0, 0))

    def test_boundaries_are_ascending_and_end_on_the_caller_value(self):
        from mqtt_bot.io.history import _local_midnights_back_from

        with _tz("Europe/Berlin"):
            end = _at(2026, 10, 27, 0, 0)
            starts = _local_midnights_back_from(end, 30)
            self.assertEqual(starts, sorted(starts))
            self.assertEqual(starts[-1], end)

    def test_week_start_is_monday_midnight_across_dst(self):
        from mqtt_bot.core.snapshot import _local_week_start

        with _tz("Europe/Berlin"):
            # Wednesday after the autumn change; Monday is on the other
            # side of it.
            got = _local_week_start(_at(2026, 10, 28, 15, 0))
            lt = time.localtime(got)
            self.assertEqual(lt.tm_wday, 0, "should be a Monday")
            self.assertEqual((lt.tm_hour, lt.tm_min), (0, 0),
                             "should be local midnight, not 23:00/01:00")

    def test_week_start_ordinary_week(self):
        from mqtt_bot.core.snapshot import _local_week_start

        with _tz("Europe/Berlin"):
            got = _local_week_start(_at(2026, 6, 17, 12, 0))   # a Wednesday
            self.assertEqual(got, _at(2026, 6, 15, 0, 0))      # that Monday


if __name__ == "__main__":
    unittest.main()
