"""Tests for RulesSweeper's loop robustness.

The sweeper is a single daemon thread and the only thing that fires
timer and time-of-day rules. Before this suite existed, only
`twin.tick_time` was inside the loop's try/except — the deadline
computation and the `Event.wait(timeout=...)` were not. Any exception
there exits the thread silently and permanently: `start()` refuses to
restart it, so every timed rule on every device stops firing until the
process is restarted.

The concrete reachable case was an unbounded timer. `Event.wait` raises
OverflowError above threading.TIMEOUT_MAX, and a poison rule is
persisted to rules.json before the sweeper ever sees it — so a restart
reproduced the outage and only hand-editing the file recovered.
"""

import threading
import time
import unittest

from mqtt_bot.core import rules as sched


class _FakeTwin:
    def __init__(self, name, deadline=None, deadline_exc=None):
        self.name = name
        self._deadline = deadline
        self._deadline_exc = deadline_exc
        self.ticks = 0
        self.ticked = threading.Event()

    def next_deadline(self):
        if self._deadline_exc is not None:
            raise self._deadline_exc
        return self._deadline

    def tick_time(self, now):
        self.ticks += 1
        self.ticked.set()


class _FakeRegistry:
    def __init__(self, twins):
        self._twins = list(twins)

    def all(self):
        return list(self._twins)


class _SweeperHarness:
    """Start a sweeper, wait for it to do work, always stop it."""

    def __init__(self, testcase, twins):
        self.twins = twins
        self.sweeper = sched.RulesSweeper(_FakeRegistry(twins))
        testcase.addCleanup(self._shutdown)

    def _shutdown(self):
        self.sweeper.stop()
        t = self.sweeper._thread
        if t is not None:
            t.join(timeout=5)

    def run_until_tick(self, twin, timeout=5):
        self.sweeper.start()
        self.sweeper.wake()
        return twin.ticked.wait(timeout=timeout)

    def alive(self):
        t = self.sweeper._thread
        return t is not None and t.is_alive()


class TestSweeperSurvivesBadDeadlines(unittest.TestCase):
    def test_absurd_deadline_does_not_kill_the_thread(self):
        """A deadline beyond threading.TIMEOUT_MAX must not be fatal.

        The poison twin is alone so that its deadline really is the
        minimum the loop waits on. Fails against the previous loop with
        OverflowError, after which the thread is gone and no rule ever
        fires again.
        """
        poison = _FakeTwin("kaffeete", deadline=int(time.time()) + 10 ** 17)
        h = _SweeperHarness(self, [poison])

        self.assertTrue(h.run_until_tick(poison),
                        "sweeper died on an out-of-range deadline")
        self.assertTrue(h.alive(), "sweeper thread exited")

    def test_absurd_deadline_alongside_a_healthy_twin(self):
        """The mixed case: one poison rule must not stop the other
        device's rules from firing."""
        poison = _FakeTwin("kaffeete", deadline=int(time.time()) + 10 ** 17)
        healthy = _FakeTwin("km", deadline=int(time.time()) + 1)
        h = _SweeperHarness(self, [poison, healthy])

        self.assertTrue(h.run_until_tick(healthy),
                        "sweeper died before ticking a healthy twin")
        self.assertTrue(h.alive(), "sweeper thread exited")

    def test_raising_next_deadline_does_not_kill_the_thread(self):
        broken = _FakeTwin("kaffeete", deadline_exc=RuntimeError("boom"))
        healthy = _FakeTwin("km", deadline=int(time.time()) + 1)
        h = _SweeperHarness(self, [broken, healthy])

        self.assertTrue(h.run_until_tick(healthy),
                        "one twin's broken next_deadline stopped the sweeper")
        self.assertTrue(h.alive())

    def test_raising_tick_does_not_stop_other_twins(self):
        class Exploding(_FakeTwin):
            def tick_time(self, now):
                super().tick_time(now)
                raise RuntimeError("tick failed")

        bad = Exploding("kaffeete", deadline=int(time.time()) + 1)
        good = _FakeTwin("km", deadline=int(time.time()) + 1)
        h = _SweeperHarness(self, [bad, good])

        self.assertTrue(h.run_until_tick(good))
        self.assertTrue(h.alive())

    def test_stop_ends_the_thread(self):
        twin = _FakeTwin("kaffeete", deadline=int(time.time()) + 1)
        h = _SweeperHarness(self, [twin])
        h.sweeper.start()
        h.sweeper.stop()
        h.sweeper._thread.join(timeout=5)
        self.assertFalse(h.alive())


class TestDurationBounds(unittest.TestCase):
    """`durations.parse` fed ScheduledJob.deadline_ts with no ceiling, so
    `/kaffeete off in 999999999999d` produced a deadline ~8.6e16 seconds
    out. Bound it at the parser, which is the shared entry point for
    every chat-typed duration."""

    def test_rejects_absurd_duration(self):
        from mqtt_bot.util import durations
        with self.assertRaises(ValueError):
            durations.parse("999999999999d")

    def test_rejects_just_over_the_cap(self):
        from mqtt_bot.util import durations
        over = durations.MAX_SECONDS // 86400 + 1
        with self.assertRaises(ValueError):
            durations.parse(f"{over}d")

    def test_accepts_the_cap(self):
        from mqtt_bot.util import durations
        self.assertEqual(durations.parse(f"{durations.MAX_SECONDS}s"),
                         durations.MAX_SECONDS)

    def test_ordinary_durations_still_parse(self):
        from mqtt_bot.util import durations
        self.assertEqual(durations.parse("30m"), 1800)
        self.assertEqual(durations.parse("1h30m"), 5400)
        self.assertEqual(durations.parse("7d"), 604800)


class TestPoisonRuleIsNotLoaded(unittest.TestCase):
    """A poison rule written by an earlier version is still on disk in
    any deployment that hit the bug, and load_into re-armed it happily
    (its deadline is in the future, so it never looked expired)."""

    def test_out_of_range_deadline_is_dropped_on_load(self):
        import json
        import tempfile
        from pathlib import Path

        from mqtt_bot.core.twins import TwinRegistry
        from mqtt_bot.util import durations
        from tests._fixtures import _build_twin

        twin, _calls, _cfg = _build_twin()
        registry = TwinRegistry([twin])
        now = int(time.time())

        # Two real timer rules, saved the way the bot saves them...
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        twin.schedule("on", sched.ScheduledPolicy(timer_seconds=900), 12)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "rules.json"
            sched.save_all(registry, p)

            # ...then poison one of them the way the old unbounded
            # parser would have: a deadline ~8.6e16 seconds out.
            doc = json.loads(p.read_text())
            self.assertEqual(len(doc["jobs"]), 2)
            poisoned_id = doc["jobs"][0]["rule_id"]
            survivor_id = doc["jobs"][1]["rule_id"]
            doc["jobs"][0]["deadline_ts"] = now + 10 ** 17
            doc["jobs"][0]["timer_seconds"] = 10 ** 17
            p.write_text(json.dumps(doc))

            twin.rules.clear()
            with self.assertLogs("mqtt_bot.rules", level="WARNING"):
                loaded = sched.load_into(registry, p)

        self.assertEqual(loaded, 1)
        ids = [j.rule_id for j in twin.jobs_snapshot()]
        self.assertEqual(ids, [survivor_id])
        self.assertNotIn(poisoned_id, ids)
        self.assertLessEqual(twin.next_deadline(),
                             now + durations.MAX_SECONDS)


if __name__ == "__main__":
    unittest.main()
