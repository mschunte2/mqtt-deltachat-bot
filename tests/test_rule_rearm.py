"""Restart must preserve exactly the rules that keep firing at runtime.

Three code paths independently decided what "recurring" means:

  twin.tick_time      re-arms when `not job.once` (TOD -> next
                      occurrence, timer -> now + timer_seconds)
  rules.load_into     re-armed only when `job.recurring_tod and
                      job.time_of_day`, with no timer branch at all
  formatters          printed " daily" only when `job.recurring_tod`

Since v0.1.5 the recurrence flag is `once`, not `recurring_tod`, so
load_into silently dropped every recurring timer rule and every
non-`daily` TOD rule whose deadline had passed during downtime — while
the running bot treated both as recurring. `/kaffeete off in 30m`
vanished on any restart more than 30 minutes later, with no chat
notification.
"""

import tempfile
import time
import unittest
from pathlib import Path

from mqtt_bot.core import rules as sched
from mqtt_bot.core.twins import TwinRegistry
from mqtt_bot import formatters

from tests._fixtures import _build_twin


class _Base(unittest.TestCase):
    def setUp(self):
        self.twin, self.calls, _ = _build_twin()
        self.registry = TwinRegistry([self.twin])
        self.path = Path(tempfile.mkdtemp()) / "rules.json"
        self.now = int(time.time())

    def _save_then_expire(self, minutes_ago=60):
        """Persist current rules, then rewind every deadline into the
        past to simulate downtime longer than the rule's period."""
        sched.save_all(self.registry, self.path)
        import json
        doc = json.loads(self.path.read_text())
        for j in doc["jobs"]:
            if j.get("deadline_ts"):
                j["deadline_ts"] = self.now - minutes_ago * 60
        self.path.write_text(json.dumps(doc))
        self.twin.rules.clear()
        return sched.load_into(self.registry, self.path)


class TestRecurringRulesSurviveRestart(_Base):
    def test_recurring_timer_rule_is_rearmed_not_dropped(self):
        self.twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        loaded = self._save_then_expire()

        self.assertEqual(loaded, 1, "recurring timer rule was dropped")
        job = self.twin.jobs_snapshot()[0]
        self.assertFalse(job.once)
        self.assertGreater(job.deadline_ts, self.now,
                           "re-armed deadline must be in the future")
        self.assertLessEqual(job.deadline_ts, self.now + 1800 + 5)

    def test_non_daily_tod_rule_is_rearmed_not_dropped(self):
        self.twin.schedule("off", sched.ScheduledPolicy(
            time_of_day=(18, 30), recurring_tod=False), 12)
        loaded = self._save_then_expire()

        self.assertEqual(loaded, 1, "non-daily TOD rule was dropped")
        job = self.twin.jobs_snapshot()[0]
        self.assertGreater(job.deadline_ts, self.now)

    def test_recurring_tod_rule_still_rearmed(self):
        self.twin.schedule("on", sched.ScheduledPolicy(
            time_of_day=(7, 0), recurring_tod=True), 12)
        loaded = self._save_then_expire()

        self.assertEqual(loaded, 1)
        self.assertGreater(self.twin.jobs_snapshot()[0].deadline_ts, self.now)

    def test_one_shot_rule_is_still_dropped(self):
        self.twin.schedule("off", sched.ScheduledPolicy(
            timer_seconds=1800, once=True), 12)
        loaded = self._save_then_expire()

        self.assertEqual(loaded, 0, "expired one-shot must not survive")
        self.assertEqual(self.twin.jobs_snapshot(), [])

    def test_future_deadline_is_untouched(self):
        self.twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        sched.save_all(self.registry, self.path)
        before = self.twin.jobs_snapshot()[0].deadline_ts
        self.twin.rules.clear()

        self.assertEqual(sched.load_into(self.registry, self.path), 1)
        self.assertEqual(self.twin.jobs_snapshot()[0].deadline_ts, before)


class TestRearmAgreesWithRuntime(_Base):
    """load_into and tick_time must reach the same verdict for the same
    rule — that divergence is what made the bug invisible."""

    def _runtime_survives(self, policy):
        twin, _, _ = _build_twin()
        twin.schedule("off", policy, 12)
        job = twin.jobs_snapshot()[0]
        job.deadline_ts = self.now - 3600
        job._loaded_at = 0          # skip the post-restart grace period
        twin.fields["output"] = True   # not dormant for an off-rule
        twin.tick_time(self.now)
        return len(twin.jobs_snapshot()) == 1

    def _restart_survives(self, policy):
        self.twin.rules.clear()
        self.twin.schedule("off", policy, 12)
        return self._save_then_expire() == 1

    def test_verdicts_match_across_policy_shapes(self):
        shapes = {
            "recurring timer": sched.ScheduledPolicy(timer_seconds=1800),
            "one-shot timer": sched.ScheduledPolicy(timer_seconds=1800,
                                                    once=True),
            "plain tod": sched.ScheduledPolicy(time_of_day=(18, 30)),
            "daily tod": sched.ScheduledPolicy(time_of_day=(18, 30),
                                               recurring_tod=True),
            "one-shot tod": sched.ScheduledPolicy(time_of_day=(18, 30),
                                                  once=True),
        }
        for label, policy in shapes.items():
            with self.subTest(label):
                self.assertEqual(
                    self._restart_survives(policy),
                    self._runtime_survives(policy),
                    f"{label}: restart and runtime disagree on recurrence")


class TestRuleDisplayMatchesBehaviour(unittest.TestCase):
    def test_non_once_tod_rule_renders_as_daily(self):
        """A TOD rule with once=False re-arms to the next occurrence
        every day, so calling it anything but daily misleads the user."""
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(
            time_of_day=(18, 30), recurring_tod=False), 12)
        job = twin.jobs_snapshot()[0]

        self.assertIn("daily", " ".join(formatters.rule_clauses(job)))
        self.assertNotIn("(once)",
                         " ".join(formatters.format_rule_lines(job)))

    def test_one_shot_tod_rule_does_not_render_as_daily(self):
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(
            time_of_day=(18, 30), once=True), 12)
        job = twin.jobs_snapshot()[0]

        self.assertNotIn("daily", " ".join(formatters.rule_clauses(job)))
        self.assertIn("(once)", " ".join(formatters.format_rule_lines(job)))


if __name__ == "__main__":
    unittest.main()
