"""A cancelled rule must not fire.

Both tick paths select the rules to fire under the twin lock, release
it, and only then run the side effects (MQTT publish + chat post). A
cancel arriving in that window — the app's × button, or
`/<dev> cancel-auto-off` from chat — removes the job from `self.rules`
but not from the already-built `fires` list, so the user gets
"cancelled 1 schedule(s)" and the plug switches a moment later anyway.

The state-rule path holds the window open much longer: `_eval_consumed`
and `_eval_avg` run several SQL round-trips outside the lock on jobs
captured before it was released.
"""

import threading
import time
import unittest

from mqtt_bot.core import rules as sched

from tests._fixtures import _build_twin


class TestCancelBeatsFire(unittest.TestCase):
    def _twin_with_due_timer(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True          # not dormant for an off-rule
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        job = twin.jobs_snapshot()[0]
        job.deadline_ts = 0                   # due now
        job._loaded_at = 0                    # skip the restart grace
        calls["published"].clear()
        return twin, calls, job

    def test_cancel_between_selection_and_fire_wins(self):
        """Deterministic reproduction of the window: the rule is
        selected, then cancelled, then the fire is attempted."""
        twin, calls, job = self._twin_with_due_timer()
        real_fire = twin._fire_rule
        fired_jobs = []

        def fire_after_cancelling(j, mode, ctx):
            # Stand in for the DC handler thread cancelling while this
            # thread sits between the lock release and the side effect.
            twin.cancel(target_action="off")
            fired_jobs.append(j)
            real_fire(j, mode, ctx)

        twin._fire_rule = fire_after_cancelling
        twin.tick_time(10 ** 9)

        self.assertEqual(calls["published"], [],
                         "a cancelled rule still switched the plug")

    def test_uncancelled_rule_still_fires(self):
        twin, calls, _ = self._twin_with_due_timer()
        twin.tick_time(10 ** 9)
        self.assertEqual(len(calls["published"]), 1,
                         "the guard suppressed a legitimate fire")

    def test_cancel_marks_the_job(self):
        twin, _, job = self._twin_with_due_timer()
        self.assertFalse(job._cancelled)
        twin.cancel(target_action="off")
        self.assertTrue(job._cancelled,
                        "cancel must mark the job, not only unlist it")

    def test_cancelled_flag_is_not_persisted(self):
        """It is transient race-guard state, not part of a rule."""
        twin, _, job = self._twin_with_due_timer()
        twin.cancel(target_action="off")
        self.assertNotIn("_cancelled", job.to_dict())

    def test_concurrent_cancel_is_all_or_nothing(self):
        """The realistic version: a ticker and a canceller racing.

        Either outcome is legitimate — the cancel lands first and
        nothing is published, or the fire completes first and publishes
        exactly once. What must never happen is a partial result: more
        than one publish, or a publish from a job the twin has already
        reported cancelled AND marked.
        """
        for attempt in range(15):
            with self.subTest(attempt=attempt):
                twin, calls, job = self._twin_with_due_timer()
                barrier = threading.Barrier(2, timeout=5)
                errors: list[BaseException] = []

                def guarded(fn):
                    def run():
                        try:
                            barrier.wait()
                            fn()
                        except BaseException as ex:   # noqa: BLE001
                            errors.append(ex)
                    return run

                threads = [
                    threading.Thread(target=guarded(
                        lambda: twin.tick_time(10 ** 9))),
                    threading.Thread(target=guarded(
                        lambda: twin.cancel(target_action="off"))),
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10)

                # Leaked threads destabilise every test that runs after
                # this one, so treat a hang as a failure here and now.
                self.assertFalse([t for t in threads if t.is_alive()],
                                 "thread did not finish")
                self.assertEqual([repr(e) for e in errors], [])
                self.assertLessEqual(len(calls["published"]), 1,
                                     "rule fired more than once")
                if job._cancelled:
                    self.assertEqual(
                        calls["published"], [],
                        "job was marked cancelled yet still published")

    def test_state_rule_path_also_honours_cancel(self):
        """This path holds the window open across SQL round-trips."""
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True
        twin.schedule("off", sched.ScheduledPolicy(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=1), 12)
        job = twin.jobs_snapshot()[0]
        job._loaded_at = 0
        job._below_since = 0                  # already satisfied
        calls["published"].clear()

        real_fire = twin._fire_rule

        def fire_after_cancelling(j, mode, ctx):
            twin.cancel(target_action="off")
            real_fire(j, mode, ctx)

        twin._fire_rule = fire_after_cancelling
        twin._tick_state_rules({"apower": 0.0, "output": True}, 10 ** 9)

        self.assertEqual(calls["published"], [],
                         "a cancelled state rule still switched the plug")


if __name__ == "__main__":
    unittest.main()
