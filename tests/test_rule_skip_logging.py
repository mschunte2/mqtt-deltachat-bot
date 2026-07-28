"""'My rule didn't fire' must be answerable from the journal.

Only the positive path logged. Every negative decision point — dormancy,
the post-restart grace period, the avg/consumed warm-up gates, the
avg-coverage gate — was a bare `continue` or `return False`. So at ANY
log level, including debug, the journal contained nothing explaining a
rule that never fired.

Dormancy is the most common real cause (an off-rule on an already-off
plug is silent by design) and the user had no way to learn it. The
avg-coverage gate is the hardest to guess from outside: a rule refuses
to fire because the device was offline for more than 10% of the window.
"""

import logging
import time
import unittest

from mqtt_bot.core import rules as sched

from tests._fixtures import _FakeHistory, _build_twin


class _Base(unittest.TestCase):
    def assertSkipLogged(self, needle, fn):
        with self.assertLogs("mqtt_bot.plug", level="DEBUG") as cap:
            fn()
        joined = "\n".join(cap.output)
        self.assertIn("rule skip", joined,
                      f"no skip reason logged at all; got: {joined}")
        self.assertIn(needle, joined,
                      f"skip reason did not mention {needle!r}; got: {joined}")


class TestDormancyIsExplained(_Base):
    def test_off_rule_on_an_already_off_plug_says_dormant(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = False           # already off
        twin.schedule("off", sched.ScheduledPolicy(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60), 12)
        self.assertSkipLogged(
            "dormant",
            lambda: twin._tick_state_rules({"apower": 0.0, "output": False},
                                           int(time.time())))

    def test_dormant_message_names_the_device_and_action(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule("off", sched.ScheduledPolicy(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60), 12)
        with self.assertLogs("mqtt_bot.plug", level="DEBUG") as cap:
            twin._tick_state_rules({"apower": 0.0, "output": False},
                                   int(time.time()))
        joined = "\n".join(cap.output)
        self.assertIn(twin.name, joined)
        self.assertIn("off", joined)


class TestWarmupGatesAreExplained(_Base):
    def _twin_with(self, policy):
        # The consumed/avg evaluators return early when history is None,
        # before reaching their warm-up gates.
        twin, _, _ = _build_twin(history=_FakeHistory())
        twin.fields["output"] = True            # not dormant for an off-rule
        twin.schedule("off", policy, 12)
        return twin

    def test_consumed_warmup_is_logged(self):
        twin = self._twin_with(sched.ScheduledPolicy(
            consumed_field="apower", consumed_threshold_wh=5.0,
            consumed_window_s=600))
        self.assertSkipLogged(
            "warm-up",
            lambda: twin._tick_state_rules({"apower": 0.0, "output": True},
                                           int(time.time())))

    def test_avg_warmup_is_logged(self):
        twin = self._twin_with(sched.ScheduledPolicy(
            avg_field="apower", avg_threshold_w=5.0, avg_window_s=600))
        self.assertSkipLogged(
            "warm-up",
            lambda: twin._tick_state_rules({"apower": 0.0, "output": True},
                                           int(time.time())))

    def test_warmup_message_reports_progress_not_just_the_fact(self):
        """'not warmed up' is useless; 'Ns of 600s observed' tells the
        user whether to keep waiting."""
        twin = self._twin_with(sched.ScheduledPolicy(
            avg_field="apower", avg_threshold_w=5.0, avg_window_s=600))
        with self.assertLogs("mqtt_bot.plug", level="DEBUG") as cap:
            twin._tick_state_rules({"apower": 0.0, "output": True},
                                   int(time.time()))
        self.assertIn("600", "\n".join(cap.output))


class TestGracePeriodIsExplained(_Base):
    def test_post_restart_grace_is_logged(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = True
        twin.schedule("off", sched.ScheduledPolicy(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=1), 12)
        job = twin.jobs_snapshot()[0]
        job._loaded_at = int(time.time())       # just restored from rules.json
        self.assertSkipLogged(
            "grace",
            lambda: twin._tick_state_rules({"apower": 0.0, "output": True},
                                           int(time.time())))


class TestSkipLoggingIsCheapWhenDisabled(unittest.TestCase):
    def test_nothing_emitted_above_debug(self):
        """A busy device evaluates rules on every status update, so the
        skip lines must not reach a default INFO deployment."""
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule("off", sched.ScheduledPolicy(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60), 12)
        logger = logging.getLogger("mqtt_bot.plug")
        with self.assertLogs(logger, level="DEBUG") as cap:
            logger.debug("sentinel")            # assertLogs needs >=1 record
            prev = logger.level
            logger.setLevel(logging.INFO)
            try:
                twin._tick_state_rules({"apower": 0.0, "output": False},
                                       int(time.time()))
            finally:
                logger.setLevel(prev)
        self.assertEqual([r for r in cap.output if "rule skip" in r], [])


if __name__ == "__main__":
    unittest.main()
