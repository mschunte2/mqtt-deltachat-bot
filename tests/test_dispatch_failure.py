"""A switch that did not happen must not be reported as one.

MqttClient.publish returned None unconditionally: a non-success rc
(MQTT_ERR_NO_CONN while paho reconnects, or a dead broker) was logged
at WARNING and discarded. dispatch() then returned (True, ""), reacted
🆗, and broadcast; _fire_rule posted "switching off kaffeete after 30m
idle" to chat.

So with the broker down the user was told the espresso machine was off
while it stayed on, and the only trace was a WARNING in the journal.
For a safety feature on a mains appliance, fail-open with a success ack
is the wrong failure mode.
"""

import dataclasses
import unittest

from mqtt_bot.core import rules as sched

from tests._fixtures import _build_twin


class _Base(unittest.TestCase):
    def _twin(self, publish_result=True):
        """Twin whose mqtt_publish reports `publish_result`.

        TwinDeps is frozen, so swap the whole deps object rather than
        assigning the field.
        """
        twin, calls, _ = _build_twin()
        calls["published"] = []

        def publish(topic, payload):
            calls["published"].append((topic, payload))
            return publish_result

        twin.deps = dataclasses.replace(twin.deps, mqtt_publish=publish)
        return twin, calls


class TestDispatchReportsPublishFailure(_Base):
    def test_failed_publish_returns_not_ok(self):
        twin, _ = self._twin(publish_result=False)
        ok, msg = twin.dispatch("off")
        self.assertFalse(ok, "dispatch claimed success on a failed publish")
        self.assertTrue(msg, "a failed dispatch must explain itself")

    def test_failed_publish_does_not_react_ok(self):
        twin, calls = self._twin(publish_result=False)
        twin.dispatch("off", source_msgid=4242)
        oks = [e for _, e in calls["reactions"] if e == "🆗"]
        self.assertEqual(oks, [], "🆗 reacted to a switch that never happened")

    def test_successful_publish_still_reacts_ok(self):
        twin, calls = self._twin(publish_result=True)
        ok, _ = twin.dispatch("off", source_msgid=4242)
        self.assertTrue(ok)
        self.assertIn((4242, "🆗"), calls["reactions"])

    def test_publish_still_attempted_on_failure(self):
        twin, calls = self._twin(publish_result=False)
        twin.dispatch("off")
        self.assertEqual(len(calls["published"]), 1)

    def test_legacy_publish_returning_none_is_treated_as_success(self):
        """TwinDeps.mqtt_publish was typed -> None. Anything that still
        returns None (a test fake, a future backend) must not suddenly
        start reporting every switch as failed."""
        twin, calls = self._twin(publish_result=None)
        ok, _ = twin.dispatch("off", source_msgid=1)
        self.assertTrue(ok)
        self.assertIn((1, "🆗"), calls["reactions"])


class TestRuleFireReportsPublishFailure(_Base):
    def _fire_timer_rule(self, publish_result):
        twin, calls = self._twin(publish_result=publish_result)
        twin.fields["output"] = True          # not dormant for an off-rule
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1), 12)
        job = twin.jobs_snapshot()[0]
        job.deadline_ts = 0                   # due now
        job._loaded_at = 0
        twin.tick_time(10 ** 9)
        return calls

    def test_chat_is_not_told_the_plug_switched_when_it_did_not(self):
        calls = self._fire_timer_rule(publish_result=False)
        self.assertEqual(len(calls["published"]), 1)
        switched = [t for _n, t in calls["posted"] if "switch" in t.lower()]
        self.assertEqual(
            switched, [],
            "chat was told the plug switched off after a failed publish")

    def test_failed_rule_fire_warns_the_chat(self):
        calls = self._fire_timer_rule(publish_result=False)
        texts = " ".join(t for _n, t in calls["posted"]).lower()
        self.assertTrue(
            any(w in texts for w in ("could not", "failed", "not reach")),
            f"a failed rule fire must say so; chat got: {calls['posted']!r}")

    def test_successful_rule_fire_still_posts_the_trigger_message(self):
        calls = self._fire_timer_rule(publish_result=True)
        self.assertTrue(calls["posted"],
                        "a successful rule fire should still notify chat")


if __name__ == "__main__":
    unittest.main()
