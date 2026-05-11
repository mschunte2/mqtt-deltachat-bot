"""Tests for PlugTwin: on_mqtt edge handling, dispatch, schedule,
cancel, tick_time, manual-toggle resets, counter-reset detection,
and the resettable Counter feature."""

import json
import tempfile
import time
import unittest
from pathlib import Path

from mqtt_bot.io import history as history_mod
from mqtt_bot.core import twin as plug_mod
from mqtt_bot.core import rules as sched

from tests._fixtures import _build_twin


# --- on_mqtt: state extraction, on_change, threshold ---------------------

class TestPlugTwinOnMqtt(unittest.TestCase):
    def test_on_change_fires_post_and_broadcast(self):
        twin, calls, _ = _build_twin()
        # First arrival: prev=None, new=False — that IS a transition, so
        # on_change fires (good — boot-up shows initial state in chat).
        twin.on_mqtt("status/switch:0", json.dumps({"output": False}).encode())
        self.assertEqual(twin.fields.get("output"), False)
        self.assertEqual(calls["broadcasts"][-1], "kitchen")

        # Re-deliver same value → no edge, no broadcast.
        broadcasts_before = len(calls["broadcasts"])
        twin.on_mqtt("status/switch:0", json.dumps({"output": False}).encode())
        self.assertEqual(len(calls["broadcasts"]), broadcasts_before)

        # Flip → on_change fires and broadcasts.
        twin.on_mqtt("status/switch:0", json.dumps({"output": True}).encode())
        self.assertEqual(twin.fields["output"], True)
        self.assertIn(("kitchen", "💡 kitchen ON"), calls["posted"])
        self.assertEqual(calls["broadcasts"][-1], "kitchen")

    def test_manual_on_resets_off_rule_idle_window(self):
        # User toggles plug ON after a long idle period; off-rule's stale
        # _below_since must be cleared so the user gets a fresh window.
        # Long idle_duration_s so the rule doesn't insta-fire on the
        # stale 2h-ago timestamp before the reset path runs.
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower", idle_threshold=5.0,
                                  idle_duration_s=86400),
            12,
        )
        rule = twin.rules[0]
        stale = int(time.time()) - 7200  # 2h ago
        rule._below_since = stale
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 1.5}).encode())
        # Reset cleared the stale value; _eval_idle then re-stamped
        # _below_since to ~now since apower (1.5) is still < threshold.
        self.assertNotEqual(rule._below_since, stale)
        self.assertGreater(rule._below_since, int(time.time()) - 5)

    def test_manual_on_resets_off_rule_consumed_window(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=5.0,
                                  consumed_window_s=600),
            12,
        )
        rule = twin.rules[0]
        long_ago = int(time.time()) - 7200
        rule._consumed_started_at = long_ago
        rule._observation_started_at = long_ago
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 50.0}).encode())
        # Manual ON edge resets the off-rule window-warmup gate so the
        # user gets a fresh observation period.
        self.assertGreater(rule._consumed_started_at, long_ago)
        self.assertGreater(rule._observation_started_at, long_ago)

    def test_manual_off_resets_on_rule_window(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = True
        twin.schedule(
            "on",
            sched.ScheduledPolicy(idle_field="apower", idle_threshold=5.0,
                                  idle_duration_s=86400),
            12,
        )
        rule = twin.rules[0]
        stale = int(time.time()) - 7200
        rule._below_since = stale
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": False, "apower": 0.0}).encode())
        self.assertNotEqual(rule._below_since, stale)
        self.assertGreater(rule._below_since, int(time.time()) - 5)

    def test_first_seen_output_does_not_reset(self):
        # None→True is bot hydration on startup, not a user edge.
        twin, _, _ = _build_twin()
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower", idle_threshold=5.0,
                                  idle_duration_s=86400),
            12,
        )
        rule = twin.rules[0]
        marker = int(time.time()) - 7200
        rule._below_since = marker
        # Twin starts with no prior `output` field.
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 1.5}).encode())
        # Reset hook was skipped; _eval_idle leaves _below_since alone
        # when v < threshold and _below_since is already set.
        self.assertEqual(rule._below_since, marker)

    def test_threshold_above_then_below(self):
        twin, calls, _ = _build_twin(params={
            "power_threshold_watts": 100,
            "power_threshold_duration_s": 1,
        })
        # First sample above: latches above_since.
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 200}).encode())
        # No fire yet because duration not met.
        self.assertFalse(any("⚠" in t for _, t in calls["posted"]))
        time.sleep(1.5)
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 200}).encode())
        # Above duration: above-template should have fired.
        self.assertTrue(any("⚠" in t for _, t in calls["posted"]))
        # Now drop below threshold — below-template fires.
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 50}).encode())
        self.assertTrue(any("✅" in t for _, t in calls["posted"]))


# --- online-edge debounce (suppress brief LWT flap from broker
#     client-ID collisions during Wi-Fi blips) -----------------------------

class TestOnlineFlapDebounce(unittest.TestCase):
    """Build a twin whose class has an on_change rule for `online`
    (the production `shelly_plug` does; the default test fixture
    doesn't). Then drive online edges and assert the chat post is
    debounced."""

    ONLINE_RULE = {"type": "on_change", "field": "online",
                   "values": {"true": "🟢 {name} back online",
                              "false": "🔴 {name} went offline"}}

    def _twin_with_online_rule(self):
        # Inherit the fixture's chat_events and append the online rule.
        # _build_twin's class_overrides REPLACES top-level keys, so we
        # rebuild the full chat_events list.
        from tests._fixtures import CLASS_JSON_OK
        events = list(CLASS_JSON_OK["chat_events"]) + [self.ONLINE_RULE]
        twin, calls, _ = _build_twin(class_overrides={"chat_events": events})
        # Tighten the debounce so tests run fast.
        twin.ONLINE_FLAP_DEBOUNCE_S = 0.05
        return twin, calls

    def _wait_for_pending(self, twin, timeout=1.0):
        """Block until any queued offline-post timer has fired."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            t = twin._pending_offline_post
            if t is None or not t.is_alive():
                return
            time.sleep(0.01)

    def test_brief_flap_suppresses_both_posts(self):
        twin, calls = self._twin_with_online_rule()
        # Hydrate online=True so we get a real True→False edge.
        twin.on_mqtt("online", b"true")
        calls["posted"].clear()

        twin.on_mqtt("online", b"false")  # offline edge — defer
        twin.on_mqtt("online", b"true")   # quick recovery — cancel
        self._wait_for_pending(twin)

        self.assertEqual(calls["posted"], [],
                         "brief flap should produce no chat posts")

    def test_sustained_offline_posts_after_debounce(self):
        twin, calls = self._twin_with_online_rule()
        twin.on_mqtt("online", b"true")
        calls["posted"].clear()

        twin.on_mqtt("online", b"false")
        # No post yet — still pending.
        self.assertEqual(calls["posted"], [])
        self._wait_for_pending(twin)
        # Debounce window elapsed; offline should be posted.
        self.assertEqual(calls["posted"], [("kitchen", "🔴 kitchen went offline")])

        # Subsequent recovery posts the back-online message normally
        # (no pending timer to suppress).
        twin.on_mqtt("online", b"true")
        self.assertEqual(calls["posted"][-1],
                         ("kitchen", "🟢 kitchen back online"))

    def test_app_broadcasts_immediately_on_offline_edge(self):
        # Debounce only delays the chat post; the app should still see
        # the offline state immediately.
        twin, calls = self._twin_with_online_rule()
        twin.on_mqtt("online", b"true")
        broadcasts_before = len(calls["broadcasts"])
        twin.on_mqtt("online", b"false")
        self.assertGreater(len(calls["broadcasts"]), broadcasts_before)


# --- dispatch: publish + react + broadcast --------------------------------

class TestPlugTwinDispatch(unittest.TestCase):
    def test_dispatch_publishes_and_broadcasts(self):
        twin, calls, _ = _build_twin()
        ok, msg = twin.dispatch("on", source_msgid=42)
        self.assertTrue(ok)
        self.assertEqual(calls["published"], [("p/kitchen/command/switch:0", "on")])
        self.assertEqual(calls["reactions"], [(42, "🆗")])
        self.assertEqual(calls["broadcasts"], ["kitchen"])

    def test_dispatch_unknown_action(self):
        twin, calls, _ = _build_twin()
        ok, msg = twin.dispatch("blender")
        self.assertFalse(ok)
        self.assertIn("unknown action", msg)
        self.assertEqual(calls["published"], [])

    def test_dispatch_preserves_pending_rules(self):
        # Manual toggles never cancel pending rules; only explicit
        # `cancel-auto-*` (or the app's × button) removes a rule.
        cases = [
            ("off", False),  # off on already-off
            ("off", True),   # off on currently-on (state-changing)
            ("on",  True),   # on on already-on
            ("on",  False),  # on on currently-off (state-changing)
        ]
        for action, output in cases:
            with self.subTest(action=action, output=output):
                twin, calls, _ = _build_twin()
                twin.fields["output"] = output
                twin.schedule("off" if action == "off" else "on",
                              sched.ScheduledPolicy(timer_seconds=1800), 12)
                self.assertEqual(len(twin.rules), 1)
                calls["posted"].clear()
                ok, _ = twin.dispatch(action)
                self.assertTrue(ok)
                self.assertEqual(len(twin.rules), 1)
                self.assertFalse(
                    any("cancelled" in t for _, t in calls["posted"])
                )


# --- schedule + cancel ---------------------------------------------------

class TestPlugTwinSchedule(unittest.TestCase):
    def test_schedule_appends_to_rules(self):
        twin, calls, _ = _build_twin()
        policy = sched.ScheduledPolicy(timer_seconds=600)
        ok, msg = twin.schedule("off", policy, chat_id_origin=12)
        self.assertTrue(ok)
        self.assertEqual(len(twin.rules), 1)
        self.assertEqual(calls["saves"], 1)
        self.assertEqual(calls["broadcasts"], ["kitchen"])

    def test_schedule_replaces_same_rule_id(self):
        twin, _, _ = _build_twin()
        # Same policy → same rule_id → replace.
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        self.assertEqual(len(twin.rules), 1)

    def test_schedule_keeps_distinct_rule_ids(self):
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        self.assertEqual(len(twin.rules), 2)

    def test_cancel_filters(self):
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1800), 12)
        cancelled = twin.cancel(target_action="off")
        self.assertEqual(len(cancelled), 2)
        self.assertEqual(twin.rules, [])


# --- tick_time: deadline → fire / re-arm / drop --------------------------

class TestPlugTwinTickTime(unittest.TestCase):
    def test_one_shot_fires_and_drops(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True   # not dormant for off-rule
        policy = sched.ScheduledPolicy(timer_seconds=1, once=True)
        twin.schedule("off", policy, 12)
        twin.tick_time(int(time.time()) + 5)  # past the deadline
        self.assertEqual(twin.rules, [])
        self.assertIn(("p/kitchen/command/switch:0", "off"), calls["published"])
        self.assertTrue(any("🕐" in t for _, t in calls["posted"]))

    def test_recurring_timer_rearms(self):
        twin, _, _ = _build_twin()
        twin.fields["output"] = True
        policy = sched.ScheduledPolicy(timer_seconds=1, once=False)
        twin.schedule("off", policy, 12)
        deadline_before = twin.rules[0].deadline_ts
        twin.tick_time(int(time.time()) + 5)
        # Same rule still present, deadline pushed forward.
        self.assertEqual(len(twin.rules), 1)
        self.assertGreater(twin.rules[0].deadline_ts, deadline_before)

    def test_dormant_rule_skips_fire_but_rearms(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = False  # already off → off-rule dormant
        policy = sched.ScheduledPolicy(timer_seconds=1, once=False)
        twin.schedule("off", policy, 12)
        twin.tick_time(int(time.time()) + 5)
        self.assertEqual(len(twin.rules), 1)         # rearmed
        self.assertEqual(calls["published"], [])     # but did not fire

    def test_to_dict_includes_consumed_current_wh(self):
        # Snapshot enrichment: consumed rules get current_wh from
        # history.energy_consumed_in (the plug's authoritative
        # aenergy.total counter delta).
        from tests._fixtures import _FakeHistory
        h = _FakeHistory()
        h.consumed_wh = 1.667
        h.consumed_earliest_offset = 0  # earliest exactly at since
        twin, _, _ = _build_twin(history=h)
        policy = sched.ScheduledPolicy(consumed_field="apower",
                                       consumed_threshold_wh=10.0,
                                       consumed_window_s=600)
        twin.schedule("off", policy, 12)
        out = twin.to_dict()
        sched_jobs = out["scheduled_jobs"]
        self.assertEqual(len(sched_jobs), 1)
        consumed = sched_jobs[0]["consumed"]
        self.assertIn("current_wh", consumed)
        self.assertAlmostEqual(consumed["current_wh"], 1.667, places=2)

    def test_to_dict_omits_consumed_current_wh_when_history_partial(self):
        # If history doesn't extend back to the window start
        # (earliest_ts > since), don't show a current_wh — it would
        # be misleadingly small.
        from tests._fixtures import _FakeHistory
        h = _FakeHistory()
        h.consumed_wh = 0.0
        h.consumed_earliest_offset = None  # no data at all
        twin, _, _ = _build_twin(history=h)
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=10.0,
                                  consumed_window_s=600),
            12,
        )
        out = twin.to_dict()
        consumed = out["scheduled_jobs"][0]["consumed"]
        self.assertNotIn("current_wh", consumed)

    def test_consumed_current_window_minutes_grows_then_caps(self):
        # Fresh rule: actual elapsed grows from 0 toward the full window,
        # then caps once observation has been active for the full window.
        # The bot speaks minutes at the snapshot boundary.
        twin, _, _ = _build_twin()
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=10.0,
                                  consumed_window_s=600),
            12,
        )
        rule = twin.rules[0]
        now = int(time.time())
        # 1 minute into observation → current_window_minutes ≈ 1.
        rule._observation_started_at = now - 60
        out = twin.to_dict()
        wm = out["scheduled_jobs"][0]["consumed"]["current_window_minutes"]
        self.assertGreaterEqual(wm, 0.9)
        self.assertLessEqual(wm, 1.1)
        # 700 s in (past the 600 s window) → caps at 10 minutes.
        rule._observation_started_at = now - 700
        out = twin.to_dict()
        wm = out["scheduled_jobs"][0]["consumed"]["current_window_minutes"]
        self.assertEqual(wm, 10)

    def test_idle_current_window_minutes_grows_then_caps(self):
        # current_window_minutes is set regardless of whether history is
        # available — only current_max_w (the SQL-derived peak)
        # needs it.
        twin, _, _ = _build_twin()  # history=None
        twin.schedule(
            "off",
            sched.ScheduledPolicy(idle_field="apower",
                                  idle_threshold=5.0,
                                  idle_duration_s=1800),
            12,
        )
        rule = twin.rules[0]
        now = int(time.time())
        rule._observation_started_at = now - 120  # 2 min in
        out = twin.to_dict()
        wm = out["scheduled_jobs"][0]["idle"]["current_window_minutes"]
        self.assertGreaterEqual(wm, 1.9)
        self.assertLessEqual(wm, 2.1)
        rule._observation_started_at = now - 3600  # past the 1800 s window
        out = twin.to_dict()
        wm = out["scheduled_jobs"][0]["idle"]["current_window_minutes"]
        self.assertEqual(wm, 30)

    def test_observation_started_at_reset_on_manual_toggle(self):
        # Manual ON resets _observation_started_at on off-target rules.
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=10.0,
                                  consumed_window_s=600),
            12,
        )
        rule = twin.rules[0]
        long_ago = int(time.time()) - 7200
        rule._observation_started_at = long_ago
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 0.5}).encode())
        # After F→T edge, observation_started_at is bumped to ~now.
        self.assertGreater(rule._observation_started_at, long_ago)
        self.assertGreater(rule._observation_started_at,
                           int(time.time()) - 5)


# --- Consumed rule firing uses history.energy_consumed_in ----------------

class TestConsumedRuleUsesHistory(unittest.TestCase):
    """Verifies that consumed-rule evaluation reads from
    ``history.energy_consumed_in`` (the plug's authoritative
    aenergy.total counter) — not from any in-memory apower
    integration. Burst-mode loads (espresso, kettles) draw power in
    sub-second pulses that trapezoidal-on-apower-samples
    overestimates by 3–4×; the aenergy counter integrates at
    hardware rate and gets it right."""

    def _setup(self, consumed_wh, earliest_offset=0):
        from tests._fixtures import _FakeHistory
        h = _FakeHistory()
        h.consumed_wh = consumed_wh
        h.consumed_earliest_offset = earliest_offset
        twin, calls, _ = _build_twin(history=h)
        # Pre-seed output=True so the on_mqtt below is NOT a F→T edge
        # (which would otherwise reset the off-rule's warmup gate).
        twin.fields["output"] = True
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=150.0,
                                  consumed_window_s=3600),
            12,
        )
        rule = twin.rules[0]
        # Bypass the post-creation warmup gate so the rule can fire now.
        rule._consumed_started_at = int(time.time()) - 7200
        rule._observation_started_at = int(time.time()) - 7200
        return twin, rule, h, calls

    def test_fires_when_history_below_threshold(self):
        # Espresso burst pattern: hardware aenergy delta = 104 Wh in 1h
        # (well under the 150 Wh threshold) — fire.
        twin, _, _, calls = self._setup(consumed_wh=104.0)
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 3.0}).encode())
        # Off command published.
        published_payloads = [p for _t, p in calls["published"]]
        self.assertIn("off", published_payloads,
                      f"expected off command, got {calls['published']}")

    def test_does_not_fire_when_history_above_threshold(self):
        # 200 Wh > 150 Wh threshold — no fire.
        twin, _, _, calls = self._setup(consumed_wh=200.0)
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 3.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_with_partial_history(self):
        # earliest_offset > 0 means history doesn't extend back to
        # window start — refuse to fire (we'd be comparing a partial-
        # window delta against a full-window threshold).
        twin, _, _, calls = self._setup(consumed_wh=10.0, earliest_offset=600)
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 3.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_during_warmup(self):
        # Freshly-created rule: _consumed_started_at = now → must wait
        # one full window before being eligible to fire.
        twin, rule, _, calls = self._setup(consumed_wh=10.0)
        rule._consumed_started_at = int(time.time())  # just created
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 3.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_when_history_is_none(self):
        # Bot configured without history → consumed rules are inert
        # (can't ground-truth without the plug's counter).
        twin, calls, _ = _build_twin(history=None)
        twin.fields["output"] = True
        twin.schedule(
            "off",
            sched.ScheduledPolicy(consumed_field="apower",
                                  consumed_threshold_wh=150.0,
                                  consumed_window_s=3600),
            12,
        )
        rule = twin.rules[0]
        rule._consumed_started_at = int(time.time()) - 7200
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 3.0}).encode())
        self.assertEqual(calls["published"], [])


# --- Avg rule firing (mean apower over a window below threshold) ----------

class TestAvgRuleUsesHistory(unittest.TestCase):
    """Verifies that avg-rule evaluation reads from
    ``history.query_samples_raw`` and fires when the window MEAN is
    below threshold. Cycling-load case: bursts above threshold are
    expected and the average is what matters."""

    def _setup(self, sample_apowers, window_s=600, threshold_w=5.0,
               coverage="full"):
        """Build a twin whose history returns rows with the given
        apower values evenly spaced across the window. coverage="full"
        ⇒ first row at exactly `since`; "partial" ⇒ first row half a
        window in (rule must refuse to fire)."""
        from tests._fixtures import _FakeHistory
        now = int(time.time())
        since = now - window_s
        first = since if coverage == "full" else since + window_s // 2
        h = _FakeHistory()
        if sample_apowers:
            step = max(1, (now - first) // max(1, len(sample_apowers) - 1))
            h.samples_raw_rows = [
                (first + i * step, float(w), 230.0, 0.0, 50.0, 0.0, 1, 25.0)
                for i, w in enumerate(sample_apowers)
            ]
        twin, calls, _ = _build_twin(history=h)
        twin.fields["output"] = True  # not dormant; not a F→T edge
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=threshold_w,
                                  avg_window_s=window_s),
            12,
        )
        rule = twin.rules[0]
        # Bypass the post-creation warmup gate so the rule can fire now.
        rule._avg_started_at = now - window_s - 1
        rule._observation_started_at = now - window_s - 1
        return twin, rule, h, calls

    def test_fires_when_mean_below_threshold(self):
        # Cycling load: 1, 1, 1, 30 W samples → mean = 8.25 W > 5 W
        # but with a 4 W threshold (mean 8.25 ≥ 4) it should NOT fire.
        # Use 1, 1, 1, 1, 12 → mean = 3.2 W < 5 W threshold.
        twin, _, _, calls = self._setup([1.0, 1.0, 1.0, 1.0, 12.0])
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 12.0}).encode())
        published_payloads = [p for _t, p in calls["published"]]
        self.assertIn("off", published_payloads,
                      f"expected off command, got {calls['published']}")

    def test_does_not_fire_when_mean_at_or_above_threshold(self):
        # Mean = 6 W ≥ 5 W threshold → no fire (strict <).
        twin, _, _, calls = self._setup([6.0, 6.0, 6.0])
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 6.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_during_warmup(self):
        # Freshly-created rule: _avg_started_at = now → must wait one
        # full window before being eligible to fire.
        twin, rule, _, calls = self._setup([1.0, 1.0, 1.0])
        rule._avg_started_at = int(time.time())  # just created
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 1.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_with_partial_history(self):
        # earliest_ts > since → can't compare partial-window mean to
        # full-window threshold; refuse to fire.
        twin, _, _, calls = self._setup([1.0, 1.0, 1.0], coverage="partial")
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 1.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_does_not_fire_when_history_is_none(self):
        # Bot configured without history → avg rules are inert.
        twin, calls, _ = _build_twin(history=None)
        twin.fields["output"] = True
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=5.0,
                                  avg_window_s=600),
            12,
        )
        rule = twin.rules[0]
        rule._avg_started_at = int(time.time()) - 7200
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 1.0}).encode())
        self.assertEqual(calls["published"], [])

    def test_manual_on_resets_avg_window(self):
        # Manual F→T edge re-stamps _avg_started_at and
        # _observation_started_at — symmetric with consumed.
        twin, _, _ = _build_twin()
        twin.fields["output"] = False
        twin.schedule(
            "off",
            sched.ScheduledPolicy(avg_field="apower",
                                  avg_threshold_w=5.0,
                                  avg_window_s=600),
            12,
        )
        rule = twin.rules[0]
        long_ago = int(time.time()) - 7200
        rule._avg_started_at = long_ago
        rule._observation_started_at = long_ago
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "apower": 50.0}).encode())
        self.assertGreater(rule._avg_started_at, long_ago)
        self.assertGreater(rule._observation_started_at, long_ago)

    def test_to_dict_includes_avg_current_avg_w(self):
        # Snapshot enrichment: avg rule gets current_avg_w from
        # history.query_samples_raw over the live window.
        twin, _, _, _ = self._setup([1.0, 2.0, 3.0, 4.0])
        out = twin.to_dict()
        sched_jobs = out["scheduled_jobs"]
        self.assertEqual(len(sched_jobs), 1)
        avg = sched_jobs[0]["avg"]
        self.assertIn("current_avg_w", avg)
        self.assertAlmostEqual(avg["current_avg_w"], 2.5, places=2)
        # Window field is in minutes at the boundary.
        self.assertIn("window_minutes", avg)
        self.assertIn("current_window_minutes", avg)


# --- Resettable counter ---------------------------------------------------

class TestPlugTwinResetCounter(unittest.TestCase):
    def test_reset_snaps_baseline_and_signals(self):
        twin, calls, _ = _build_twin()
        twin.fields["aenergy"] = 12345.6
        twin.reset_counter()
        self.assertEqual(twin.baseline_wh, 12345.6)
        self.assertIsNotNone(twin.reset_at_ts)
        self.assertGreaterEqual(calls.get("baseline_saves", 0), 1)
        self.assertEqual(calls["broadcasts"][-1], "kitchen")

    def test_reset_with_no_aenergy_yet_uses_zero(self):
        twin, _, _ = _build_twin()
        # No aenergy field → baseline = 0; subsequent values track lifetime.
        twin.reset_counter()
        self.assertEqual(twin.baseline_wh, 0.0)


# --- Counter-reset detection (the plug's hardware aenergy.total going
# backwards records an offset event in history.aenergy_offset_events).

class TestPlugTwinCounterResetDetection(unittest.TestCase):
    def _build_twin_with_history(self):
        """Like _build_twin but with a real History wired in so we can
        observe record_offset_event writes via SQL."""
        twin, calls, cfg = _build_twin()
        tmpdir = Path(tempfile.mkdtemp())
        h = history_mod.History(tmpdir / "h.sqlite")
        twin.deps = plug_mod.TwinDeps(
            mqtt_publish=lambda t, p: calls["published"].append((t, p)),
            post_to_chats=lambda dev, txt: calls["posted"].append((dev.name, txt)),
            broadcast=lambda n=None: calls["broadcasts"].append(n),
            save_rules=lambda: None,
            save_baselines=lambda: calls.__setitem__(
                "baseline_saves", calls.get("baseline_saves", 0) + 1),
            react=lambda *a: None,
            history=h,
            client_id="test",
        )
        return twin, calls, h

    def test_drop_records_offset_event_and_alerts(self):
        twin, calls, h = self._build_twin_with_history()
        # First arrival: 1000 Wh — no prior reading → no reset.
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "aenergy": {"total": 1000.0}}).encode())
        self.assertEqual(twin.fields["aenergy"], 1000.0)
        self.assertEqual(twin.last_seen_aenergy_wh, 1000.0)
        with h._lock:
            cnt = h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events"
            ).fetchone()[0]
        self.assertEqual(cnt, 0)

        # Second arrival: 200 Wh — counter went backwards.
        twin.on_mqtt("status/switch:0",
                     json.dumps({"output": True, "aenergy": {"total": 200.0}}).encode())
        # last_seen tracks RAW; fields["aenergy"] stays RAW.
        self.assertEqual(twin.last_seen_aenergy_wh, 200.0)
        self.assertEqual(twin.fields["aenergy"], 200.0)
        # Offset event recorded.
        with h._lock:
            row = h._db.execute(
                "SELECT delta_wh FROM aenergy_offset_events WHERE device='kitchen'"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 800.0)   # 1000 - 200
        # Chat alert posted.
        self.assertTrue(any("hardware counter reset" in t
                             for _, t in calls["posted"]),
                        msg=f"posted={calls['posted']}")

    def test_multiple_resets_accumulate_in_offset_events(self):
        twin, _, h = self._build_twin_with_history()
        twin.on_mqtt("status/switch:0",
                     json.dumps({"aenergy": {"total": 500.0}}).encode())
        time.sleep(1.1)   # ensure distinct ts for INSERT OR IGNORE
        twin.on_mqtt("status/switch:0",
                     json.dumps({"aenergy": {"total": 100.0}}).encode())  # drop 400
        twin.on_mqtt("status/switch:0",
                     json.dumps({"aenergy": {"total": 250.0}}).encode())  # rises
        time.sleep(1.1)
        twin.on_mqtt("status/switch:0",
                     json.dumps({"aenergy": {"total": 50.0}}).encode())   # drops 200
        with h._lock:
            rows = list(h._db.execute(
                "SELECT delta_wh FROM aenergy_offset_events "
                "WHERE device='kitchen' ORDER BY ts ASC"
            ))
        self.assertEqual([r[0] for r in rows], [400.0, 200.0])

    def test_no_offset_when_counter_only_grows(self):
        twin, _, h = self._build_twin_with_history()
        for total in (100.0, 200.0, 350.0, 350.0, 1000.0):
            twin.on_mqtt("status/switch:0",
                         json.dumps({"aenergy": {"total": total}}).encode())
        with h._lock:
            cnt = h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events"
            ).fetchone()[0]
        self.assertEqual(cnt, 0)
        self.assertEqual(twin.fields["aenergy"], 1000.0)


if __name__ == "__main__":
    unittest.main()
