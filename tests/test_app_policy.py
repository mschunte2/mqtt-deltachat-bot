"""The webxdc app's policy payload is untrusted input.

It comes from whatever .xdc build is installed in the chat, which may
be months old or simply buggy. Two classes of defect existed:

- Callers caught only ValueError, but the coercions raised TypeError,
  so a malformed request escaped into the RawEvent hook uncontained.
- Only timer_minutes was range-checked, so a negative
  idle.duration_minutes collapsed a 30-minute safety window to one
  status update — and the chat ack rendered "for 0s" because
  durations.format clamps negatives, so nothing looked wrong.
"""

import unittest

from mqtt_bot import app_policy
from mqtt_bot.core import rules as sched


DEFAULTS = sched.PolicyDefaults(
    idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
    consumed_field="apower", consumed_threshold_wh=5.0,
    consumed_window_s=600,
    avg_field="apower", avg_threshold_w=5.0, avg_window_s=600,
)


def build(raw):
    return app_policy.build(raw, DEFAULTS)


class TestTypeConfusionRaisesValueError(unittest.TestCase):
    """Each of these used to raise TypeError, which the callers did not
    catch — the exception escaped into the Delta Chat event hook."""

    def test_null_optional_field_falls_back_to_the_class_default(self):
        """`float(None)` used to raise TypeError here. An explicit null
        is treated the same as an absent key — a client that serialises
        an unset field as null gets the device class's default, which
        is exactly what omitting it would have given."""
        p = build({"idle": {"threshold": None, "duration_minutes": 5}})
        self.assertEqual(p.idle_threshold, DEFAULTS.idle_threshold)
        self.assertEqual(p.idle_duration_s, 300)

    def test_null_in_a_required_position_still_errors(self):
        with self.assertRaises(ValueError):
            build({"time_of_day": [None, 30]})

    def test_dict_in_time_of_day(self):
        with self.assertRaises(ValueError):
            build({"time_of_day": [{}, 0]})

    def test_list_consumed_threshold(self):
        with self.assertRaises(ValueError):
            build({"consumed": {"threshold_wh": [1]}})

    def test_string_timer(self):
        with self.assertRaises(ValueError):
            build({"timer_minutes": "30"})

    def test_non_dict_policy(self):
        for value in ("x", 5, [], None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    app_policy.build(value, DEFAULTS)

    def test_non_dict_subobject(self):
        for key in ("idle", "consumed", "avg"):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    build({key: "yes"})

    def test_bool_is_not_a_number(self):
        """isinstance(True, int) is True in Python; a client sending
        `true` for a threshold means a bug, not the value 1."""
        with self.assertRaises(ValueError):
            build({"timer_minutes": True})

    def test_nan_and_infinity_rejected(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    build({"timer_minutes": value})

    def test_never_raises_anything_but_value_error(self):
        payloads = [
            {"idle": {"threshold": None}},
            {"time_of_day": [{}, 0]},
            {"consumed": {"threshold_wh": [1]}},
            {"avg": {"window_minutes": {"a": 1}}},
            {"time_of_day": "18:30"},
            {"idle": {"field": {"x": 1}}},
            {},
        ]
        for raw in payloads:
            with self.subTest(raw=raw):
                try:
                    build(raw)
                except ValueError:
                    pass
                except Exception as ex:      # noqa: BLE001 - that's the point
                    self.fail(f"{raw} raised {type(ex).__name__}: {ex}")


class TestBoundsAreEnforced(unittest.TestCase):
    def test_negative_idle_duration_rejected(self):
        """-5 minutes gave idle_duration_s = -300, and
        `now - _below_since >= -300` is true on the second sample below
        threshold: a 30-minute safety window became ~one update."""
        with self.assertRaises(ValueError):
            build({"idle": {"duration_minutes": -5}})

    def test_negative_windows_rejected(self):
        for key, sub in (("consumed", "window_minutes"),
                         ("avg", "window_minutes")):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    build({key: {sub: -5}})

    def test_zero_duration_rejected(self):
        with self.assertRaises(ValueError):
            build({"idle": {"duration_minutes": 0}})

    def test_absurd_duration_rejected(self):
        with self.assertRaises(ValueError):
            build({"timer_minutes": 10 ** 12})

    def test_negative_threshold_rejected(self):
        with self.assertRaises(ValueError):
            build({"idle": {"threshold": -1}})

    def test_time_of_day_out_of_range_rejected(self):
        for tod in ([24, 0], [-1, 0], [12, 60], [12, -1]):
            with self.subTest(tod=tod):
                with self.assertRaises(ValueError):
                    build({"time_of_day": tod})

    def test_time_of_day_wrong_length_rejected(self):
        for tod in ([12], [12, 30, 0], []):
            with self.subTest(tod=tod):
                with self.assertRaises(ValueError):
                    build({"time_of_day": tod})

    def test_empty_policy_rejected(self):
        with self.assertRaises(ValueError):
            build({})


class TestValidPayloadsStillWork(unittest.TestCase):
    def test_timer(self):
        self.assertEqual(build({"timer_minutes": 30}).timer_seconds, 1800)

    def test_fractional_timer_rounds(self):
        self.assertEqual(build({"timer_minutes": 0.5}).timer_seconds, 30)

    def test_time_of_day(self):
        p = build({"time_of_day": [18, 30], "recurring_tod": True})
        self.assertEqual(p.time_of_day, (18, 30))
        self.assertTrue(p.recurring_tod)

    def test_idle_uses_class_defaults_when_omitted(self):
        p = build({"idle": {}})
        self.assertEqual(p.idle_field, "apower")
        self.assertEqual(p.idle_threshold, 5.0)
        self.assertEqual(p.idle_duration_s, 60)

    def test_idle_explicit_values(self):
        p = build({"idle": {"threshold": 10, "duration_minutes": 2}})
        self.assertEqual(p.idle_threshold, 10.0)
        self.assertEqual(p.idle_duration_s, 120)

    def test_consumed(self):
        p = build({"consumed": {"threshold_wh": 5, "window_minutes": 10}})
        self.assertEqual(p.consumed_threshold_wh, 5.0)
        self.assertEqual(p.consumed_window_s, 600)

    def test_avg(self):
        p = build({"avg": {"threshold_w": 20, "window_minutes": 15}})
        self.assertEqual(p.avg_threshold_w, 20.0)
        self.assertEqual(p.avg_window_s, 900)

    def test_once_flag(self):
        self.assertTrue(build({"timer_minutes": 5, "once": True}).once)
        self.assertFalse(build({"timer_minutes": 5}).once)

    def test_or_combined_policies(self):
        p = build({"timer_minutes": 60, "idle": {"duration_minutes": 5}})
        self.assertEqual(p.timer_seconds, 3600)
        self.assertEqual(p.idle_duration_s, 300)

    def test_zero_threshold_is_allowed(self):
        """0 W is a meaningful idle threshold; only negatives are wrong."""
        self.assertEqual(build({"idle": {"threshold": 0}}).idle_threshold, 0.0)


if __name__ == "__main__":
    unittest.main()
