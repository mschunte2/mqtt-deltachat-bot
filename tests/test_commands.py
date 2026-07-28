"""Tests for the command parser and the replay-freshness gate.

commands.py had no test file at all, despite owning the three replay
windows and the text-command parser — the security boundary of a bot
that switches mains voltage.

The freshness tests below cover the gap that motivated extracting
check_freshness out of bot.py: the webxdc path only checked the age
when `ts` happened to be a number, so a request with no `ts`, or with
`ts` as a string/null/bool, executed with no freshness bound and no log
line. SECURITY.md and CLAUDE.md both stated the opposite.
"""

import unittest

from mqtt_bot import commands


NOW = 1_714_000_000


class TestFreshnessRejectsBadTimestamps(unittest.TestCase):
    """Each of these bypassed replay protection entirely before."""

    def test_missing_ts_is_rejected(self):
        ok, why = commands.check_freshness(None, NOW,
                                           commands.MAX_APP_AGE_SECONDS)
        self.assertFalse(ok)
        self.assertIn("ts", why)

    def test_string_ts_is_rejected(self):
        ok, _ = commands.check_freshness(str(NOW), NOW,
                                         commands.MAX_APP_AGE_SECONDS)
        self.assertFalse(ok)

    def test_bool_ts_is_rejected(self):
        """isinstance(True, int) is True in Python, so a bare `true`
        would read as ts=1 without the explicit bool check."""
        for value in (True, False):
            with self.subTest(value=value):
                ok, _ = commands.check_freshness(
                    value, NOW, commands.MAX_APP_AGE_SECONDS)
                self.assertFalse(ok)

    def test_list_and_dict_are_rejected(self):
        for value in ([NOW], {"ts": NOW}, ()):
            with self.subTest(value=value):
                ok, _ = commands.check_freshness(
                    value, NOW, commands.MAX_APP_AGE_SECONDS)
                self.assertFalse(ok)

    def test_rejection_always_explains_itself(self):
        """Silent drops are what made this invisible; every reject must
        carry a reason the operator can grep for."""
        for value in (None, "x", True, [], {}):
            ok, why = commands.check_freshness(
                value, NOW, commands.MAX_APP_AGE_SECONDS)
            self.assertFalse(ok)
            self.assertTrue(why.strip(), f"no reason given for {value!r}")


class TestFreshnessWindow(unittest.TestCase):
    def test_current_timestamp_passes(self):
        ok, why = commands.check_freshness(NOW, NOW,
                                           commands.MAX_APP_AGE_SECONDS)
        self.assertTrue(ok, why)
        self.assertEqual(why, "")

    def test_float_timestamp_passes(self):
        ok, _ = commands.check_freshness(float(NOW), NOW,
                                         commands.MAX_APP_AGE_SECONDS)
        self.assertTrue(ok)

    def test_just_inside_the_window_passes(self):
        ok, _ = commands.check_freshness(
            NOW - commands.MAX_APP_AGE_SECONDS, NOW,
            commands.MAX_APP_AGE_SECONDS)
        self.assertTrue(ok)

    def test_just_outside_the_window_is_rejected(self):
        ok, why = commands.check_freshness(
            NOW - commands.MAX_APP_AGE_SECONDS - 1, NOW,
            commands.MAX_APP_AGE_SECONDS)
        self.assertFalse(ok)
        self.assertIn("stale", why)

    def test_days_old_request_is_rejected(self):
        """The replay this exists to stop: a stale app instance, or a
        status update redelivered after a long offline period."""
        ok, _ = commands.check_freshness(NOW - 3 * 86400, NOW,
                                         commands.MAX_APP_AGE_SECONDS)
        self.assertFalse(ok)

    def test_small_future_skew_is_tolerated(self):
        ok, _ = commands.check_freshness(
            NOW + commands.MAX_CLOCK_SKEW_SECONDS, NOW,
            commands.MAX_APP_AGE_SECONDS)
        self.assertTrue(ok)

    def test_large_future_date_is_rejected(self):
        ok, why = commands.check_freshness(
            NOW + commands.MAX_CLOCK_SKEW_SECONDS + 1, NOW,
            commands.MAX_APP_AGE_SECONDS)
        self.assertFalse(ok)
        self.assertIn("future", why)

    def test_text_window_is_wider_than_the_app_window(self):
        """Typed commands absorb a broker reconnect; app taps shouldn't."""
        self.assertGreater(commands.MAX_AGE_SECONDS,
                           commands.MAX_APP_AGE_SECONDS)
        age = commands.MAX_APP_AGE_SECONDS + 10
        self.assertFalse(commands.check_freshness(
            NOW - age, NOW, commands.MAX_APP_AGE_SECONDS)[0])
        self.assertTrue(commands.check_freshness(
            NOW - age, NOW, commands.MAX_AGE_SECONDS)[0])


class TestParseTextCommand(unittest.TestCase):
    def test_global_verb(self):
        self.assertEqual(commands.parse_text_command("/list"), ("", "list", ""))

    def test_device_verb(self):
        self.assertEqual(commands.parse_text_command("/kaffeete on"),
                         ("kaffeete", "on", ""))

    def test_device_verb_with_rest(self):
        dev, verb, rest = commands.parse_text_command("/kaffeete on for 30m")
        self.assertEqual((dev, verb), ("kaffeete", "on"))
        self.assertIn("30m", rest)

    def test_non_command_returns_none(self):
        for text in ("hello", "", "  ", "not/a/command"):
            with self.subTest(text=text):
                self.assertIsNone(commands.parse_text_command(text))


class TestSanitize(unittest.TestCase):
    def test_strips_control_characters(self):
        self.assertNotIn("\x00", commands.sanitize("ab\x00c"))
        self.assertNotIn("\n", commands.sanitize("a\nb"))

    def test_caps_length(self):
        self.assertLessEqual(len(commands.sanitize("x" * 500, max_len=64)), 64)

    def test_empty_falls_back(self):
        self.assertEqual(commands.sanitize("", fallback="?"), "?")
        self.assertEqual(commands.sanitize(None, fallback="?"), "?")


if __name__ == "__main__":
    unittest.main()
