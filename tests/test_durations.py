"""Tests for the durations parser."""

import unittest

import durations


class TestDurations(unittest.TestCase):
    def test_simple(self):
        self.assertEqual(durations.parse("30s"), 30)
        self.assertEqual(durations.parse("5m"), 300)
        self.assertEqual(durations.parse("1h"), 3600)
        self.assertEqual(durations.parse("1h30m"), 5400)
        self.assertEqual(durations.parse("2h15m30s"), 8130)
        self.assertEqual(durations.parse("90s"), 90)

    def test_case_and_whitespace(self):
        self.assertEqual(durations.parse("1H30M"), 5400)
        self.assertEqual(durations.parse(" 30s "), 30)

    def test_invalid(self):
        for bad in ["", "30", "tomorrow", "30x", "0s", "0m"]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    durations.parse(bad)

    def test_days(self):
        # `d` is accepted (added so /<dev> export 7d works)
        self.assertEqual(durations.parse("1d"), 86400)
        self.assertEqual(durations.parse("7d"), 7 * 86400)
        self.assertEqual(durations.parse("1d12h"), 86400 + 12 * 3600)

    def test_verbose_units(self):
        # User wrote "30min" in chat; previously rejected — now accepted.
        self.assertEqual(durations.parse("30min"), 1800)
        self.assertEqual(durations.parse("30mins"), 1800)
        self.assertEqual(durations.parse("30minute"), 1800)
        self.assertEqual(durations.parse("30minutes"), 1800)
        self.assertEqual(durations.parse("2hours"), 7200)
        self.assertEqual(durations.parse("2hr"), 7200)
        self.assertEqual(durations.parse("2hrs"), 7200)
        self.assertEqual(durations.parse("1day"), 86400)
        self.assertEqual(durations.parse("3days"), 3 * 86400)
        self.assertEqual(durations.parse("45sec"), 45)
        self.assertEqual(durations.parse("1hr30min"), 3600 + 1800)

    def test_format(self):
        self.assertEqual(durations.format(0), "0s")
        self.assertEqual(durations.format(30), "30s")
        self.assertEqual(durations.format(60), "1m")
        self.assertEqual(durations.format(90), "1m30s")
        self.assertEqual(durations.format(3600), "1h")
        self.assertEqual(durations.format(3661), "1h1m1s")


if __name__ == "__main__":
    unittest.main()
