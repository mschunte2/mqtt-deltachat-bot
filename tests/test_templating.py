"""Tests for the templating regex substitution."""

import unittest

from mqtt_bot.util import templating


class TestTemplating(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(templating.render("hello {name}", {"name": "world"}), "hello world")

    def test_missing_keys_empty(self):
        self.assertEqual(templating.render("a={a} b={b}", {"a": 1}), "a=1 b=")

    def test_format_spec(self):
        self.assertEqual(templating.render("{v:.2f}", {"v": 1.5}), "1.50")


if __name__ == "__main__":
    unittest.main()
