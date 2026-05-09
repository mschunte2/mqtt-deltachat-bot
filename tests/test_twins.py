"""Tests for the TwinRegistry: lookup by topic, visibility filter."""

import unittest

from twins import TwinRegistry

from tests._fixtures import _build_twin


class TestTwinRegistry(unittest.TestCase):
    def test_find_by_topic(self):
        twin, _, _ = _build_twin()
        registry = TwinRegistry([twin])
        found = registry.find_by_topic("p/kitchen/status/switch:0")
        self.assertIsNotNone(found)
        self.assertIs(found[0], twin)
        self.assertEqual(found[1], "status/switch:0")

    def test_find_unknown_topic(self):
        twin, _, _ = _build_twin()
        registry = TwinRegistry([twin])
        self.assertIsNone(registry.find_by_topic("nope/whatever"))

    def test_visible_to_filters(self):
        twin, _, _ = _build_twin(allowed_chats=(12,))
        registry = TwinRegistry([twin])
        self.assertEqual(len(registry.visible_to(12, set())), 1)
        self.assertEqual(len(registry.visible_to(99, set())), 0)


if __name__ == "__main__":
    unittest.main()
