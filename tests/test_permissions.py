"""Tests for the per-chat / per-device permission gate."""

import unittest

import config as cfg_mod
import permissions


def _device(name, allowed):
    return cfg_mod.Device(
        name=name, class_name="x", topic_prefix="p/" + name,
        description="", allowed_chats=tuple(allowed), params={},
    )


class TestPermissions(unittest.TestCase):
    def test_global_gate(self):
        self.assertTrue(permissions.is_allowed(7, {7, 8}))
        self.assertFalse(permissions.is_allowed(9, {7, 8}))

    def test_per_device_overrides_global(self):
        d = _device("kitchen", [11])
        # chat 11 in device list — allowed even if not in global
        self.assertTrue(permissions.chat_can_see(11, d, set()))
        # chat 7 in global but NOT in device list — denied
        self.assertFalse(permissions.chat_can_see(7, d, {7, 11}))

    def test_empty_device_list_falls_back(self):
        d = _device("kitchen", [])
        self.assertTrue(permissions.chat_can_see(7, d, {7, 8}))
        self.assertFalse(permissions.chat_can_see(9, d, {7, 8}))


if __name__ == "__main__":
    unittest.main()
