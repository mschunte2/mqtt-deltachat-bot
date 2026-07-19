"""Tests for dc_config — Delta Chat account retention config.

Pure helper + a thin set_config wrapper, kept out of bot.py (which has
construction side effects on import) so the days->seconds mapping and
the "0 disables" sentinel are unit-testable against a fake rpc.
"""

import unittest

from mqtt_bot.io import dc_config


class _FakeRpc:
    def __init__(self):
        self.calls = []

    def set_config(self, accid, key, value):
        self.calls.append((accid, key, value))


class TestDeleteDeviceAfterSeconds(unittest.TestCase):
    def test_maps_days_to_seconds(self):
        self.assertEqual(dc_config.delete_device_after_seconds(14), 1209600)

    def test_zero_stays_zero(self):
        self.assertEqual(dc_config.delete_device_after_seconds(0), 0)

    def test_negative_clamps_to_zero(self):
        # A misconfigured negative day count must never produce a negative
        # (nonsense) seconds value — treat it as "never".
        self.assertEqual(dc_config.delete_device_after_seconds(-3), 0)


class TestApplyRetention(unittest.TestCase):
    def test_sets_config_in_seconds(self):
        rpc = _FakeRpc()
        dc_config.apply_retention(rpc, 1, 14)
        self.assertEqual(rpc.calls, [(1, "delete_device_after", "1209600")])

    def test_zero_disables(self):
        rpc = _FakeRpc()
        dc_config.apply_retention(rpc, 7, 0)
        self.assertEqual(rpc.calls, [(7, "delete_device_after", "0")])


class TestEnsureBotMode(unittest.TestCase):
    def test_sets_bot_1(self):
        rpc = _FakeRpc()
        dc_config.ensure_bot_mode(rpc, 3)
        self.assertEqual(rpc.calls, [(3, "bot", "1")])


if __name__ == "__main__":
    unittest.main()
