"""Tests for the state-extraction pure function."""

import json
import unittest

import config as cfg_mod
import state as state_mod


def _make_class():
    return cfg_mod.DeviceClass(
        name="t", app_id="t", description="",
        subscribe=(),
        commands={},
        state_fields={
            "online":  cfg_mod.StateFieldDef(from_suffix="online",
                                             extract="bool_text"),
            "output":  cfg_mod.StateFieldDef(from_suffix="status/switch:0",
                                             json_path="output"),
            "apower":  cfg_mod.StateFieldDef(from_suffix="status/switch:0",
                                             json_path="apower"),
            "aenergy": cfg_mod.StateFieldDef(from_suffix="status/switch:0",
                                             json_path="aenergy.total"),
        },
        chat_events=(),
        auto_off=None, auto_on=None,
    )


class TestStateExtract(unittest.TestCase):
    def test_bool_text(self):
        cls = _make_class()
        self.assertEqual(state_mod.extract(cls, "online", b"true"), {"online": True})
        self.assertEqual(state_mod.extract(cls, "online", b"false"), {"online": False})

    def test_bool_text_unknown_skipped(self):
        cls = _make_class()
        self.assertEqual(state_mod.extract(cls, "online", b"maybe"), {})

    def test_json_path(self):
        cls = _make_class()
        payload = json.dumps({"output": True, "apower": 12.5,
                              "aenergy": {"total": 100.4}})
        out = state_mod.extract(cls, "status/switch:0", payload)
        self.assertEqual(out, {"output": True, "apower": 12.5, "aenergy": 100.4})

    def test_unrelated_suffix_returns_empty(self):
        cls = _make_class()
        self.assertEqual(state_mod.extract(cls, "events/rpc", b"{}"), {})

    def test_malformed_json_skipped(self):
        cls = _make_class()
        self.assertEqual(state_mod.extract(cls, "status/switch:0", b"not json"), {})


if __name__ == "__main__":
    unittest.main()
