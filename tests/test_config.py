"""Tests for the config loader (devices.json + class.json) and the
small parse_allowed_chats helper."""

import json
import tempfile
import unittest
from pathlib import Path

import config as cfg_mod


# Smallest class.json that satisfies the loader's required keys.
# Distinct from tests/_fixtures.CLASS_JSON_OK (which is the engine
# fixture used by _build_twin); this one exists to exercise the
# loader's error paths directly.
CLASS_JSON_OK = {
    "name": "tplug",
    "app_id": "tplug",
    "subscribe": [
        {"suffix": "online", "format": "text"},
        {"suffix": "status/switch:0", "format": "json"},
    ],
    "commands": {
        "on":     {"suffix": "command/switch:0", "payload": "on"},
        "off":    {"suffix": "command/switch:0", "payload": "off"},
        "toggle": {"suffix": "command/switch:0", "payload": "toggle"},
        "status": {"suffix": "rpc",
                   "payload": '{"id":1,"src":"{client_id}","method":"Switch.GetStatus","params":{"id":0}}'},
    },
    "state_fields": {
        "online": {"from_suffix": "online", "extract": "bool_text"},
        "output": {"from_suffix": "status/switch:0", "json_path": "output"},
        "apower": {"from_suffix": "status/switch:0", "json_path": "apower"},
    },
    "chat_events": [
        {"type": "on_change", "field": "output",
         "values": {"true": "{name} ON", "false": "{name} OFF"}},
        {"type": "threshold", "field": "apower",
         "limit_param":    "power_threshold_watts",
         "duration_param": "power_threshold_duration_s",
         "above": "drew {value:.0f}W for {seconds}s",
         "below": "load cleared"},
    ],
    "auto_off": {
        "command": "off",
        "default_idle_field": "apower",
        "default_idle_threshold": 5,
        "default_idle_duration": 60,
        "default_consumed_field": "apower",
        "default_consumed_threshold_wh": 5,
        "default_consumed_window_s": 600,
        "trigger_messages": {
            "timer":            "{name} auto-off (timer)",
            "tod":              "{name} auto-off ({hh}:{mm})",
            "idle":             "{name} auto-off (idle: {field}={value:.1f}W for {seconds}s)",
            "consumed":         "{name} auto-off (used {value:.2f}Wh)",
        },
    },
}


class TestConfigLoad(unittest.TestCase):
    def _setup(self, with_class=True, instance_overrides=None):
        tmp = Path(tempfile.mkdtemp())
        if with_class:
            cls_dir = tmp / "devices" / "tplug"
            cls_dir.mkdir(parents=True)
            (cls_dir / "class.json").write_text(json.dumps(CLASS_JSON_OK))
        else:
            (tmp / "devices").mkdir()
        instance = {
            "devices": [{
                "name": "kitchen",
                "class": "tplug",
                "topic_prefix": "shellyplug-aaa",
                "allowed_chats": [12],
            }],
        }
        if instance_overrides:
            instance.update(instance_overrides)
        (tmp / "devices.json").write_text(json.dumps(instance))
        return tmp

    def test_happy(self):
        tmp = self._setup()
        c = cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")
        self.assertIn("tplug", c.classes)
        self.assertIn("kitchen", c.devices)
        d = c.devices["kitchen"]
        self.assertEqual(d.topic_prefix, "shellyplug-aaa")
        self.assertEqual(d.allowed_chats, (12,))

    def test_no_classes(self):
        tmp = self._setup(with_class=False)
        with self.assertRaises(cfg_mod.ConfigError):
            cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")

    def test_bad_class_ref(self):
        tmp = self._setup(instance_overrides={
            "devices": [{
                "name": "kitchen", "class": "nope",
                "topic_prefix": "p", "allowed_chats": [],
            }]
        })
        with self.assertRaises(cfg_mod.ConfigError):
            cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")

    def test_duplicate_prefix(self):
        tmp = self._setup(instance_overrides={
            "devices": [
                {"name": "a", "class": "tplug", "topic_prefix": "x", "allowed_chats": []},
                {"name": "b", "class": "tplug", "topic_prefix": "x", "allowed_chats": []},
            ]
        })
        with self.assertRaises(cfg_mod.ConfigError):
            cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")

    def test_bad_device_name(self):
        tmp = self._setup(instance_overrides={
            "devices": [{
                "name": "Kitchen",  # uppercase rejected
                "class": "tplug", "topic_prefix": "x", "allowed_chats": [],
            }]
        })
        with self.assertRaises(cfg_mod.ConfigError):
            cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")


class TestParseAllowedChats(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(cfg_mod.parse_allowed_chats(""), set())
        self.assertEqual(cfg_mod.parse_allowed_chats(None), set())

    def test_simple(self):
        self.assertEqual(cfg_mod.parse_allowed_chats("12,34"), {12, 34})

    def test_whitespace(self):
        self.assertEqual(cfg_mod.parse_allowed_chats(" 12 , 34 ,, "), {12, 34})


if __name__ == "__main__":
    unittest.main()
