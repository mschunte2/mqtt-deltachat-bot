"""Unit tests for the pure modules.

Run with: python3 test_mqtt_bot.py     (or `python3 -m unittest`)
No external deps required — modules under test are stdlib-only.
"""

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import config as cfg_mod
import durations
import permissions
import rules as sched
import state as state_mod
import templating


# --- durations ------------------------------------------------------------

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


# --- templating -----------------------------------------------------------

class TestTemplating(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(templating.render("hello {name}", {"name": "world"}), "hello world")

    def test_missing_keys_empty(self):
        self.assertEqual(templating.render("a={a} b={b}", {"a": 1}), "a=1 b=")

    def test_format_spec(self):
        self.assertEqual(templating.render("{v:.2f}", {"v": 1.5}), "1.50")


# --- permissions ----------------------------------------------------------

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


# --- state extraction ----------------------------------------------------

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


# --- scheduler.parse_policy ----------------------------------------------

class TestParsePolicy(unittest.TestCase):
    def setUp(self):
        self.d = sched.PolicyDefaults(
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
            consumed_field="apower", consumed_threshold_wh=5.0, consumed_window_s=600,
        )

    def test_timer(self):
        p = sched.parse_policy("for 30m", self.d)
        self.assertEqual(p.timer_seconds, 1800)
        self.assertIsNone(p.time_of_day)

    def test_in_alias(self):
        self.assertEqual(sched.parse_policy("in 30m", self.d).timer_seconds, 1800)

    def test_tod(self):
        p = sched.parse_policy("at 18h", self.d)
        self.assertEqual(p.time_of_day, (18, 0))
        self.assertFalse(p.recurring_tod)
        p = sched.parse_policy("at 18:30", self.d)
        self.assertEqual(p.time_of_day, (18, 30))

    def test_tod_daily(self):
        p = sched.parse_policy("at 7h daily", self.d)
        self.assertEqual(p.time_of_day, (7, 0))
        self.assertTrue(p.recurring_tod)

    def test_tod_invalid(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("at 25h", self.d)

    def test_idle_defaults_if(self):
        p = sched.parse_policy("if idle", self.d)
        self.assertEqual(p.idle_threshold, 5.0)
        self.assertEqual(p.idle_duration_s, 60)

    def test_idle_defaults_until_synonym(self):
        # `until` is a synonym for `if` (preserved for older syntax)
        p = sched.parse_policy("until idle", self.d)
        self.assertEqual(p.idle_threshold, 5.0)

    def test_idle_power_overrides(self):
        p = sched.parse_policy("if idle 10W 120s", self.d)
        self.assertEqual(p.idle_threshold, 10.0)
        self.assertEqual(p.idle_duration_s, 120)
        # No consumed policy on a W-unit clause.
        self.assertIsNone(p.consumed_field)

    def test_idle_energy_via_wh(self):
        # Wh unit on the value -> rolling-window energy policy ("consumed")
        p = sched.parse_policy("if idle 10Wh in 2m", self.d)
        self.assertEqual(p.consumed_threshold_wh, 10.0)
        self.assertEqual(p.consumed_window_s, 120)
        # No idle-power policy on a Wh clause.
        self.assertIsNone(p.idle_field)

    def test_idle_energy_via_kwh(self):
        p = sched.parse_policy("if idle 1.5kWh in 1h", self.d)
        self.assertEqual(p.consumed_threshold_wh, 1500.0)
        self.assertEqual(p.consumed_window_s, 3600)

    def test_idle_in_optional_for_power(self):
        # `in` keyword is accepted on either form
        p = sched.parse_policy("if idle 10W in 120s", self.d)
        self.assertEqual(p.idle_threshold, 10.0)
        self.assertEqual(p.idle_duration_s, 120)

    def test_idle_with_spaces_in_value_and_duration(self):
        # User reported "200Wh in 30min" works but "200 Wh in 30 min" failed.
        # Both should now parse identically.
        a = sched.parse_policy("if idle 200Wh in 30min", self.d)
        b = sched.parse_policy("if idle 200 Wh in 30 min", self.d)
        self.assertEqual(a.consumed_threshold_wh, 200.0)
        self.assertEqual(a.consumed_window_s, 1800)
        self.assertEqual(b.consumed_threshold_wh, 200.0)
        self.assertEqual(b.consumed_window_s, 1800)

    def test_idle_power_with_spaces(self):
        a = sched.parse_policy("if idle 5W 60s", self.d)
        b = sched.parse_policy("if idle 5 W 60 s", self.d)
        c = sched.parse_policy("if idle 5 W in 60 sec", self.d)
        for p in (a, b, c):
            self.assertEqual(p.idle_threshold, 5.0)
            self.assertEqual(p.idle_duration_s, 60)

    def test_timer_with_spaces(self):
        # "for 30 min" used to fail because (\S+) captured only "30".
        self.assertEqual(sched.parse_policy("for 30 min", self.d).timer_seconds, 1800)
        self.assertEqual(sched.parse_policy("in 1 hour", self.d).timer_seconds, 3600)
        self.assertEqual(sched.parse_policy("for 1 hr 30 min", self.d).timer_seconds, 5400)

    def test_idle_unknown_unit_rejected(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle 10J 60s", self.d)

    def test_or_combination(self):
        p = sched.parse_policy("for 1h or if idle", self.d)
        self.assertEqual(p.timer_seconds, 3600)
        self.assertIsNotNone(p.idle_field)

    def test_two_time_policies_rejected(self):
        with self.assertRaises(ValueError):
            sched.parse_policy("for 1h or at 18h", self.d)

    def test_restricted_kinds(self):
        # auto-on only allows timer + tod
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle", self.d, allowed=frozenset({"timer", "tod"}))
        # consumed (Wh form) also rejected for auto-on
        with self.assertRaises(ValueError):
            sched.parse_policy("if idle 5Wh in 1m", self.d,
                               allowed=frozenset({"timer", "tod"}))
        # but tod alone is fine
        p = sched.parse_policy("at 7h", self.d, allowed=frozenset({"timer", "tod"}))
        self.assertEqual(p.time_of_day, (7, 0))


# --- scheduler.integrate_wh ----------------------------------------------

class TestIntegrate(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(sched.integrate_wh([], 0, 60), 0.0)

    def test_constant_one_sample(self):
        # 100W constant for 60s = 100 * 60 / 3600 ≈ 1.667 Wh
        wh = sched.integrate_wh([(0, 100.0)], 0, 60)
        self.assertAlmostEqual(wh, 100 * 60 / 3600.0, places=4)

    def test_two_samples(self):
        # samples at t=0 and t=60, power 100→100, then extends to t=120 at 100W
        wh = sched.integrate_wh([(0, 100.0), (60, 100.0)], 0, 120)
        self.assertAlmostEqual(wh, 100 * 120 / 3600.0, places=4)

    def test_window_trim(self):
        # window starts at 30, sample at 0 should be ignored
        wh = sched.integrate_wh([(0, 1000.0), (60, 100.0)], 30, 60)
        # only (60, 100.0) inside window → constant 100W * 30s = ~0.833Wh
        self.assertAlmostEqual(wh, 100 * 30 / 3600.0, places=4)


# --- scheduler.next_tod_deadline ------------------------------------------

class TestTodDeadline(unittest.TestCase):
    def test_future_today(self):
        # pick a target far enough in the future to not be ambiguous
        now = int(time.time())
        lt = time.localtime(now)
        h = (lt.tm_hour + 2) % 24
        target = sched.next_tod_deadline(h, 0, now)
        self.assertGreater(target, now)
        # within 25 hours
        self.assertLess(target - now, 25 * 3600)

    def test_past_rolls_to_tomorrow(self):
        now = int(time.time())
        lt = time.localtime(now)
        h_past = (lt.tm_hour - 1) % 24
        target = sched.next_tod_deadline(h_past, lt.tm_min, now)
        # must be in the future
        self.assertGreater(target, now)
        # within ~25 hours
        self.assertLess(target - now, 25 * 3600)


# --- config loader -------------------------------------------------------

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
            "cancelled_manual": "{name} auto-off cancelled (manually toggled)",
        },
    },
}


class TestConfigLoad(unittest.TestCase):
    def _setup(self, with_class=True, instance_overrides=None,
               class_overrides=None):
        tmp = Path(tempfile.mkdtemp())
        if with_class:
            cls_dir = tmp / "devices" / "tplug"
            cls_dir.mkdir(parents=True)
            cls_def = json.loads(json.dumps(CLASS_JSON_OK))   # deep copy
            if class_overrides:
                cls_def.update(class_overrides)
            (cls_dir / "class.json").write_text(json.dumps(cls_def))
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
        # Default for the optional flag is False when omitted.
        self.assertFalse(c.classes["tplug"].echo_actions_to_chat)

    def test_echo_actions_to_chat_opt_in(self):
        # When `echo_actions_to_chat: true` is set in class.json,
        # the parsed DeviceClass reflects it.
        tmp = self._setup(class_overrides={"echo_actions_to_chat": True})
        c = cfg_mod.load(devices_dir=tmp / "devices",
                         instances_file=tmp / "devices.json")
        self.assertTrue(c.classes["tplug"].echo_actions_to_chat)

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


# --- parse_allowed_chats --------------------------------------------------

class TestParseAllowedChats(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(cfg_mod.parse_allowed_chats(""), set())
        self.assertEqual(cfg_mod.parse_allowed_chats(None), set())

    def test_simple(self):
        self.assertEqual(cfg_mod.parse_allowed_chats("12,34"), {12, 34})

    def test_whitespace(self):
        self.assertEqual(cfg_mod.parse_allowed_chats(" 12 , 34 ,, "), {12, 34})

# --- PlugTwin / Publisher / snapshot integration ------------------------
#
# We stub deltachat2.MsgData here so plug.py + bot.py imports don't fail
# without the real package installed. The tests below stub the four
# side-effecting callables (mqtt_publish, post_to_chats, broadcast,
# react) and feed inputs into the twin directly.

import logging  # noqa: E402
import types as _types  # noqa: E402

_deltachat_stub = _types.ModuleType("deltachat2")
class _MsgData:
    def __init__(self, text=None, file=None):
        self.text = text
        self.file = file
_deltachat_stub.MsgData = _MsgData
sys.modules.setdefault("deltachat2", _deltachat_stub)

import plug as plug_mod  # noqa: E402
import publisher as publisher_mod  # noqa: E402
import snapshot as snap_mod  # noqa: E402
from twins import TwinRegistry  # noqa: E402


# --- Class fixture: same shape config.load() produces from class.json ---

CLASS_JSON_OK = {
    "name": "tplug",
    "app_id": "tplug",
    "description": "test plug",
    "subscribe": [
        {"suffix": "online", "format": "text"},
        {"suffix": "status/switch:0", "format": "json"},
    ],
    "commands": {
        "on":  {"suffix": "command/switch:0", "payload": "on"},
        "off": {"suffix": "command/switch:0", "payload": "off"},
    },
    "state_fields": {
        "online":  {"from_suffix": "online", "extract": "bool_text"},
        "output":  {"from_suffix": "status/switch:0", "json_path": "output"},
        "apower":  {"from_suffix": "status/switch:0", "json_path": "apower"},
        "aenergy": {"from_suffix": "status/switch:0",
                    "json_path": "aenergy.total"},
    },
    "chat_events": [
        {"type": "on_change", "field": "output",
         "values": {"true": "💡 {name} ON", "false": "💡 {name} OFF"}},
        {"type": "threshold", "field": "apower",
         "limit_param": "power_threshold_watts",
         "duration_param": "power_threshold_duration_s",
         "above": "⚠️ {name} {value:.0f}W for {seconds}s",
         "below": "✅ {name} cleared"},
    ],
    "auto_off": {
        "command": "off",
        "default_idle_field": "apower",
        "default_idle_threshold": 5.0,
        "default_idle_duration": 60,
        "default_consumed_field": "apower",
        "default_consumed_threshold_wh": 5.0,
        "default_consumed_window_s": 600,
        "trigger_messages": {
            "timer": "🕐 {name} timer", "tod": "📅 {name} {hh}:{mm}",
            "idle": "💤 {name} idle", "consumed": "🔋 {name} consumed",
            "cancelled_manual": "↩️ {name} cancelled",
        },
    },
    "auto_on": {
        "command": "on",
        "trigger_messages": {
            "tod": "📅 {name} on at {hh}:{mm}",
            "cancelled_manual": "↩️ {name} on-cancelled",
        },
    },
}


def _build_twin(class_overrides=None, params=None, allowed_chats=(12,)):
    """Build a single PlugTwin against an in-memory class+device config
    and a stub TwinDeps. Returns (twin, calls), where calls is a dict
    that records all side-effect invocations."""
    cls_def = json.loads(json.dumps(CLASS_JSON_OK))
    if class_overrides:
        cls_def.update(class_overrides)
    tmp = Path(tempfile.mkdtemp())
    cls_dir = tmp / "devices" / "tplug"
    cls_dir.mkdir(parents=True)
    (cls_dir / "class.json").write_text(json.dumps(cls_def))
    instance = {"devices": [{
        "name": "kitchen", "class": "tplug",
        "topic_prefix": "p/kitchen",
        "allowed_chats": list(allowed_chats),
        **(params or {}),
    }]}
    inst_path = tmp / "devices.json"
    inst_path.write_text(json.dumps(instance))
    cfg = cfg_mod.load(devices_dir=tmp / "devices", instances_file=inst_path)

    calls = {"published": [], "posted": [], "broadcasts": [],
             "saves": 0, "reactions": []}
    deps = plug_mod.TwinDeps(
        mqtt_publish=lambda t, p: calls["published"].append((t, p)),
        post_to_chats=lambda dev, txt: calls["posted"].append((dev.name, txt)),
        broadcast=lambda name=None: calls["broadcasts"].append(name),
        save_rules=lambda: calls.__setitem__("saves", calls["saves"] + 1),
        save_baselines=lambda: calls.__setitem__("baseline_saves", calls.get("baseline_saves",0) + 1),
        react=lambda mid, e: calls["reactions"].append((mid, e)),
        history=None,
        client_id="tester",
    )
    twin = plug_mod.PlugTwin(
        cls=cfg.classes["tplug"], cfg=cfg.devices["kitchen"], deps=deps,
    )
    return twin, calls, cfg


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


# --- dispatch: publish + cancel + react + broadcast ----------------------

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

    def test_dispatch_cancels_same_direction_pending_rule(self):
        twin, calls, _ = _build_twin()
        # Schedule an auto-off, then dispatch off manually.
        policy = sched.ScheduledPolicy(timer_seconds=1800)
        twin.schedule("off", policy, chat_id_origin=12)
        self.assertEqual(len(twin.rules), 1)
        calls["broadcasts"].clear()
        calls["posted"].clear()
        ok, _ = twin.dispatch("off")
        self.assertTrue(ok)
        self.assertEqual(len(twin.rules), 0)  # cancelled
        self.assertTrue(any("cancelled" in t for _, t in calls["posted"]))


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


# --- snapshot.build_for_chat --------------------------------------------

class TestSnapshot(unittest.TestCase):
    def test_visible_devices_only(self):
        twin, _, cfg = _build_twin(allowed_chats=(12,))
        registry = TwinRegistry([twin])

        # Chat 12 sees the device.
        out = snap_mod.build_for_chat(12, "tplug", registry, set())
        self.assertIn("kitchen", out["devices"])

        # Chat 99 does not.
        self.assertIsNone(snap_mod.build_for_chat(99, "tplug", registry, set()))

    def test_unknown_class_returns_none(self):
        twin, _, _ = _build_twin()
        registry = TwinRegistry([twin])
        self.assertIsNone(snap_mod.build_for_chat(12, "nope", registry, set()))


# --- Publisher ----------------------------------------------------------

class TestPublisher(unittest.TestCase):
    def test_broadcast_iterates_msgid_map(self):
        sent = []
        builds = []

        def fake_build(chat, cls):
            builds.append((chat, cls))
            return {"class": cls, "devices": {}, "server_ts": 0}

        msgids = {12: {"tplug": 1001}, 14: {"tplug": 2002}}
        pub = publisher_mod.Publisher(
            build=fake_build,
            msgids=lambda: msgids,
            send=lambda c, m, p: (sent.append((c, m, p)), True)[1],
            interval_s=300,
        )
        pub.broadcast()
        self.assertEqual(builds, [(12, "tplug"), (14, "tplug")])
        self.assertEqual([(c, m) for c, m, _ in sent], [(12, 1001), (14, 2002)])

    def test_push_unicast_skips_when_build_returns_none(self):
        sent = []
        pub = publisher_mod.Publisher(
            build=lambda c, cl: None,
            msgids=lambda: {},
            send=lambda c, m, p: (sent.append((c, m, p)), True)[1],
            interval_s=300,
        )
        ok = pub.push_unicast(12, 1001, "tplug")
        self.assertFalse(ok)
        self.assertEqual(sent, [])


# --- TwinRegistry --------------------------------------------------------

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


# --- History (must come after the deltachat2 stub) ----------------------

import history as history_mod  # noqa: E402

class TestHistory(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.h = history_mod.History(self.tmp / "h.sqlite", retention_days=0)

    def tearDown(self):
        self.h.close()

    def test_write_sample_buffers_until_minute_rolls(self):
        # All samples in the same minute → not yet flushed
        self.h.write_sample("kaffeete", 1000, 5.0, output=True)
        self.h.write_sample("kaffeete", 1010, 15.0, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(pts, [])
        # Cross minute boundary → previous minute flushes
        self.h.write_sample("kaffeete", 1080, 100.0, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][0], 960)  # 1000 // 60 * 60
        self.assertAlmostEqual(pts[0][1], 10.0)
        self.assertEqual(pts[0][2], 1)  # output=on

    def test_output_off_persisted(self):
        self.h.write_sample("k", 100, 0.0, output=False)
        self.h.write_sample("k", 110, 0.1, output=False)
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][2], 0)  # output=off

    def test_output_unknown_when_never_reported(self):
        self.h.write_sample("k", 100, 5.0)  # no output kwarg
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertIsNone(pts[0][2])  # output unknown

    def test_flush_pending_minutes(self):
        self.h.write_sample("k", 100, 7.0, output=True)
        self.h.write_sample("k", 110, 13.0, output=True)
        self.h.flush_pending_minutes(now=200)
        bucket, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertAlmostEqual(pts[0][1], 10.0)
        self.assertEqual(pts[0][2], 1)

    def test_query_power_downsamples(self):
        # Insert 240 minutes of data, ask for 60 points → bucket ≥ 240s
        for i in range(240):
            self.h.write_sample("k", i * 60, float(i))
        # Force final minute to flush
        self.h.write_sample("k", 240 * 60 + 5, 0.0)
        bucket, pts = self.h.query_power("k", 0, 240 * 60, max_points=60)
        # bucket should be ≥ 240s (4 min) so ≤ 60 buckets cover 4 hours
        self.assertGreaterEqual(bucket, 240)
        self.assertLessEqual(len(pts), 60)
        self.assertGreater(len(pts), 0)

    def test_prune_with_retention(self):
        h = history_mod.History(self.tmp / "h2.sqlite", retention_days=1)
        try:
            now = 100_000_000
            old_ts = now - 2 * 86400  # 2 days ago
            recent_ts = now - 60       # 1 minute ago
            # Manually insert (write_sample needs minute boundaries to flush)
            h._db.execute(
                "INSERT INTO power_minute (device, ts, avg_apower_w, sample_count) "
                "VALUES ('k', ?, 5.0, 1)", (old_ts,)
            )
            h._db.execute(
                "INSERT INTO power_minute (device, ts, avg_apower_w, sample_count) "
                "VALUES ('k', ?, 5.0, 1)", (recent_ts,)
            )
            h._db.commit()
            h._last_prune_ts = 0  # force prune
            h._maybe_prune(now)
            # Query a tight window so the bucket stays at minute granularity.
            _, pts = h.query_power("k", recent_ts - 60, now + 60, max_points=200)
            self.assertEqual(len(pts), 1)
            self.assertEqual(pts[0][0], recent_ts - (recent_ts % 60))
            # Old sample is gone from the wider window too.
            _, all_pts = h.query_power("k", 0, now + 60, max_points=200)
            self.assertNotIn(old_ts - (old_ts % 60),
                              [p[0] for p in all_pts])
        finally:
            h.close()

    def _put_aenergy(self, device, ts, total_wh):
        """Test helper: write a raw aenergy_total_wh row to samples_raw
        via record_status (the production write path)."""
        self.h.record_status(device, ts, {"aenergy": {"total": total_wh}})

    def test_aenergy_at_via_samples_raw(self):
        # New semantics: latest at-or-before(target_ts), pulled directly
        # from samples_raw. No energy_hour / aenergy_minute involved.
        self._put_aenergy("k", 100, 100.0)
        self._put_aenergy("k", 200, 150.0)
        self._put_aenergy("k", 300, 220.0)
        self.assertEqual(self.h.aenergy_at("k", 99), None)   # before any data
        self.assertEqual(self.h.aenergy_at("k", 100), 100.0)
        self.assertEqual(self.h.aenergy_at("k", 250), 150.0) # latest <=250
        self.assertEqual(self.h.aenergy_at("k", 1000), 220.0) # latest overall

    def test_record_status_writes_samples_raw(self):
        payload = {
            "id": 0, "output": True,
            "apower": 42.0, "voltage": 230.5, "current": 0.18,
            "freq": 49.95, "temperature": {"tC": 28.5},
            "aenergy": {"total": 100.5},
        }
        self.h.record_status("k", 1700000050, payload)
        rows = self.h.query_samples_raw("k", 0, 2_000_000_000)
        self.assertEqual(len(rows), 1)
        ts, ap, v, c, fz, ae, out, tc = rows[0]
        self.assertEqual(ap, 42.0)
        self.assertEqual(v, 230.5)
        self.assertEqual(c, 0.18)
        self.assertEqual(fz, 49.95)
        self.assertEqual(ae, 100.5)
        self.assertEqual(out, 1)
        self.assertEqual(tc, 28.5)

    def test_aenergy_at_applies_offset_events(self):
        # Counter-reset offset events shift the effective lifetime.
        # 1000 Wh at t=100, then a reset at t=200 records +400 delta.
        # t=300 raw=50 → effective = 50 + 400 = 450 Wh.
        self._put_aenergy("k", 100, 1000.0)
        self._put_aenergy("k", 200, 50.0)   # raw dropped
        self.h.record_offset_event("k", 200, 400.0)
        self._put_aenergy("k", 300, 60.0)   # still raw
        # At t=99: no data → None.
        self.assertIsNone(self.h.aenergy_at("k", 99))
        # At t=150: pre-reset, raw=1000, no offsets <= 150 → 1000.
        self.assertEqual(self.h.aenergy_at("k", 150), 1000.0)
        # At t=250: raw=50 (latest <= 250), offset SUM=400 → 450.
        self.assertEqual(self.h.aenergy_at("k", 250), 450.0)
        # At t=300: raw=60, offset=400 → 460.
        self.assertEqual(self.h.aenergy_at("k", 300), 460.0)

    def test_record_offset_event_idempotent(self):
        # Same (device, ts) twice → INSERT OR IGNORE → only one row.
        self.h.record_offset_event("k", 100, 50.0)
        self.h.record_offset_event("k", 100, 999.0)  # ignored
        with self.h._lock:
            rows = list(self.h._db.execute(
                "SELECT ts, delta_wh FROM aenergy_offset_events "
                "WHERE device='k' ORDER BY ts ASC"
            ))
        self.assertEqual(rows, [(100, 50.0)])

    def test_energy_consumed_in_spans_offset_event(self):
        # Window straddling a reset event reports the post-offset
        # delta — user-facing math doesn't see the discontinuity.
        self._put_aenergy("k", 100, 1000.0)
        self._put_aenergy("k", 200, 100.0)            # raw dropped 900
        self.h.record_offset_event("k", 200, 900.0)   # offset closes the gap
        self._put_aenergy("k", 300, 250.0)            # +150 over post-reset
        # Window [99, 350] = effective(350) - effective(99 → falls back
        # to earliest) = (250+900) - (1000+0) = 1150 - 1000 = 150 Wh.
        wh, earliest = self.h.energy_consumed_in("k", 99, 350)
        self.assertAlmostEqual(wh, 150.0, places=3)
        self.assertEqual(earliest, 100)   # earliest available sample

    def test_energy_consumed_in_delta_of_total(self):
        # Two cumulative readings: 1000 Wh at t1, 1500 Wh at t2.
        # Energy in [t1-1, t2+1] = 500 Wh, derived from samples_raw.
        self._put_aenergy("k", 1000, 1000.0)
        self._put_aenergy("k", 4000, 1500.0)
        wh, earliest = self.h.energy_consumed_in("k", 999, 4001)
        self.assertAlmostEqual(wh, 500.0, places=3)
        self.assertEqual(earliest, 1000)

    def test_energy_consumed_in_clamps_negative_to_zero(self):
        # If raw counter dropped without an offset event being
        # recorded (corrupted state, lost write), clamp to 0.
        self._put_aenergy("k", 1000, 1500.0)
        self._put_aenergy("k", 4000, 100.0)
        wh, _ = self.h.energy_consumed_in("k", 999, 4001)
        self.assertEqual(wh, 0.0)

    def test_energy_consumed_in_partial_window(self):
        # Window starts before any data → use earliest available row
        # as the lower bound; report it as earliest_ts.
        self._put_aenergy("k", 1000, 100.0)
        self._put_aenergy("k", 4000, 200.0)
        wh, earliest = self.h.energy_consumed_in("k", 0, 4001)
        self.assertAlmostEqual(wh, 100.0, places=3)
        self.assertEqual(earliest, 1000)

    def test_daily_energy_kwh_uses_aenergy_at(self):
        # Daily bars are differences between aenergy_at(midnight) values.
        midnight = 1700006400
        oldest = midnight - 2 * 86400
        # A reading just before each midnight (so each "day" is bounded).
        self._put_aenergy("k", oldest - 60, 1000.0)
        self._put_aenergy("k", oldest + 86400 - 60, 1050.0)
        self._put_aenergy("k", oldest + 2 * 86400 - 60, 1130.0)
        self._put_aenergy("k", oldest + 3 * 86400 - 60, 1160.0)
        days = self.h.daily_energy_kwh("k", midnight, days=3)
        self.assertEqual(len(days), 3)
        self.assertAlmostEqual(days[0][1], 50.0, places=3)
        self.assertAlmostEqual(days[1][1], 80.0, places=3)
        self.assertAlmostEqual(days[2][1], 30.0, places=3)

    def test_query_power_raw(self):
        self.h.write_sample("k", 60,  10.0, output=True)
        self.h.write_sample("k", 70,  20.0, output=True)  # same minute
        self.h.write_sample("k", 130, 30.0, output=False) # next minute
        self.h.flush_pending_minutes(now=200)
        rows = self.h.query_power_raw("k", 0, 200)
        self.assertEqual(len(rows), 2)
        # First row: minute 60, avg of 10 and 20 = 15, output=on, count=2
        self.assertEqual(rows[0][0], 60)
        self.assertAlmostEqual(rows[0][1], 15.0)
        self.assertEqual(rows[0][2], 1)
        self.assertEqual(rows[0][3], 2)
        # Second: minute 120, single sample 30, output=off, count=1
        self.assertEqual(rows[1][0], 120)
        self.assertEqual(rows[1][2], 0)
        self.assertEqual(rows[1][3], 1)

    def test_retention_zero_keeps_forever(self):
        h = history_mod.History(self.tmp / "h3.sqlite", retention_days=0)
        try:
            old_ts = 1_000_000  # ancient
            h._db.execute(
                "INSERT INTO power_minute (device, ts, avg_apower_w, sample_count) "
                "VALUES ('k', ?, 5.0, 1)", (old_ts,)
            )
            h._db.commit()
            h._maybe_prune(int(time.time()))
            _, pts = h.query_power("k", 0, 2_000_000)
            self.assertEqual(len(pts), 1)
        finally:
            h.close()


# --- Resettable counter -----------------------------------------------

class TestPlugTwinResetCounter(unittest.TestCase):
    def test_reset_snaps_baseline_and_signals(self):
        twin, calls, _ = _build_twin()
        # Capture save_baselines() calls — _build_twin's deps already
        # has it via the sed-injected lambda.
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


class TestPlugTwinCounterResetDetection(unittest.TestCase):
    """The plug's hardware aenergy.total going backwards records an
    offset event in history.aenergy_offset_events. self.fields
    ["aenergy"] keeps the RAW counter value; the displayed lifetime
    is computed at read time via history.aenergy_at (which adds the
    cumulative offset SUM)."""

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


class TestKwhSinceResetMath(unittest.TestCase):
    def test_baseline_subtracted_correctly(self):
        from snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=12345.0,
                            baseline_wh=345.0,
                            reset_at_ts=int(time.time()))
        self.assertAlmostEqual(e["kwh_since_reset"], 12.0, places=6)
        self.assertEqual(e["current_total_wh"], 12345.0)  # Lifetime untouched

    def test_clamped_to_zero_on_rollover(self):
        # Plug counter rollover or a stale baseline shouldn't show negative.
        from snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=10.0, baseline_wh=100.0,
                            reset_at_ts=None)
        self.assertEqual(e["kwh_since_reset"], 0.0)

    def test_none_when_no_current_reading(self):
        from snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=None, baseline_wh=0.0,
                            reset_at_ts=None)
        self.assertIsNone(e["kwh_since_reset"])

    def test_includes_kwh_last_365d(self):
        from snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=100.0, baseline_wh=0.0,
                            reset_at_ts=None)
        self.assertIn("kwh_last_365d", e)
        self.assertEqual(set(e["kwh_last_365d"].keys()),
                         {"kwh", "partial_since_ts"})


# --- Grace period for rehydrated rules --------------------------------

class TestGracePeriod(unittest.TestCase):
    def test_loaded_rule_does_not_fire_during_grace(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True   # not dormant for off-rule
        # Simulate a rule restored from rules.json: deadline already
        # elapsed AND _loaded_at set to "now".
        now = int(time.time())
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=now - 5,            # would fire immediately
            _time_mode="timer",
            timer_seconds=600,
            once=False,
            _loaded_at=now,                  # within grace period
        )
        twin.add_persisted_rule(job)
        twin.tick_time(now)
        # No fire, rule survives.
        self.assertEqual(calls["published"], [])
        self.assertEqual(len(twin.rules), 1)

    def test_loaded_rule_fires_after_grace(self):
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True
        now = int(time.time())
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=now - 5, _time_mode="timer", timer_seconds=600,
            once=False, _loaded_at=now,
        )
        twin.add_persisted_rule(job)
        # Past the grace period.
        twin.tick_time(now + sched.GRACE_PERIOD_S + 1)
        self.assertEqual(len(calls["published"]), 1)

    def test_runtime_scheduled_rule_fires_immediately(self):
        # Counter-test: rules added via twin.schedule (not load_into)
        # have _loaded_at == 0 and skip the grace gate entirely.
        twin, calls, _ = _build_twin()
        twin.fields["output"] = True
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=1), 12)
        # Fast-forward past the deadline.
        now = int(time.time()) + 5
        twin.tick_time(now)
        self.assertEqual(len(calls["published"]), 1)

    def test_load_into_stamps_loaded_at(self):
        # The persistence path sets _loaded_at; round-trip via save_all
        # then load_into into a fresh twin and assert.
        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        from twins import TwinRegistry as _Reg
        registry = _Reg([twin])
        path = Path(tempfile.mkdtemp()) / "rules.json"
        sched.save_all(registry, path)
        # Wipe and reload.
        twin.rules.clear()
        sched.load_into(registry, path)
        self.assertEqual(len(twin.rules), 1)
        self.assertGreater(twin.rules[0]._loaded_at, 0)


# --- Snapshot protocol contract -----------------------------------------
#
# Locks the bot↔app payload shape. A protocol drift on either side
# (renamed key, missing field, wrong nesting level) fails the test
# instead of leaving the UI silently blank — which is the exact bug
# we hit when first deploying v0.2.0.

class TestSnapshotContract(unittest.TestCase):
    def setUp(self):
        # Build a twin with mock history so build_for_chat populates
        # every per-device key.
        twin, _, _ = _build_twin()
        twin.deps = plug_mod.TwinDeps(
            mqtt_publish=lambda *a: None,
            post_to_chats=lambda *a: None,
            broadcast=lambda *a: None,
            save_rules=lambda: None,
            save_baselines=lambda: None,
            react=lambda *a: None,
            history=_FakeHistory(),
            client_id="test",
        )
        twin.fields = {"online": True, "output": True,
                        "apower": 42.0, "aenergy": 1234.5}
        twin.last_update_ts = int(time.time())
        from twins import TwinRegistry as _Reg
        self.registry = _Reg([twin])

    def test_top_level_keys(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        self.assertIsNotNone(snap)
        # Snapshot is at the top level of payload — NO `snapshot:` wrapper.
        # The app reads `payload.devices`, `payload.server_ts`,
        # `payload.echo_actions_to_chat`, etc.
        self.assertEqual(set(snap.keys()),
                         {"class", "server_ts", "echo_actions_to_chat",
                          "devices"})
        self.assertEqual(snap["class"], "tplug")
        self.assertIsInstance(snap["server_ts"], int)
        # Default for the test fixture is False (off).
        self.assertEqual(snap["echo_actions_to_chat"], False)

    def test_per_device_keys(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        dev = snap["devices"]["kitchen"]
        # Every key the app reads. If you add/rename one in plug.py.to_dict,
        # update both here AND devices/<class>/app/main.js.
        for k in ("name", "description", "fields", "last_update_ts",
                  "scheduled_jobs", "params", "energy",
                  "daily_energy_wh", "power_history"):
            self.assertIn(k, dev, f"missing top-level device key: {k}")
        # power_history shape — minute @ ≤24h, hour @ ≤31d.
        self.assertEqual(set(dev["power_history"].keys()), {"minute", "hour"})

    def test_power_history_tuple_shape(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        ph = snap["devices"]["kitchen"]["power_history"]
        # Each entry is [ts:int, w:float, output:0|1|None].
        for series in (ph["minute"], ph["hour"]):
            for entry in series[:5]:  # first 5 are enough
                self.assertEqual(len(entry), 3)
                self.assertIsInstance(entry[0], int)
                self.assertIsInstance(entry[1], (int, float))
                self.assertIn(entry[2], (0, 1, None))

    def test_webxdc_io_wraps_payload_correctly(self):
        # webxdc.push_to_msgid is what the publisher actually calls.
        # It must serialise to {"payload": <snapshot>} with NO extra
        # wrapping — main.js reads update.payload.devices directly.
        captured = []
        class _StubBot:
            class rpc:
                @staticmethod
                def send_webxdc_status_update(_a, _m, body, _d):
                    captured.append(body)

        import webxdc_io as wio
        tmp = Path(tempfile.mkdtemp())
        io = wio.WebxdcIO(state_dir=tmp, devices_dir=tmp / "devices")
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        ok = io.push_to_msgid(_StubBot(), 0, 99, snap)
        self.assertTrue(ok)
        body = json.loads(captured[0])
        # Single `payload` wrapper, snapshot keys at the top level
        # under it (NO extra `snapshot:` nesting).
        self.assertEqual(set(body.keys()), {"payload"})
        # The required structural keys; class-level flags (like
        # echo_actions_to_chat) may also be present.
        for k in ("class", "server_ts", "devices"):
            self.assertIn(k, body["payload"])
        self.assertIn("kitchen", body["payload"]["devices"])


# --- rules.save_all / load_into round trip -----------------------------

class TestRulesPersistence(unittest.TestCase):
    def setUp(self):
        twin, _, _ = _build_twin()
        # Schedule three rules of mixed shapes.
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        twin.schedule("off", sched.ScheduledPolicy(
            consumed_field="apower", consumed_threshold_wh=5.0,
            consumed_window_s=600), 12)
        twin.schedule("on", sched.ScheduledPolicy(
            time_of_day=(7, 30), recurring_tod=True), 12)
        from twins import TwinRegistry as _Reg
        self.registry = _Reg([twin])
        self.path = Path(tempfile.mkdtemp()) / "rules.json"

    def test_save_load_round_trip(self):
        sched.save_all(self.registry, self.path)
        before = sorted(j.rule_id for j in self.registry.get("kitchen").rules)

        # Wipe the in-memory rules and reload from disk.
        self.registry.get("kitchen").rules.clear()
        n = sched.load_into(self.registry, self.path)
        self.assertEqual(n, 3)
        after = sorted(j.rule_id for j in self.registry.get("kitchen").rules)
        self.assertEqual(before, after)

    def test_load_into_drops_rules_for_unknown_device(self):
        # Save rules from a registry with `kitchen`, then load into a
        # registry that no longer has that twin.
        sched.save_all(self.registry, self.path)
        from twins import TwinRegistry as _Reg
        empty = _Reg([])
        with self.assertLogs("mqtt_bot.rules", level="WARNING") as cap:
            n = sched.load_into(empty, self.path)
        self.assertEqual(n, 0)
        # The summary log line should mention `kitchen`.
        self.assertTrue(any("kitchen" in m for m in cap.output))


# --- Second device class (Tasmota) — engine is class-agnostic ---------
#
# Loads the real devices/tasmota_plug/class.json from the repo, wires
# a twin, feeds Tasmota-shaped MQTT payloads through it, and asserts
# state extraction + dispatch + snapshot all work without any Python
# changes. The whole point of the declarative class model is that
# adding a device type is a config-only change; this test guards that.

class TestTasmotaClass(unittest.TestCase):
    def setUp(self):
        # Use the real devices/ directory + a one-device temp instances file.
        repo_devices = HERE / "devices"
        self.assertTrue((repo_devices / "tasmota_plug" / "class.json").exists())
        tmp = Path(tempfile.mkdtemp())
        (tmp / "devices.json").write_text(json.dumps({"devices": [{
            "name": "lamp", "class": "tasmota_plug",
            "topic_prefix": "tasmota_AB12CD",
            "allowed_chats": [12],
            "power_threshold_watts": 1000,
            "power_threshold_duration_s": 5,
        }]}))
        self.cfg = cfg_mod.load(devices_dir=repo_devices,
                                instances_file=tmp / "devices.json")
        self.calls = {"published": [], "posted": [], "broadcasts": []}
        deps = plug_mod.TwinDeps(
            mqtt_publish=lambda t, p: self.calls["published"].append((t, p)),
            post_to_chats=lambda dev, txt: self.calls["posted"].append(txt),
            broadcast=lambda n=None: self.calls["broadcasts"].append(n),
            save_rules=lambda: None,
            save_baselines=lambda: None,
            react=lambda *a: None,
            history=None,
            client_id="test",
        )
        self.twin = plug_mod.PlugTwin(
            cls=self.cfg.classes["tasmota_plug"],
            cfg=self.cfg.devices["lamp"], deps=deps,
        )

    def test_lwt_drives_online_field(self):
        # Tasmota LWT is text "Online"/"Offline" → bool_text extractor
        # should accept "online" (lowercase via normalisation in state.py).
        # The class config uses suffix "LWT" with extract:"bool_text".
        # bool_text accepts "true|1|on" and "false|0|off"; anything else
        # silently skips. Tasmota's "Online"/"Offline" payload doesn't
        # match those keywords — so this is intentional: the LWT topic
        # only updates `online` to True/False if the user has Tasmota
        # configured to publish "true"/"false" (a 1-line SetOption).
        # We verify the path exists; full LWT mapping is the user's
        # configuration concern.
        self.twin.on_mqtt("LWT", b"true")
        self.assertEqual(self.twin.fields.get("online"), True)
        self.twin.on_mqtt("LWT", b"false")
        self.assertEqual(self.twin.fields.get("online"), False)

    def test_stat_power_drives_output_field(self):
        # Tasmota's relay echo on stat/POWER is "ON"/"OFF" text.
        self.twin.on_mqtt("stat/POWER", b"on")
        self.assertEqual(self.twin.fields["output"], True)
        self.assertTrue(any("ON" in s for s in self.calls["posted"]))
        self.twin.on_mqtt("stat/POWER", b"off")
        self.assertEqual(self.twin.fields["output"], False)

    def test_sensor_payload_extracts_apower_aenergy(self):
        # Standard Tasmota tele/SENSOR shape.
        payload = {
            "Time": "2026-05-08T12:00:00",
            "ENERGY": {
                "Total": 12.345, "Yesterday": 1.0, "Today": 0.5,
                "Power": 42, "ApparentPower": 50, "ReactivePower": 10,
                "Factor": 0.84, "Voltage": 230, "Current": 0.183,
            },
        }
        self.twin.on_mqtt("tele/SENSOR", json.dumps(payload).encode())
        self.assertEqual(self.twin.fields["apower"], 42)
        self.assertEqual(self.twin.fields["aenergy"], 12.345)
        self.assertEqual(self.twin.fields["voltage"], 230)

    def test_dispatch_publishes_tasmota_topic(self):
        # /dev on for a Tasmota device should publish to cmnd/POWER
        # with payload "ON" — totally different from Shelly's
        # command/switch:0 + "on".
        ok, _ = self.twin.dispatch("on")
        self.assertTrue(ok)
        self.assertEqual(self.calls["published"],
                         [("tasmota_AB12CD/cmnd/POWER", "ON")])

    def test_two_classes_coexist_in_one_registry(self):
        # The whole point: one bot, two classes, no Python change.
        repo_devices = HERE / "devices"
        tmp = Path(tempfile.mkdtemp())
        (tmp / "devices.json").write_text(json.dumps({"devices": [
            {"name": "kitchen", "class": "shelly_plug",
             "topic_prefix": "p/kitchen", "allowed_chats": [12]},
            {"name": "lamp", "class": "tasmota_plug",
             "topic_prefix": "tasmota_AB12CD", "allowed_chats": [12]},
        ]}))
        cfg2 = cfg_mod.load(devices_dir=repo_devices,
                            instances_file=tmp / "devices.json")
        self.assertEqual(set(cfg2.classes), {"shelly_plug", "tasmota_plug"})
        self.assertEqual(set(cfg2.devices), {"kitchen", "lamp"})


# --- In-process integration test ---------------------------------------
#
# Exercises the full bot-internal routing chain — topic → registry →
# twin → state edge → publisher.broadcast → snapshot.build_for_chat →
# send — without Delta Chat or paho. Catches wiring bugs that no
# single-module test would (e.g. the v0.2 deploy that pushed snapshots
# the app couldn't read).

class TestIntegrationRoutingChain(unittest.TestCase):
    def setUp(self):
        # Build two twins (different classes, different chats) wired to
        # a captured-send Publisher and a real TwinRegistry.
        repo_devices = HERE / "devices"
        tmp = Path(tempfile.mkdtemp())
        (tmp / "devices.json").write_text(json.dumps({"devices": [
            {"name": "kitchen", "class": "shelly_plug",
             "topic_prefix": "p/kitchen", "allowed_chats": [12]},
            {"name": "lamp", "class": "tasmota_plug",
             "topic_prefix": "tasmota_AB", "allowed_chats": [12, 14]},
        ]}))
        cfg = cfg_mod.load(devices_dir=repo_devices,
                           instances_file=tmp / "devices.json")

        # Captured-send publisher: records every (chat, msgid, payload).
        self.sent: list[tuple[int, int, dict]] = []

        from twins import TwinRegistry as _Reg
        from snapshot import build_for_chat as _build
        from publisher import Publisher as _Pub

        # We construct twins below with a deps that calls broadcast on
        # every edge — deps.broadcast captures `self.publisher` via
        # closure. The forward reference is fine because Python
        # resolves the closure lazily at call time.
        self.registry: "_Reg | None" = None
        self.publisher: "_Pub | None" = None
        twins = []
        for d in cfg.devices.values():
            cls_name = d.class_name
            deps = plug_mod.TwinDeps(
                mqtt_publish=lambda t, p: None,
                post_to_chats=lambda dev, txt: None,
                # Mirror bot._publisher_broadcast: filter by class so
                # an edge on one class doesn't churn another class's
                # apps. Capture class via default-arg trick.
                broadcast=(lambda name=None, _cls=cls_name:
                           self.publisher.broadcast(
                               name, only_class=_cls, force=True)),
                save_rules=lambda: None,
            save_baselines=lambda: None,
                react=lambda *a: None,
                history=None,
                client_id="test",
            )
            twins.append(plug_mod.PlugTwin(
                cls=cfg.classes[d.class_name], cfg=d, deps=deps))
        self.registry = _Reg(twins)
        # Two chats know about apps for both classes.
        msgid_map = {
            12: {"shelly_plug": 1001, "tasmota_plug": 1002},
            14: {"tasmota_plug": 2002},
        }
        self.publisher = _Pub(
            build=lambda chat, cls: _build(chat, cls, self.registry, set()),
            msgids=lambda: msgid_map,
            send=lambda c, m, p: (self.sent.append((c, m, p)), True)[1],
            interval_s=300,
        )

    def _route(self, topic: str, payload: bytes) -> None:
        """Mimic bot.on_mqtt_message: registry lookup + twin dispatch."""
        found = self.registry.find_by_topic(topic)
        self.assertIsNotNone(found, f"no twin for topic {topic}")
        twin, suffix = found
        twin.on_mqtt(suffix, payload)

    def test_shelly_status_propagates_to_visible_chats(self):
        # status/switch:0 with output=true → on_change fires → broadcast
        # → publisher pushes to chat 12 (kitchen visible) but NOT 14
        # (kitchen hidden — lamp visible only).
        self.sent.clear()
        self._route("p/kitchen/status/switch:0",
                    json.dumps({"output": True}).encode())
        self.assertGreaterEqual(len(self.sent), 1)
        targets = {(c, m) for c, m, _ in self.sent}
        self.assertIn((12, 1001), targets)   # kitchen visible to chat 12
        # chat 14 sees no shelly_plug devices → no shelly_plug push.
        for c, m, _ in self.sent:
            if c == 14:
                self.assertNotEqual(m, 1001)

    def test_tasmota_lwt_propagates_to_both_chats(self):
        # lamp is visible in chats 12 AND 14, so an edge on it should
        # push tasmota_plug snapshots to both.
        self.sent.clear()
        self._route("tasmota_AB/LWT", b"true")
        targets = {(c, m) for c, m, _ in self.sent}
        self.assertIn((12, 1002), targets)
        self.assertIn((14, 2002), targets)

    def test_payload_shape_matches_app_expectations(self):
        # Drive a state change, capture the pushed snapshot, and assert
        # the app would be able to render it. Top-level keys MUST be
        # {class, server_ts, devices} — anything else and main.js
        # bails (which is the v0.2 deploy bug we're guarding against).
        self.sent.clear()
        self._route("tasmota_AB/stat/POWER", b"on")
        self.assertGreater(len(self.sent), 0)
        for chat_id, msgid, payload in self.sent:
            for k in ("class", "server_ts", "devices"):
                self.assertIn(k, payload)
            self.assertIn("lamp", payload["devices"])
            dev = payload["devices"]["lamp"]
            self.assertIn("fields", dev)
            self.assertIn("scheduled_jobs", dev)


# --- baselines.json round-trip + legacy migration ---------------------

class TestBaselinesPersistence(unittest.TestCase):
    """The user-Counter state (baseline_wh, reset_at_ts) round-trips
    through baselines.json. Hardware-counter-reset offsets are NOT
    in this file in v0.2+; if a legacy `aenergy_offset_wh` entry
    appears, it migrates to a single aenergy_offset_events row at
    ts=0 (idempotent)."""

    def setUp(self):
        import baselines as baselines_mod
        self.baselines_mod = baselines_mod
        self.tmpdir = Path(tempfile.mkdtemp())
        self.h = history_mod.History(self.tmpdir / "h.sqlite")
        twin, _, _ = _build_twin()
        # Wire the twin to the real History so legacy-offset migration
        # can hit aenergy_offset_events.
        twin.deps = plug_mod.TwinDeps(
            mqtt_publish=lambda t, p: None, post_to_chats=lambda *a: None,
            broadcast=lambda n=None: None, save_rules=lambda: None,
            save_baselines=lambda: None, react=lambda *a: None,
            history=self.h, client_id="test",
        )
        from twins import TwinRegistry as _Reg
        self.registry = _Reg([twin])
        self.path = self.tmpdir / "baselines.json"

    def test_save_load_round_trip(self):
        twin = self.registry.get("kitchen")
        twin.set_baseline(baseline_wh=12345.6, reset_at_ts=1714000000)
        self.baselines_mod.save(self.registry, self.path)
        # Wipe in-memory state and reload.
        twin.set_baseline(0.0, None)
        n = self.baselines_mod.load_into(self.registry, self.h, self.path)
        self.assertEqual(n, 1)
        self.assertEqual(twin.baseline_wh, 12345.6)
        self.assertEqual(twin.reset_at_ts, 1714000000)

    def test_legacy_aenergy_offset_wh_migration(self):
        # Hand-craft a baselines.json that an older bot version would
        # have written: includes aenergy_offset_wh per device.
        self.path.write_text(json.dumps({
            "kitchen": {
                "baseline_wh": 500.0,
                "reset_at_ts": 1714000000,
                "aenergy_offset_wh": 800.0,    # legacy field
            }
        }))
        # No offset events yet.
        with self.h._lock:
            cnt = self.h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events"
            ).fetchone()[0]
        self.assertEqual(cnt, 0)

        n = self.baselines_mod.load_into(self.registry, self.h, self.path)
        self.assertEqual(n, 1)
        twin = self.registry.get("kitchen")
        # Baseline + reset_at_ts loaded.
        self.assertEqual(twin.baseline_wh, 500.0)
        self.assertEqual(twin.reset_at_ts, 1714000000)
        # Legacy offset migrated as a single ts=0 row.
        with self.h._lock:
            rows = list(self.h._db.execute(
                "SELECT ts, delta_wh FROM aenergy_offset_events "
                "WHERE device='kitchen' ORDER BY ts ASC"
            ))
        self.assertEqual(rows, [(0, 800.0)])
        # Idempotent: a second load doesn't double-insert (ts=0 PK collides).
        self.baselines_mod.load_into(self.registry, self.h, self.path)
        with self.h._lock:
            rows = list(self.h._db.execute(
                "SELECT ts, delta_wh FROM aenergy_offset_events "
                "WHERE device='kitchen' ORDER BY ts ASC"
            ))
        self.assertEqual(rows, [(0, 800.0)])
        # And the offset is reflected in aenergy_at: a sample of 100 Wh
        # at any ts becomes effective 100 + 800 = 900 Wh.
        self.h.record_status("kitchen", 1714001000, {"aenergy": {"total": 100.0}})
        self.assertEqual(self.h.aenergy_at("kitchen", 1714001000), 900.0)

    def test_legacy_zero_offset_not_migrated(self):
        # An aenergy_offset_wh of 0 (the common case for v0.2.2 users
        # who never had a hardware reset) should NOT produce an event.
        self.path.write_text(json.dumps({
            "kitchen": {
                "baseline_wh": 100.0, "reset_at_ts": None,
                "aenergy_offset_wh": 0.0,
            }
        }))
        self.baselines_mod.load_into(self.registry, self.h, self.path)
        with self.h._lock:
            cnt = self.h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events"
            ).fetchone()[0]
        self.assertEqual(cnt, 0)

    def test_unknown_device_warns_and_skips(self):
        self.path.write_text(json.dumps({
            "ghost": {"baseline_wh": 99.0, "reset_at_ts": None}
        }))
        with self.assertLogs("mqtt_bot.baselines", level="WARNING") as cap:
            n = self.baselines_mod.load_into(self.registry, self.h, self.path)
        self.assertEqual(n, 0)
        self.assertTrue(any("ghost" in m for m in cap.output))


# --- Cold-start end-to-end ---------------------------------------------
#
# Empty SQLite + empty baselines.json + a fresh PlugTwin. The first
# MQTT status update should land in samples_raw, populate
# twin.fields["aenergy"] (RAW), and result in a snapshot whose
# Lifetime / Counter / kwh_today numbers are sourced via
# history.aenergy_at (with no offsets recorded). This guards the
# happy path that v0.2.x users hit on their first deploy.

class TestColdStartIntegration(unittest.TestCase):
    def test_first_status_update_produces_correct_snapshot(self):
        # Real History (in a temp file), real PlugTwin, stubbed
        # publisher / chat sinks. No baselines.json — we're cold-starting.
        twin, calls, _ = _build_twin()
        tmpdir = Path(tempfile.mkdtemp())
        h = history_mod.History(tmpdir / "h.sqlite")
        twin.deps = plug_mod.TwinDeps(
            mqtt_publish=lambda t, p: None,
            post_to_chats=lambda dev, txt: calls["posted"].append((dev.name, txt)),
            broadcast=lambda n=None: calls["broadcasts"].append(n),
            save_rules=lambda: None,
            save_baselines=lambda: None,
            react=lambda *a: None,
            history=h,
            client_id="test",
        )

        # Pre-state: empty samples_raw + no offset events.
        with h._lock:
            self.assertEqual(h._db.execute(
                "SELECT COUNT(*) FROM samples_raw").fetchone()[0], 0)
            self.assertEqual(h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events").fetchone()[0], 0)

        # Drive a single status update with all the typical fields.
        twin.on_mqtt("status/switch:0", json.dumps({
            "output": True, "apower": 42.0, "voltage": 230.0,
            "aenergy": {"total": 12345.6},
            "temperature": {"tC": 25.5},
        }).encode())

        # samples_raw populated with the RAW aenergy.
        with h._lock:
            row = h._db.execute(
                "SELECT aenergy_total_wh FROM samples_raw "
                "WHERE device='kitchen'"
            ).fetchone()
        self.assertEqual(row[0], 12345.6)

        # No reset → no offset events.
        with h._lock:
            self.assertEqual(h._db.execute(
                "SELECT COUNT(*) FROM aenergy_offset_events").fetchone()[0], 0)

        # twin.fields["aenergy"] holds the RAW counter (effective
        # is computed at read time via aenergy_at).
        self.assertEqual(twin.fields["aenergy"], 12345.6)

        # aenergy_at(now) returns the same RAW (no offsets to add).
        eff = h.aenergy_at("kitchen", int(time.time()))
        self.assertEqual(eff, 12345.6)

        # Snapshot built via to_dict has Lifetime = 12345.6 (Wh) and
        # kwh_since_reset == kwh_lifetime when no Counter reset has
        # happened (baseline_wh defaults to 0).
        snap = twin.to_dict()
        self.assertIn("energy", snap)
        self.assertEqual(snap["energy"]["current_total_wh"], 12345.6)
        # Counter math: (12345.6 - 0) / 1000 = 12.3456 kWh.
        self.assertAlmostEqual(snap["energy"]["kwh_since_reset"],
                                12.3456, places=4)

    def test_aenergy_at_returns_none_before_first_sample(self):
        # Cold DB, no samples_raw rows yet — aenergy_at returns None,
        # to_dict gracefully falls back to the in-memory raw field.
        twin, _, _ = _build_twin()
        tmpdir = Path(tempfile.mkdtemp())
        h = history_mod.History(tmpdir / "h.sqlite")
        twin.deps = plug_mod.TwinDeps(
            mqtt_publish=lambda t, p: None, post_to_chats=lambda *a: None,
            broadcast=lambda n=None: None, save_rules=lambda: None,
            save_baselines=lambda: None, react=lambda *a: None,
            history=h, client_id="test",
        )
        # No fields, no samples_raw — graceful empty.
        self.assertIsNone(h.aenergy_at("kitchen", int(time.time())))
        snap = twin.to_dict()
        # current_total_wh is None when nothing's been seen yet.
        self.assertIsNone(snap["energy"]["current_total_wh"])


# Stub History used by the snapshot-contract test. Just enough of the
# query API for build_for_chat to populate every key.
class _FakeHistory:
    def query_power(self, *_a, **_kw):
        return (60, [])
    def daily_energy_kwh(self, *_a, **_kw):
        return [(0, 0.0)] * 30
    def energy_consumed_in(self, *_a, **_kw):
        return (0.0, None)
    def aenergy_at(self, *_a, **_kw):
        return None
    def record_offset_event(self, *_a, **_kw):
        pass


if __name__ == "__main__":
    unittest.main()
