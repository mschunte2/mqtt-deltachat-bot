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
        self.h.write_sample("kaffeete", 1000, 5.0, None, output=True)
        self.h.write_sample("kaffeete", 1010, 15.0, None, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(pts, [])
        # Cross minute boundary → previous minute flushes
        self.h.write_sample("kaffeete", 1080, 100.0, None, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][0], 960)  # 1000 // 60 * 60
        self.assertAlmostEqual(pts[0][1], 10.0)
        self.assertEqual(pts[0][2], 1)  # output=on

    def test_output_off_persisted(self):
        self.h.write_sample("k", 100, 0.0, None, output=False)
        self.h.write_sample("k", 110, 0.1, None, output=False)
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][2], 0)  # output=off

    def test_output_unknown_when_never_reported(self):
        self.h.write_sample("k", 100, 5.0, None)  # no output kwarg
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertIsNone(pts[0][2])  # output unknown

    def test_flush_pending_minutes(self):
        self.h.write_sample("k", 100, 7.0, None, output=True)
        self.h.write_sample("k", 110, 13.0, None, output=True)
        self.h.flush_pending_minutes(now=200)
        bucket, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertAlmostEqual(pts[0][1], 10.0)
        self.assertEqual(pts[0][2], 1)

    def test_energy_snapshot_replaces_within_hour(self):
        self.h.write_sample("k", 100, None, 50.0)
        self.h.write_sample("k", 200, None, 75.0)  # later in same hour
        rows = self.h.query_energy("k", 0, 4000)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 0)         # hour-aligned
        self.assertAlmostEqual(rows[0][1], 75.0)  # latest snapshot wins

    def test_energy_snapshot_per_hour(self):
        self.h.write_sample("k", 100, None, 50.0)
        self.h.write_sample("k", 3700, None, 60.0)  # next hour
        rows = self.h.query_energy("k", 0, 7200)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 0)
        self.assertEqual(rows[1][0], 3600)


    def test_query_power_downsamples(self):
        # Insert 240 minutes of data, ask for 60 points → bucket ≥ 240s
        for i in range(240):
            self.h.write_sample("k", i * 60, float(i), None)
        # Force final minute to flush
        self.h.write_sample("k", 240 * 60 + 5, 0.0, None)
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

    def test_aenergy_at(self):
        # Insert hourly snapshots
        self.h.write_sample("k", 0,    None, 100.0)
        self.h.write_sample("k", 3700, None, 150.0)
        self.h.write_sample("k", 7300, None, 220.0)
        # Asking for ts within first hour returns first snapshot
        self.assertEqual(self.h.aenergy_at("k", 0), 100.0)
        # Asking exactly at second hour boundary returns that snapshot
        self.assertEqual(self.h.aenergy_at("k", 3600), 150.0)
        # Asking after the last snapshot returns None
        self.assertIsNone(self.h.aenergy_at("k", 10000))

    def test_record_status_writes_samples_raw(self):
        payload = {
            "id": 0, "output": True,
            "apower": 42.0, "voltage": 230.5, "current": 0.18,
            "freq": 49.95, "temperature": {"tC": 28.5},
            "aenergy": {"total": 100.5, "by_minute": [0.0, 3500.0, 3200.0],
                        "minute_ts": 1700000060},
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

    def test_record_status_extracts_energy_minute(self):
        # by_minute[1] should land at minute_ts-60, by_minute[2] at -120.
        # by_minute[0] (the in-progress 0) should be skipped.
        payload = {
            "aenergy": {"total": 100.0,
                        "by_minute": [0.0, 3500.0, 3200.0],
                        "minute_ts": 1700000060},
        }
        self.h.record_status("k", 1700000050, payload)
        rows = self.h.query_energy_minute("k", 0, 2_000_000_000)
        self.assertEqual(len(rows), 2)
        # Sorted ascending. First = oldest = 1700000060 - 120
        self.assertEqual(rows[0][0], 1699999940)
        self.assertAlmostEqual(rows[0][1], 3200.0)
        self.assertEqual(rows[1][0], 1700000000)
        self.assertAlmostEqual(rows[1][1], 3500.0)

    def test_daily_energy_kwh_padded(self):
        # Insert one day's worth of by_minute data and ask for last 5 days.
        # Result should have 5 buckets with only the relevant one non-zero.
        midnight = 1700001600  # any aligned-ish ts; we'll pretend it's local midnight
        # Two minutes inside the "day at midnight - 0":
        for minute_ts, mwh in ((midnight + 60, 5000.0), (midnight + 120, 3000.0)):
            self.h._db.execute(
                "INSERT INTO energy_minute (device, ts, energy_mwh) "
                "VALUES (?, ?, ?)", ("k", minute_ts, mwh),
            )
        self.h._db.commit()
        days = self.h.daily_energy_kwh("k", midnight, days=5)
        self.assertEqual(len(days), 5)
        # The most recent bucket (today) holds the consumption.
        # 5000 + 3000 mWh = 8 Wh
        self.assertAlmostEqual(days[-1][1], 8.0)
        # Earlier buckets are zero-padded.
        for ts, wh in days[:-1]:
            self.assertEqual(wh, 0.0)

    def test_record_status_idempotent(self):
        # Re-reporting the same minute is a no-op (INSERT OR REPLACE
        # against the same primary key).
        payload = {"aenergy": {"total": 100.0,
                               "by_minute": [0.0, 3500.0, 3200.0],
                               "minute_ts": 1700000060}}
        for _ in range(3):
            self.h.record_status("k", 1700000050 + 1, payload)
        rows = self.h.query_energy_minute("k", 0, 2_000_000_000)
        self.assertEqual(len(rows), 2)  # not 6

    def test_energy_consumed_in_prefers_authoritative(self):
        # When energy_minute has data, it wins over power_minute integration.
        # Insert minute_ts=1700000060 → by_minute[1]=10000mWh covers
        # minute starting 1700000000.
        payload = {"aenergy": {"by_minute": [0.0, 10000.0, 0.0],
                                "minute_ts": 1700000060}}
        self.h.record_status("k", 1700000050, payload)
        # Also insert a competing power_minute row that would yield a
        # different number — to prove we read from energy_minute, not
        # power_minute.
        with self.h._lock:
            self.h._db.execute(
                "INSERT INTO power_minute (device, ts, avg_apower_w, sample_count) "
                "VALUES ('k', 1700000000, 1.0, 1)"
            )
            self.h._db.commit()
        wh, _earliest = self.h.energy_consumed_in("k", 1699999990, 1700000060)
        # Authoritative: 10000 mWh = 10 Wh
        self.assertAlmostEqual(wh, 10.0)

    def test_query_power_raw(self):
        self.h.write_sample("k", 60,  10.0, None, output=True)
        self.h.write_sample("k", 70,  20.0, None, output=True)  # same minute
        self.h.write_sample("k", 130, 30.0, None, output=False) # next minute
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


if __name__ == "__main__":
    unittest.main()
