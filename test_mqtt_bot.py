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
import scheduler as sched
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


# --- Scheduler integration: timer fires + cancel by manual action --------

class TestSchedulerJob(unittest.TestCase):
    def test_cancel_returns_jobs(self):
        fires = []
        s = sched.Scheduler(on_fire=lambda *a: fires.append(a))
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=int(time.time()) + 3600, _time_mode="timer",
        )
        s.schedule(job)
        cancelled = s.cancel("kitchen", target_action="off")
        self.assertEqual(len(cancelled), 1)
        # second cancel returns empty
        self.assertEqual(s.cancel("kitchen", target_action="off"), [])

    def test_cancel_only_matching_action(self):
        s = sched.Scheduler(on_fire=lambda *a: None)
        s.schedule(sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=int(time.time()) + 3600))
        s.schedule(sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="on",
            deadline_ts=int(time.time()) + 3600))
        # cancel only "off" should leave the "on" job alone
        cancelled = s.cancel("kitchen", target_action="off")
        self.assertEqual(len(cancelled), 1)
        remaining = s.jobs_for_device("kitchen")
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0].target_action, "on")

    def test_idle_fires_when_below_for_long_enough(self):
        fires = []
        s = sched.Scheduler(on_fire=lambda *a: fires.append(a))
        now = int(time.time())
        job = sched.ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
        )
        s.schedule(job)
        # First tick at t0: power 1 → just below; below_since set to now
        s.tick("kitchen", {"apower": 1.0}, now=now)
        self.assertEqual(fires, [])
        # Tick 30s later: still below, but only 30s elapsed
        s.tick("kitchen", {"apower": 1.0}, now=now + 30)
        self.assertEqual(fires, [])
        # Tick 70s later: 70 >= 60 → fires
        s.tick("kitchen", {"apower": 1.0}, now=now + 70)
        self.assertEqual(len(fires), 1)
        device, chat, target, mode, ctx = fires[0]
        self.assertEqual(device, "kitchen")
        self.assertEqual(target, "off")
        self.assertEqual(mode, "idle")

    def test_idle_fire_includes_threshold_and_duration_human(self):
        fires = []
        s = sched.Scheduler(on_fire=lambda *a: fires.append(a))
        now = int(time.time())
        s.schedule(sched.ScheduledJob(
            device_name="k", chat_id_origin=1, target_action="off",
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
        ))
        s.tick("k", {"apower": 1.0}, now=now)
        s.tick("k", {"apower": 1.0}, now=now + 65)
        self.assertEqual(len(fires), 1)
        _, _, _, _, ctx = fires[0]
        self.assertEqual(ctx["threshold"], 5.0)
        self.assertEqual(ctx["seconds"], 65)
        self.assertEqual(ctx["duration_human"], "1m5s")

    def test_consumed_fire_includes_threshold_and_window_human(self):
        fires = []
        s = sched.Scheduler(on_fire=lambda *a: fires.append(a))
        now = int(time.time())
        s.schedule(sched.ScheduledJob(
            device_name="k", chat_id_origin=1, target_action="off",
            consumed_field="apower", consumed_threshold_wh=5.0,
            consumed_window_s=120, _consumed_started_at=now - 200,
        ))
        # apower=1 W constant for 120s ≈ 0.033 Wh, well below 5 Wh
        s.tick("k", {"apower": 1.0}, now=now)
        s.tick("k", {"apower": 1.0}, now=now + 1)
        self.assertEqual(len(fires), 1)
        _, _, _, _, ctx = fires[0]
        self.assertEqual(ctx["threshold"], 5.0)
        self.assertEqual(ctx["seconds"], 120)
        self.assertEqual(ctx["window_human"], "2m")

    def test_idle_resets_when_above(self):
        fires = []
        s = sched.Scheduler(on_fire=lambda *a: fires.append(a))
        now = int(time.time())
        s.schedule(sched.ScheduledJob(
            device_name="k", chat_id_origin=1, target_action="off",
            idle_field="apower", idle_threshold=5.0, idle_duration_s=60,
        ))
        s.tick("k", {"apower": 1.0}, now=now)
        s.tick("k", {"apower": 100.0}, now=now + 30)  # spike — resets
        s.tick("k", {"apower": 1.0}, now=now + 60)    # 60s still not enough since reset
        self.assertEqual(fires, [])


# --- engine integration ---------------------------------------------------
#
# These tests stub deltachat2.MsgData (engine imports it at module level)
# so we don't need the real package installed. We also use stub objects
# for bot.rpc, mqtt, webxdc, scheduler so we can observe what the engine
# tries to do without any network or thread.

import types as _types

_deltachat_stub = _types.ModuleType("deltachat2")
class _MsgData:
    def __init__(self, text=None, file=None):
        self.text = text
        self.file = file
_deltachat_stub.MsgData = _MsgData
sys.modules.setdefault("deltachat2", _deltachat_stub)

import engine as engine_mod  # noqa: E402  (after sys.modules patch)


class _StubMqtt:
    def __init__(self):
        self.published: list[tuple[str, str]] = []
    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))


class _StubWebxdc:
    def __init__(self):
        self.pushes = 0
    def push_filtered(self, *_a, **_kw):
        self.pushes += 1
    def class_for_msgid(self, *_a):
        return None


class _StubScheduler:
    def __init__(self):
        self._jobs: dict[tuple[str, str], object] = {}
        self.cancel_calls: list[tuple[str, str | None]] = []
    def schedule(self, job):
        self._jobs[(job.device_name, job.target_action)] = job
    def cancel(self, device_name, target_action=None):
        self.cancel_calls.append((device_name, target_action))
        out = []
        for k in list(self._jobs.keys()):
            if k[0] != device_name:
                continue
            if target_action is not None and k[1] != target_action:
                continue
            out.append(self._jobs.pop(k))
        return out
    def jobs_for_device(self, device_name):
        return [j for k, j in self._jobs.items() if k[0] == device_name]
    def all_jobs(self):
        return list(self._jobs.values())
    def tick(self, *_a, **_kw):
        pass


class _StubBotRpc:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.reactions: list[tuple[int, list[str]]] = []
    def send_msg(self, _accid, chat_id, msg):
        self.sent.append((chat_id, msg.text))
    def send_reaction(self, _accid, msgid, emojis):
        self.reactions.append((msgid, emojis))


class _StubBot:
    def __init__(self):
        self.rpc = _StubBotRpc()
        self.logger = logging.getLogger("test")


import logging  # noqa: E402
from pathlib import Path as _P  # noqa: E402


def _build_engine_with_class(class_overrides=None, device_overrides=None):
    """Build an Engine wired to stubs against a temp config dir."""
    tmp = _P(tempfile.mkdtemp())
    cls_def = json.loads(json.dumps(CLASS_JSON_OK))  # deep copy
    if class_overrides:
        cls_def.update(class_overrides)
    cls_dir = tmp / "devices" / "tplug"
    cls_dir.mkdir(parents=True)
    (cls_dir / "class.json").write_text(json.dumps(cls_def))
    instance = {
        "devices": [{
            "name": "kitchen", "class": "tplug",
            "topic_prefix": "p/kitchen", "allowed_chats": [12],
            "power_threshold_watts": 1500,
            "power_threshold_duration_s": 30,
        }],
    }
    if device_overrides:
        instance["devices"][0].update(device_overrides)
    (tmp / "devices.json").write_text(json.dumps(instance))
    cfg = cfg_mod.load(devices_dir=tmp / "devices",
                       instances_file=tmp / "devices.json")
    e = engine_mod.Engine(
        cfg=cfg,
        allowed_chats={12},
        mqtt=_StubMqtt(),
        webxdc=_StubWebxdc(),
        scheduler=_StubScheduler(),
        client_id="bot",
    )
    e.set_bot(_StubBot(), accid=1)
    return e


class TestEngineDispatch(unittest.TestCase):
    def test_unknown_device(self):
        e = _build_engine_with_class()
        ok, msg = e.dispatch_command(12, "ghost", "on")
        self.assertFalse(ok)
        self.assertIn("unknown", msg)

    def test_permission_denied_for_chat_not_in_device_list(self):
        e = _build_engine_with_class()
        ok, msg = e.dispatch_command(99, "kitchen", "on")
        self.assertFalse(ok)
        self.assertEqual(msg, "permission denied")

    def test_unknown_action(self):
        e = _build_engine_with_class()
        ok, msg = e.dispatch_command(12, "kitchen", "explode")
        self.assertFalse(ok)
        self.assertIn("unknown action", msg)

    def test_publish_on_success(self):
        e = _build_engine_with_class()
        ok, _ = e.dispatch_command(12, "kitchen", "on")
        self.assertTrue(ok)
        self.assertEqual(e.mqtt.published, [("p/kitchen/command/switch:0", "on")])

    def test_template_substitution(self):
        e = _build_engine_with_class()
        ok, _ = e.dispatch_command(12, "kitchen", "status")
        self.assertTrue(ok)
        topic, payload = e.mqtt.published[0]
        self.assertEqual(topic, "p/kitchen/rpc")
        self.assertIn('"src":"bot"', payload)

    def test_manual_off_cancels_pending_auto_off(self):
        e = _build_engine_with_class()
        # Pre-seed a fake auto-off job
        from scheduler import ScheduledJob
        e.scheduler.schedule(ScheduledJob(
            device_name="kitchen", chat_id_origin=12, target_action="off",
            deadline_ts=int(time.time()) + 600,
        ))
        e.dispatch_command(12, "kitchen", "off")
        # cancel should have been called for action="off"
        self.assertEqual(e.scheduler.cancel_calls,
                         [("kitchen", "off")])
        # cancellation chat message should have been posted
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("cancelled" in (t or "").lower() for t in sent),
                        f"expected cancelled message; got {sent}")


class TestEngineThreshold(unittest.TestCase):
    def test_threshold_fires_after_sustained_above(self):
        e = _build_engine_with_class()
        device = e.cfg.devices["kitchen"]
        # Drive power above threshold for >= duration via repeated on_message.
        # We simulate two updates: first sets above_since, second exceeds duration.
        cls = e.cfg.device_class(device)
        # Seed via on_mqtt_message — pretend the plug published a status JSON.
        topic = "p/kitchen/status/switch:0"
        e.on_mqtt_message(topic, json.dumps({"output": True, "apower": 2000}).encode())
        # Inject above_since back in time so the second message exceeds duration.
        ts = e._thresholds[("kitchen", "apower")]
        ts.above_since = int(time.time()) - 31  # > 30s threshold duration
        e.on_mqtt_message(topic, json.dumps({"output": True, "apower": 2100}).encode())
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("drew" in (t or "") for t in sent),
                        f"expected ⚠️ drew message; got {sent}")

    def test_threshold_clears(self):
        e = _build_engine_with_class()
        topic = "p/kitchen/status/switch:0"
        # Trip the alert
        e.on_mqtt_message(topic, json.dumps({"apower": 2000}).encode())
        e._thresholds[("kitchen", "apower")].above_since = int(time.time()) - 31
        e.on_mqtt_message(topic, json.dumps({"apower": 2100}).encode())
        self.assertTrue(e._thresholds[("kitchen", "apower")].active)
        # Drop below
        e.on_mqtt_message(topic, json.dumps({"apower": 5}).encode())
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("cleared" in (t or "") for t in sent),
                        f"expected ✅ cleared message; got {sent}")
        self.assertFalse(e._thresholds[("kitchen", "apower")].active)


class TestEngineOnFire(unittest.TestCase):
    def test_on_fire_publishes_and_posts(self):
        e = _build_engine_with_class()
        e.on_fire("kitchen", chat_id_origin=12,
                  target_action="off", mode="timer", ctx={})
        self.assertEqual(e.mqtt.published, [("p/kitchen/command/switch:0", "off")])
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("auto-off (timer)" in (t or "") for t in sent),
                        f"expected timer message; got {sent}")

    def test_on_fire_idle_uses_idle_template(self):
        e = _build_engine_with_class()
        e.on_fire("kitchen", chat_id_origin=12, target_action="off",
                  mode="idle", ctx={"value": 1.5, "seconds": 60, "field": "apower"})
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("auto-off (idle" in (t or "") for t in sent),
                        f"expected idle message; got {sent}")

    def test_on_fire_threads_action_verb_into_template(self):
        # Build a class with a template that uses {action_verb} + {threshold}
        # + {duration_human} so we can prove the enriched ctx flows.
        cls = json.loads(json.dumps(CLASS_JSON_OK))
        cls["auto_off"]["trigger_messages"]["consumed"] = (
            "{name} consumed {value:.2f} Wh < {threshold:.1f} Wh "
            "in last {window_human}; {action_verb}"
        )
        e = _build_engine_with_class(class_overrides=cls)
        e.on_fire("kitchen", chat_id_origin=12, target_action="off",
                  mode="consumed",
                  ctx={"value": 3.21, "threshold": 5.0, "seconds": 600,
                       "window_human": "10m", "field": "apower"})
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(
            any("3.21 Wh < 5.0 Wh in last 10m; switching off" in (t or "")
                for t in sent),
            f"expected enriched consumed message; got {sent}")


class TestEngineSnapshot(unittest.TestCase):
    def test_snapshot_for_visible_chat(self):
        e = _build_engine_with_class()
        snap = e.snapshot_for(12, "tplug")
        self.assertIsNotNone(snap)
        self.assertEqual(snap["class"], "tplug")
        self.assertIn("kitchen", snap["devices"])

    def test_snapshot_filters_invisible_chat(self):
        e = _build_engine_with_class()
        # chat 99 is not in ALLOWED_CHATS and not in device.allowed_chats
        self.assertIsNone(e.snapshot_for(99, "tplug"))

    def test_snapshot_includes_params_when_history_present(self):
        e = _build_engine_with_class()
        e.history = history_mod.History(
            Path(tempfile.mkdtemp()) / "h.sqlite", retention_days=0)
        try:
            snap = e.snapshot_for(12, "tplug")
            params = snap["devices"]["kitchen"].get("params") or {}
            self.assertEqual(params.get("power_threshold_watts"), 1500)
        finally:
            e.history.close()


class TestEngineSetParam(unittest.TestCase):
    def _make(self):
        e = _build_engine_with_class()
        # The webxdc class lookup is short-circuited; engine's set_param
        # path runs without it.
        e.webxdc = _StubWebxdc()
        return e

    def test_override_applied(self):
        e = self._make()
        e._handle_set_param_request(
            12, "kitchen",
            {"param": "power_threshold_watts", "value": 200},
        )
        self.assertEqual(
            e._param_overrides["kitchen"]["power_threshold_watts"], 200)

    def test_override_rejects_unknown_param(self):
        e = self._make()
        e._handle_set_param_request(
            12, "kitchen", {"param": "nope", "value": 42})
        self.assertNotIn("kitchen", e._param_overrides)

    def test_override_rejects_negative(self):
        e = self._make()
        e._handle_set_param_request(
            12, "kitchen",
            {"param": "power_threshold_watts", "value": -5})
        self.assertNotIn("kitchen", e._param_overrides)

    def test_override_overrides_threshold_evaluation(self):
        e = self._make()
        # Override to a low limit so a small power triggers the rule.
        e._handle_set_param_request(
            12, "kitchen",
            {"param": "power_threshold_watts", "value": 50},
        )
        e._handle_set_param_request(
            12, "kitchen",
            {"param": "power_threshold_duration_s", "value": 1},
        )
        # Run two on_mqtt_message calls with apower=100 across the duration
        topic = "p/kitchen/status/switch:0"
        e.on_mqtt_message(topic, json.dumps({"output": True, "apower": 100}).encode())
        ts_state = e._thresholds[("kitchen", "apower")]
        ts_state.above_since = int(time.time()) - 5  # past the 1s threshold
        e.on_mqtt_message(topic, json.dumps({"output": True, "apower": 100}).encode())
        sent = [t for _, t in e.bot.rpc.sent]
        self.assertTrue(any("drew" in (t or "") for t in sent),
                        f"expected ⚠️ drew message; got {sent}")


# --- history -------------------------------------------------------------

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

    def test_write_event_extracts_method(self):
        self.h.write_event("k", 100, "events/rpc",
                           '{"method":"NotifyStatus","params":{}}')
        rows = self.h.query_events("k", 0, 1000)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "NotifyStatus")  # kind

    def test_write_event_handles_bad_json(self):
        self.h.write_event("k", 100, "events/rpc", "not json")
        rows = self.h.query_events("k", 0, 1000)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "")  # kind is empty for non-JSON

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
