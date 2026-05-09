"""Tests for snapshot.build_for_chat: visibility filtering, the
contract that locks the bot↔app payload shape, and the energy-
summary math (kwh_since_reset, kwh_last_*)."""

import json
import tempfile
import time
import unittest
from pathlib import Path

from mqtt_bot.core import twin as plug_mod
from mqtt_bot.core import snapshot as snap_mod
from mqtt_bot.io import webxdc_io as wio
from mqtt_bot.core.twins import TwinRegistry

from tests._fixtures import _build_twin, _FakeHistory


class TestSnapshot(unittest.TestCase):
    def test_visible_devices_only(self):
        twin, _, _ = _build_twin(allowed_chats=(12,))
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


class TestKwhSinceResetMath(unittest.TestCase):
    def test_baseline_subtracted_correctly(self):
        from mqtt_bot.core.snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=12345.0,
                            baseline_wh=345.0,
                            reset_at_ts=int(time.time()))
        self.assertAlmostEqual(e["kwh_since_reset"], 12.0, places=6)
        self.assertEqual(e["current_total_wh"], 12345.0)  # Lifetime untouched

    def test_clamped_to_zero_on_rollover(self):
        # Plug counter rollover or a stale baseline shouldn't show negative.
        from mqtt_bot.core.snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=10.0, baseline_wh=100.0,
                            reset_at_ts=None)
        self.assertEqual(e["kwh_since_reset"], 0.0)

    def test_none_when_no_current_reading(self):
        from mqtt_bot.core.snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=None, baseline_wh=0.0,
                            reset_at_ts=None)
        self.assertIsNone(e["kwh_since_reset"])

    def test_includes_kwh_last_365d(self):
        from mqtt_bot.core.snapshot import _energy_summary
        e = _energy_summary(_FakeHistory(), "x",
                            current_wh=100.0, baseline_wh=0.0,
                            reset_at_ts=None)
        self.assertIn("kwh_last_365d", e)
        self.assertEqual(set(e["kwh_last_365d"].keys()),
                         {"kwh", "partial_since_ts"})


# --- Snapshot protocol contract ------------------------------------------
#
# Locks the bot↔app payload shape. A protocol drift on either side
# (renamed key, missing field, wrong nesting level) fails the test
# instead of leaving the UI silently blank — which is the exact bug
# we hit when first deploying v0.2.0.

class TestSnapshotContract(unittest.TestCase):
    def setUp(self):
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
        self.registry = TwinRegistry([twin])

    def test_top_level_keys(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        self.assertIsNotNone(snap)
        # Snapshot is at the top level of payload — NO `snapshot:` wrapper.
        # The app reads `payload.devices`, `payload.server_ts`, etc.
        self.assertEqual(set(snap.keys()), {"class", "server_ts", "devices"})
        self.assertEqual(snap["class"], "tplug")
        self.assertIsInstance(snap["server_ts"], int)

    def test_per_device_keys(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        dev = snap["devices"]["kitchen"]
        # Every key the app reads. If you add/rename one in plug.py.to_dict,
        # update both here AND devices/<class>/app/main.js.
        for k in ("name", "description", "fields", "last_update_ts",
                  "scheduled_jobs", "params", "energy",
                  "daily_energy_wh", "power_history"):
            self.assertIn(k, dev, f"missing top-level device key: {k}")
        # power_history shape — three resolutions.
        self.assertEqual(set(dev["power_history"].keys()),
                          {"minute", "hour", "day"})

    def test_power_history_tuple_shape(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        ph = snap["devices"]["kitchen"]["power_history"]
        # Each entry is [ts, min_w, max_w, avg_w, output].
        for series in (ph["minute"], ph["hour"], ph["day"]):
            for entry in series[:5]:  # first 5 are enough
                self.assertEqual(len(entry), 5)
                self.assertIsInstance(entry[0], int)
                self.assertTrue(entry[1] is None or isinstance(entry[1], (int, float)))
                self.assertTrue(entry[2] is None or isinstance(entry[2], (int, float)))
                self.assertIsInstance(entry[3], (int, float))
                self.assertIn(entry[4], (0, 1, None))

    def test_power_history_minute_has_live_tail(self):
        # The right edge of the minute series should reflect the twin's
        # live apower (42.0 W in setUp), not the gap-filled zero of the
        # in-flight current minute. Live point: min == max == avg.
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        minute = snap["devices"]["kitchen"]["power_history"]["minute"]
        self.assertGreater(len(minute), 0)
        last = minute[-1]
        self.assertAlmostEqual(last[1], 42.0)  # min
        self.assertAlmostEqual(last[2], 42.0)  # max
        self.assertAlmostEqual(last[3], 42.0)  # avg
        self.assertEqual(last[4], 1)           # output=on

    def test_power_history_day_series_size(self):
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        day = snap["devices"]["kitchen"]["power_history"]["day"]
        # 365 days at 1-day buckets — gap-filled with zeros for the
        # _FakeHistory which returns no rows.
        self.assertGreaterEqual(len(day), 364)
        self.assertLessEqual(len(day), 366)
        # Bucket size is 86400 s.
        if len(day) >= 2:
            self.assertEqual(day[1][0] - day[0][0], 86400)

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

        tmp = Path(tempfile.mkdtemp())
        io = wio.WebxdcIO(state_dir=tmp, devices_dir=tmp / "devices")
        snap = snap_mod.build_for_chat(12, "tplug", self.registry, set())
        ok = io.push_to_msgid(_StubBot(), 0, 99, snap)
        self.assertTrue(ok)
        body = json.loads(captured[0])
        # Single `payload` wrapper, snapshot keys at the top level
        # under it (NO extra `snapshot:` nesting).
        self.assertEqual(set(body.keys()), {"payload"})
        self.assertEqual(set(body["payload"].keys()), {"class", "server_ts", "devices"})
        self.assertIn("kitchen", body["payload"]["devices"])


if __name__ == "__main__":
    unittest.main()
