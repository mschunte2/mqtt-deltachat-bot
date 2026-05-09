"""End-to-end integration tests:

- ``TestTasmotaClass``: validates the engine is genuinely class-
  agnostic. Loads the real ``devices/tasmota_plug/class.json``,
  feeds Tasmota-shaped MQTT payloads through a PlugTwin, and
  asserts state extraction + dispatch + snapshot all work without
  any Python changes. The whole point of the declarative class
  model is that adding a device type is config-only; this test
  guards that.
- ``TestIntegrationRoutingChain``: drives MQTT topic → registry →
  twin → state edge → publisher.broadcast → snapshot.build_for_chat
  → send. Catches wiring bugs that no single-module test would
  (e.g. the v0.2 deploy that pushed snapshots the app couldn't
  read).
- ``TestColdStartIntegration``: empty SQLite + empty
  baselines.json + a fresh PlugTwin. The first MQTT status update
  should land in samples_raw, populate twin.fields["aenergy"]
  (RAW), and result in a snapshot whose Lifetime / Counter /
  kwh_today numbers are sourced via history.aenergy_at (with no
  offsets recorded). Guards the happy path that v0.2.x users hit
  on their first deploy.
"""

import json
import tempfile
import time
import unittest
from pathlib import Path

from mqtt_bot.util import config as cfg_mod
from mqtt_bot.io import history as history_mod
from mqtt_bot.core import twin as plug_mod
from mqtt_bot.io.publisher import Publisher
from mqtt_bot.core.snapshot import build_for_chat
from mqtt_bot.core.twins import TwinRegistry

from tests._fixtures import _build_twin


HERE = Path(__file__).resolve().parent.parent  # project root


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
        # The class config uses suffix "LWT" with extract:"bool_text".
        # bool_text accepts "true|1|on" and "false|0|off"; anything else
        # silently skips. Tasmota's "Online"/"Offline" payload doesn't
        # match those keywords by default — the user has Tasmota
        # configured to publish "true"/"false" (a 1-line SetOption).
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

        # Forward-refs resolved at call time via closure.
        self.registry: "TwinRegistry | None" = None
        self.publisher: "Publisher | None" = None
        twins = []
        for d in cfg.devices.values():
            cls_name = d.class_name
            deps = plug_mod.TwinDeps(
                mqtt_publish=lambda t, p: None,
                post_to_chats=lambda dev, txt: None,
                # Mirror bot._publisher_broadcast: filter by class so an
                # edge on one class doesn't churn another class's apps.
                # Capture class via default-arg trick.
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
        self.registry = TwinRegistry(twins)
        # Two chats know about apps for both classes.
        msgid_map = {
            12: {"shelly_plug": 1001, "tasmota_plug": 1002},
            14: {"tasmota_plug": 2002},
        }
        self.publisher = Publisher(
            build=lambda chat, cls: build_for_chat(chat, cls, self.registry, set()),
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
            self.assertEqual(set(payload.keys()),
                             {"class", "server_ts", "devices"})
            self.assertIn("lamp", payload["devices"])
            dev = payload["devices"]["lamp"]
            self.assertIn("fields", dev)
            self.assertIn("scheduled_jobs", dev)


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

        # twin.fields["aenergy"] holds the RAW counter.
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


if __name__ == "__main__":
    unittest.main()
