"""Tests for baselines.json round-trip + legacy aenergy_offset_wh
migration. Exercises the persistence-side of the resettable
Counter feature."""

import json
import tempfile
import unittest
from pathlib import Path

from mqtt_bot.io import baselines as baselines_mod
from mqtt_bot.io import history as history_mod
from mqtt_bot.core import twin as plug_mod
from mqtt_bot.core.twins import TwinRegistry

from tests._fixtures import _build_twin


class TestBaselinesPersistence(unittest.TestCase):
    """The user-Counter state (baseline_wh, reset_at_ts) round-trips
    through baselines.json. Hardware-counter-reset offsets are NOT
    in this file in v0.2+; if a legacy `aenergy_offset_wh` entry
    appears, it migrates to a single aenergy_offset_events row at
    ts=0 (idempotent)."""

    def setUp(self):
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
        self.registry = TwinRegistry([twin])
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


if __name__ == "__main__":
    unittest.main()
