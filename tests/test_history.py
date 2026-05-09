"""Tests for the SQLite time series: samples_raw / power_minute /
aenergy_offset_events. Covers per-minute aggregation flush, max/
min tracking, retention, the legacy-schema migration that backfills
max_apower_w / min_apower_w from samples_raw, and the energy-query
math (aenergy_at, energy_consumed_in, daily_energy_kwh) including
counter-reset offset handling."""

import sqlite3 as _sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import history as history_mod


class TestHistory(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.h = history_mod.History(self.tmp / "h.sqlite", retention_days=0)

    def tearDown(self):
        self.h.close()

    def test_write_sample_buffers_until_minute_rolls(self):
        # query_power tuples: (ts, min_w, max_w, avg_w, output)
        self.h.write_sample("kaffeete", 1000, 5.0, output=True)
        self.h.write_sample("kaffeete", 1010, 15.0, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(pts, [])
        # Cross minute boundary → previous minute flushes
        self.h.write_sample("kaffeete", 1080, 100.0, output=True)
        bucket, pts = self.h.query_power("kaffeete", 0, 2000)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][0], 960)              # 1000 // 60 * 60
        self.assertAlmostEqual(pts[0][1], 5.0)        # min
        self.assertAlmostEqual(pts[0][2], 15.0)       # max
        self.assertAlmostEqual(pts[0][3], 10.0)       # avg
        self.assertEqual(pts[0][4], 1)                # output=on

    def test_max_tracked_alongside_avg(self):
        # Three samples within the same minute window [60, 120).
        # 5W, 200W, 10W → min=5, max=200, avg ≈ 71.67.
        self.h.write_sample("k", 65, 5.0, output=True)
        self.h.write_sample("k", 90, 200.0, output=True)
        self.h.write_sample("k", 115, 10.0, output=True)
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertAlmostEqual(pts[0][1], 5.0)        # min
        self.assertAlmostEqual(pts[0][2], 200.0)      # max
        self.assertAlmostEqual(pts[0][3], 215.0 / 3, places=2)  # avg

    def test_output_off_persisted(self):
        self.h.write_sample("k", 100, 0.0, output=False)
        self.h.write_sample("k", 110, 0.1, output=False)
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertEqual(pts[0][4], 0)                # output=off

    def test_output_unknown_when_never_reported(self):
        self.h.write_sample("k", 100, 5.0)  # no output kwarg
        self.h.flush_pending_minutes(now=200)
        _, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertIsNone(pts[0][4])                  # output unknown

    def test_flush_pending_minutes(self):
        self.h.write_sample("k", 100, 7.0, output=True)
        self.h.write_sample("k", 110, 13.0, output=True)
        self.h.flush_pending_minutes(now=200)
        bucket, pts = self.h.query_power("k", 0, 200)
        self.assertEqual(len(pts), 1)
        self.assertAlmostEqual(pts[0][1], 7.0)        # min
        self.assertAlmostEqual(pts[0][2], 13.0)       # max
        self.assertAlmostEqual(pts[0][3], 10.0)       # avg
        self.assertEqual(pts[0][4], 1)

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

    def test_legacy_schema_migration_backfills_max(self):
        """Cold-start on a db from before max_apower_w existed: ALTER runs
        and the backfill query fills max from samples_raw."""
        legacy_path = self.tmp / "legacy.sqlite"
        db = _sqlite3.connect(str(legacy_path))
        # Legacy schema: power_minute without max_apower_w.
        db.executescript(
            "CREATE TABLE samples_raw ("
            "  device TEXT, ts INTEGER, apower_w REAL, voltage_v REAL,"
            "  current_a REAL, freq_hz REAL, aenergy_total_wh REAL,"
            "  output INTEGER, temperature_c REAL, payload_json TEXT,"
            "  PRIMARY KEY (device, ts)"
            ");"
            "CREATE TABLE power_minute ("
            "  device TEXT NOT NULL, ts INTEGER NOT NULL,"
            "  avg_apower_w REAL NOT NULL, sample_count INTEGER NOT NULL,"
            "  output INTEGER, PRIMARY KEY (device, ts)"
            ");"
            "CREATE TABLE aenergy_offset_events ("
            "  device TEXT, ts INTEGER, delta_wh REAL,"
            "  PRIMARY KEY (device, ts)"
            ");"
        )
        # Samples in [60, 120): 5W, 200W, 50W → expected max = 200.
        for ts, w in [(65, 5.0), (90, 200.0), (115, 50.0)]:
            db.execute(
                "INSERT INTO samples_raw (device, ts, apower_w) "
                "VALUES ('k', ?, ?)", (ts, w),
            )
        # Legacy power_minute row carries only the avg.
        db.execute(
            "INSERT INTO power_minute (device, ts, avg_apower_w, sample_count) "
            "VALUES ('k', 60, 85.0, 3)",
        )
        db.commit()
        db.close()

        # Instantiate History → triggers ALTER + backfill for both
        # max_apower_w and min_apower_w.
        h = history_mod.History(legacy_path, retention_days=0)
        try:
            cols = {r[1] for r in h._db.execute(
                "PRAGMA table_info(power_minute)")}
            self.assertIn("max_apower_w", cols)
            self.assertIn("min_apower_w", cols)
            row = h._db.execute(
                "SELECT avg_apower_w, max_apower_w, min_apower_w "
                "FROM power_minute WHERE device='k' AND ts=60"
            ).fetchone()
            self.assertAlmostEqual(row[0], 85.0)   # avg untouched
            self.assertAlmostEqual(row[1], 200.0)  # max backfilled
            self.assertAlmostEqual(row[2], 5.0)    # min backfilled
        finally:
            h.close()

    def _put_aenergy(self, device, ts, total_wh):
        """Test helper: write a raw aenergy_total_wh row to samples_raw
        via record_status (the production write path)."""
        self.h.record_status(device, ts, {"aenergy": {"total": total_wh}})

    def test_aenergy_at_via_samples_raw(self):
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
        self.assertIsNone(self.h.aenergy_at("k", 99))
        self.assertEqual(self.h.aenergy_at("k", 150), 1000.0)
        self.assertEqual(self.h.aenergy_at("k", 250), 450.0)
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
        wh, earliest = self.h.energy_consumed_in("k", 99, 350)
        self.assertAlmostEqual(wh, 150.0, places=3)
        self.assertEqual(earliest, 100)   # earliest available sample

    def test_energy_consumed_in_delta_of_total(self):
        # Two cumulative readings: 1000 Wh at t1, 1500 Wh at t2.
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


if __name__ == "__main__":
    unittest.main()
