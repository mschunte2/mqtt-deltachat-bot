"""Time-series storage for power consumption.

Single SQLite file under the bot's config dir. Tables:

  samples_raw    — every status/switch:0 captured verbatim
                   (lossless audit trail; raw aenergy_total_wh).
                   The single source of truth for energy queries.
  power_minute   — average apower (W) per minute per device
                   (used for the app's power chart + rule rehydration)
  aenergy_offset_events — append-only log of detected hardware
                   counter resets (typically empty)

Energy "kWh consumed in [a, b]" is computed as the difference
between two effective-aenergy readings:

    effective_at(t) = raw_at_or_before(t)
                    + Σ delta_wh from aenergy_offset_events WHERE ts ≤ t

Both lookups are O(log N) index seeks against samples_raw; the
offset SUM is over a tiny table (typically 0 rows). For 99% of users
the math collapses to `raw_at_or_before(b) - raw_at_or_before(a)`.

Why offsets exist: when the plug's hardware aenergy.total counter
goes backwards (manufacturer reset, plug replacement, firmware
glitch), PlugTwin.on_mqtt detects the drop and appends a row to
aenergy_offset_events. Subsequent queries add the cumulative offset
so the lifetime series stays continuous across the discontinuity.
samples_raw stores the RAW counter (untouched) so CSV exports stay
1:1 with the plug's webUI.

Retention: a single RETENTION_DAYS knob applies to samples_raw +
power_minute. aenergy_offset_events is forever (tiny + load-bearing
for energy queries spanning a reset).
  - 0       → keep forever
  - >0      → delete rows older than N days, once per day

Single shared connection guarded by a threading.Lock; writes serialize
on the lock, which is fine for the few-writes-per-minute traffic this
bot generates.

(Earlier versions had `events`, `energy_minute`, `aenergy_minute`,
and `energy_hour` tables; all dropped from the code. Existing rows
in users' SQLite files are dead data the bot never reads.)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict
from pathlib import Path

log = logging.getLogger("mqtt_bot.history")

_SCHEMA = """
-- Every status/switch:0 message, captured verbatim. Lossless audit
-- trail and the single source of truth for energy queries:
-- aenergy_total_wh holds the RAW plug counter on every sample.
CREATE TABLE IF NOT EXISTS samples_raw (
  device           TEXT    NOT NULL,
  ts               INTEGER NOT NULL,
  apower_w         REAL,
  voltage_v        REAL,
  current_a        REAL,
  freq_hz          REAL,
  aenergy_total_wh REAL,
  output           INTEGER,
  temperature_c    REAL,
  payload_json     TEXT,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_samples_raw_ts ON samples_raw (ts);

-- Per-minute apower average (computed from raw samples). Kept for the
-- power chart in the app and for rule rehydration.
CREATE TABLE IF NOT EXISTS power_minute (
  device       TEXT    NOT NULL,
  ts           INTEGER NOT NULL,
  avg_apower_w REAL    NOT NULL,
  sample_count INTEGER NOT NULL,
  output       INTEGER,
  max_apower_w REAL,
  min_apower_w REAL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_power_minute_ts ON power_minute (ts);

-- Append-only log of detected hardware counter resets. Each row's
-- delta_wh is added to every effective_at(T) lookup where T >= ts.
-- Forever-retained (typically 0 rows; never bigger than KB).
CREATE TABLE IF NOT EXISTS aenergy_offset_events (
  device   TEXT    NOT NULL,
  ts       INTEGER NOT NULL,
  delta_wh REAL    NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_aenergy_offset_events_ts
  ON aenergy_offset_events (ts);
"""


class History:
    def __init__(self, db_path: Path,
                 retention_days: int = 0) -> None:
        self.db_path = db_path
        # Single retention knob — applies to samples_raw + power_minute.
        # aenergy_offset_events is forever (tiny + load-bearing).
        self.retention_days = max(0, int(retention_days))
        self._lock = threading.Lock()
        # Per-device current-minute accumulator for power_minute:
        # {device: (minute_start_ts, [apower, ...], latest_output_or_None)}
        self._minute: dict[str, tuple[int, list[float], int | None]] = {}
        self._last_prune_ts = 0
        self._closed = False

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        with self._lock:
            self._db.executescript(_SCHEMA)
            # Idempotent migration: add `output` column to existing dbs that
            # were created before we tracked it.
            cols = {row[1] for row in self._db.execute("PRAGMA table_info(power_minute)")}
            if "output" not in cols:
                self._db.execute("ALTER TABLE power_minute ADD COLUMN output INTEGER")
            if "max_apower_w" not in cols:
                self._db.execute("ALTER TABLE power_minute ADD COLUMN max_apower_w REAL")
            if "min_apower_w" not in cols:
                self._db.execute("ALTER TABLE power_minute ADD COLUMN min_apower_w REAL")
            # Idempotent backfill: any NULL max/min_apower_w gets the
            # per-minute MAX/MIN from samples_raw. WHERE clauses make
            # these no-ops on subsequent boots once filled.
            backfill_max = self._db.execute(
                "UPDATE power_minute "
                "SET max_apower_w = ("
                "  SELECT MAX(s.apower_w) FROM samples_raw s "
                "  WHERE s.device = power_minute.device "
                "    AND s.ts >= power_minute.ts "
                "    AND s.ts <  power_minute.ts + 60 "
                "    AND s.apower_w IS NOT NULL"
                ") "
                "WHERE max_apower_w IS NULL"
            )
            if backfill_max.rowcount > 0:
                log.info("history: backfilled max_apower_w for %d power_minute rows",
                         backfill_max.rowcount)
            backfill_min = self._db.execute(
                "UPDATE power_minute "
                "SET min_apower_w = ("
                "  SELECT MIN(s.apower_w) FROM samples_raw s "
                "  WHERE s.device = power_minute.device "
                "    AND s.ts >= power_minute.ts "
                "    AND s.ts <  power_minute.ts + 60 "
                "    AND s.apower_w IS NOT NULL"
                ") "
                "WHERE min_apower_w IS NULL"
            )
            if backfill_min.rowcount > 0:
                log.info("history: backfilled min_apower_w for %d power_minute rows",
                         backfill_min.rowcount)
            self._db.commit()
        log.info("history db at %s (retention_days=%s)",
                 db_path,
                 "forever" if self.retention_days == 0 else self.retention_days)

    # --- writes ----------------------------------------------------------

    def write_sample(self, device: str, ts: int,
                     apower: float | None,
                     output: bool | None = None) -> None:
        """Buffer the apower reading for power_minute aggregation.
        Called on every MQTT status update via PlugTwin.on_mqtt.

        `output` is the relay state at the time of the sample. When the
        minute boundary rolls over, the LATEST output value seen during
        that minute is what gets persisted alongside the avg apower.

        No-op once the connection has been closed — late writes can
        race the SIGTERM-triggered close() while the MQTT thread is
        still draining its inbox.

        (Note: aenergy is NOT touched here. The cumulative counter
        comes in via record_status → samples_raw.aenergy_total_wh,
        which is the single source of truth for energy queries.)
        """
        if self._closed:
            return
        if apower is not None and isinstance(apower, (int, float)):
            out_int = (1 if output is True else 0 if output is False else None)
            self._buffer_apower(device, ts, float(apower), out_int)
        self._maybe_prune(ts)

    def record_offset_event(self, device: str, ts: int,
                             delta_wh: float) -> None:
        """Append one row to aenergy_offset_events. Idempotent on
        (device, ts) — INSERT OR IGNORE so a retry-storm during a
        flapping plug only records the first event."""
        if self._closed:
            return
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO aenergy_offset_events "
                "(device, ts, delta_wh) VALUES (?, ?, ?)",
                (device, int(ts), float(delta_wh)),
            )
            self._db.commit()

    def record_status(self, device: str, ts: int,
                      payload: dict[str, Any]) -> None:
        """Capture a status/switch:0 message verbatim into samples_raw.

        The raw aenergy_total_wh value the plug reported is stored
        as-is (no offset adjustment) — samples_raw is the lossless
        audit trail; the `/<dev> export Nd` CSV stays comparable
        with the plug's webUI.

        Called from PlugTwin.on_mqtt alongside write_sample. The
        per-minute aenergy snapshot (post-offset) is written via
        the separate write_aenergy_minute method.
        """
        if self._closed or not isinstance(payload, dict):
            return
        ts = int(ts)
        aenergy = payload.get("aenergy") if isinstance(payload.get("aenergy"), dict) else {}
        temperature = payload.get("temperature") if isinstance(payload.get("temperature"), dict) else {}
        try:
            payload_json = json.dumps(payload, separators=(",", ":"))
        except (TypeError, ValueError):
            payload_json = None

        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO samples_raw "
                "(device, ts, apower_w, voltage_v, current_a, freq_hz, "
                " aenergy_total_wh, output, temperature_c, payload_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    device, ts,
                    _coerce_float(payload.get("apower")),
                    _coerce_float(payload.get("voltage")),
                    _coerce_float(payload.get("current")),
                    _coerce_float(payload.get("freq")),
                    _coerce_float(aenergy.get("total")),  # RAW
                    _coerce_bool_int(payload.get("output")),
                    _coerce_float(temperature.get("tC")),
                    payload_json,
                ),
            )
            self._db.commit()

    def flush_pending_minutes(self, now: int | None = None) -> None:
        """Force-flush minute buffers regardless of boundary. Called on
        clean shutdown so an interrupted minute isn't lost."""
        now = now if now is not None else int(time.time())
        for device in list(self._minute.keys()):
            self._flush_minute(device, now=now)

    # --- reads -----------------------------------------------------------

    def query_power(self, device: str, since_ts: int, until_ts: int,
                    max_points: int = 200,
                    ) -> tuple[int, list[tuple[int, float | None, float | None, float, int | None]]]:
        if self._closed:
            return (60, [])
        """Return (bucket_seconds, [(ts, min_w, max_w, avg_w, output), ...]).

        `min_w` / `max_w` are the trough / peak apower observed in the
        bucket (None for legacy rows the migration couldn't reach —
        caller falls back to avg_w). `avg_w` is the sample-count-weighted
        mean. `output` is MAX over the bucket's minutes: 1 if on for
        any minute, 0 if every minute off, NULL if no minutes had a
        known output.
        """
        if until_ts <= since_ts:
            return (60, [])
        window = until_ts - since_ts
        bucket = max(60, _round_bucket(window // max(1, max_points)))
        with self._lock:
            cur = self._db.execute(
                "SELECT (ts/?)*?, "
                "       MIN(min_apower_w), "
                "       MAX(max_apower_w), "
                "       SUM(avg_apower_w * sample_count) / SUM(sample_count), "
                "       MAX(output) "
                "FROM power_minute "
                "WHERE device=? AND ts >= ? AND ts < ? "
                "GROUP BY ts/? "
                "ORDER BY ts ASC",
                (bucket, bucket, device, int(since_ts), int(until_ts), bucket),
            )
            rows = cur.fetchall()
        return (bucket, [
            (int(t),
             float(mn) if mn is not None else None,
             float(mx) if mx is not None else None,
             float(avg),
             int(o) if o is not None else None)
            for t, mn, mx, avg, o in rows if avg is not None
        ])

    def query_power_raw(self, device: str, since_ts: int, until_ts: int
                        ) -> list[tuple[int, float, int | None, int]]:
        """Un-bucketed minute rows for the window. Used for CSV export."""
        if self._closed:
            return []
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, avg_apower_w, output, sample_count FROM power_minute "
                "WHERE device=? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (device, int(since_ts), int(until_ts)),
            )
            rows = cur.fetchall()
        return [(int(t), float(w), int(o) if o is not None else None, int(c))
                for t, w, o, c in rows]

    def aenergy_at(self, device: str, target_ts: int) -> float | None:
        """Effective cumulative aenergy at-or-before `target_ts`, in
        post-offset Wh. Single source of truth: samples_raw, plus the
        cumulative offset sum from aenergy_offset_events.

        Returns None when no samples_raw row exists at-or-before
        target_ts for this device (e.g. asking about a window before
        the plug was first seen).
        """
        if self._closed:
            return None
        target = int(target_ts)
        with self._lock:
            raw_row = self._db.execute(
                "SELECT aenergy_total_wh FROM samples_raw "
                "WHERE device=? AND aenergy_total_wh IS NOT NULL "
                "  AND ts<=? "
                "ORDER BY ts DESC LIMIT 1",
                (device, target),
            ).fetchone()
            if raw_row is None:
                return None
            offset_row = self._db.execute(
                "SELECT COALESCE(SUM(delta_wh), 0.0) "
                "FROM aenergy_offset_events "
                "WHERE device=? AND ts<=?",
                (device, target),
            ).fetchone()
        offset = float(offset_row[0]) if offset_row else 0.0
        return float(raw_row[0]) + offset

    def energy_consumed_in(self, device: str, since_ts: int, until_ts: int
                           ) -> tuple[float, int | None]:
        """Wh consumed in the window — delta of the cumulative aenergy
        counter at the two boundaries.

            wh = total_at(until) - total_at(since)

        Storage holds absolute counter values (post-offset, so
        continuous across hardware counter resets). Two ~constant-time
        SQL queries against indexed tables; works for any window size.

        Returns (wh, earliest_ts) where earliest_ts is the timestamp
        of the lower-bound sample we used (caller flags `partial_since`
        when this is noticeably later than the requested `since_ts`).

        Edge cases:
        - No upper sample: `(0.0, None)` (no data for this device).
        - No lower sample: window starts before our data; use the
          earliest available sample as the lower bound and report its
          ts as `earliest_ts` so the caller can flag "partial".
        - upper < lower: hardware counter went backwards despite our
          offset detection (should be impossible unless a write got
          missed). Clamp to 0.
        """
        if self._closed or until_ts <= since_ts:
            return (0.0, None)
        since, until = int(since_ts), int(until_ts)
        # Fast path: latest at-or-before(until) and at-or-before(since).
        upper_wh = self.aenergy_at(device, until)
        if upper_wh is None:
            return (0.0, None)
        # Lower-bound: try at-or-before(since); if window starts
        # before our data, fall back to the earliest available sample
        # (caller flags partial via earliest_ts > since).
        lower_wh = self.aenergy_at(device, since)
        earliest_ts: int | None
        if lower_wh is not None:
            # We have a reading at-or-before(since); its ts is the
            # latest row in samples_raw at-or-before since. Look it
            # up to report earliest_ts to the caller.
            with self._lock:
                row = self._db.execute(
                    "SELECT ts FROM samples_raw "
                    "WHERE device=? AND aenergy_total_wh IS NOT NULL "
                    "  AND ts<=? ORDER BY ts DESC LIMIT 1",
                    (device, since),
                ).fetchone()
            earliest_ts = int(row[0]) if row else None
        else:
            # No row at-or-before since: use the earliest sample.
            with self._lock:
                row = self._db.execute(
                    "SELECT ts, aenergy_total_wh FROM samples_raw "
                    "WHERE device=? AND aenergy_total_wh IS NOT NULL "
                    "ORDER BY ts ASC LIMIT 1",
                    (device,),
                ).fetchone()
            if row is None:
                return (0.0, None)
            earliest_ts = int(row[0])
            # aenergy_at applies the offset SUM at this earliest ts.
            lower_wh = self.aenergy_at(device, earliest_ts)
            if lower_wh is None:
                return (0.0, None)
        wh = max(0.0, upper_wh - lower_wh)
        return (wh, earliest_ts)

    def daily_energy_kwh(self, device: str, midnight_ts: int,
                         days: int = 30) -> list[tuple[int, float]]:
        """Return [(local_midnight_ts, wh), ...] for the last `days`
        days, oldest first. Each bucket = `aenergy_at(midnight + 1d) -
        aenergy_at(midnight)` — exact daily energy, derived from the
        same cumulative counter the energy summary uses.

        midnight_ts must be the local-time midnight of TODAY (callers
        compute it via _local_midnight from snapshot.py).
        """
        if self._closed or days <= 0:
            return []
        oldest_start = midnight_ts - (days - 1) * 86400
        out: list[tuple[int, float]] = []
        prev_wh = self.aenergy_at(device, oldest_start)
        for d in range(days):
            day_start = oldest_start + d * 86400
            day_end = day_start + 86400
            end_wh = self.aenergy_at(device, day_end)
            if prev_wh is None or end_wh is None:
                out.append((day_start, 0.0))
            else:
                out.append((day_start, max(0.0, end_wh - prev_wh)))
            if end_wh is not None:
                prev_wh = end_wh
        return out

    def query_samples_raw(self, device: str, since_ts: int, until_ts: int
                           ) -> list[tuple]:
        """Lossless dump of every status update in the window."""
        if self._closed:
            return []
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, apower_w, voltage_v, current_a, freq_hz, "
                "       aenergy_total_wh, output, temperature_c "
                "FROM samples_raw "
                "WHERE device=? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (device, int(since_ts), int(until_ts)),
            )
            return cur.fetchall()

    # --- internals -------------------------------------------------------

    def _buffer_apower(self, device: str, ts: int, apower: float,
                       output: int | None) -> None:
        ts = int(ts)
        minute_start = ts - (ts % 60)
        with self._lock:
            cur = self._minute.get(device)
            if cur is None:
                self._minute[device] = (minute_start, [apower], output)
                return
            prev_start, samples, prev_output = cur
            if minute_start > prev_start:
                # Flush previous minute, start new
                if samples:
                    avg = sum(samples) / len(samples)
                    mx = max(samples)
                    mn = min(samples)
                    self._db.execute(
                        "INSERT OR REPLACE INTO power_minute "
                        "(device, ts, avg_apower_w, sample_count, output, "
                        " max_apower_w, min_apower_w) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (device, prev_start, avg, len(samples),
                         prev_output, mx, mn),
                    )
                    self._db.commit()
                self._minute[device] = (minute_start, [apower], output)
            else:
                samples.append(apower)
                # Latest known output wins; preserve previous if this sample
                # didn't include the field.
                final_output = output if output is not None else prev_output
                self._minute[device] = (prev_start, samples, final_output)

    def _flush_minute(self, device: str, now: int) -> None:
        with self._lock:
            cur = self._minute.pop(device, None)
            if cur is None:
                return
            minute_start, samples, output = cur
            if not samples:
                return
            avg = sum(samples) / len(samples)
            mx = max(samples)
            mn = min(samples)
            self._db.execute(
                "INSERT OR REPLACE INTO power_minute "
                "(device, ts, avg_apower_w, sample_count, output, "
                " max_apower_w, min_apower_w) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (device, minute_start, avg, len(samples), output, mx, mn),
            )
            self._db.commit()

    def _maybe_prune(self, now: int) -> None:
        # Single knob applies to samples_raw + power_minute.
        # aenergy_offset_events is forever — tiny + load-bearing for
        # offset-spanning energy queries.
        if self.retention_days <= 0:
            return
        if now - self._last_prune_ts < 86400:
            return
        cutoff = now - self.retention_days * 86400
        counts: dict[str, int] = {}
        with self._lock:
            for table in ("samples_raw", "power_minute"):
                counts[table] = self._db.execute(
                    f"DELETE FROM {table} WHERE ts < ?", (cutoff,)
                ).rowcount
            self._db.commit()
        self._last_prune_ts = now
        if any(counts.values()):
            log.info("history prune: %s",
                     ", ".join(f"{n} {t}" for t, n in counts.items() if n))

    def close(self) -> None:
        # Idempotent: SIGTERM handler runs close(), then sys.exit fires atexit
        # which would otherwise re-enter and ProgrammingError on the now-closed
        # connection.
        if self._closed:
            return
        try:
            self.flush_pending_minutes()
        finally:
            with self._lock:
                self._db.close()
            self._closed = True


def _coerce_float(v: Any) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _coerce_bool_int(v: Any) -> int | None:
    if v is True:
        return 1
    if v is False:
        return 0
    return None


def _round_bucket(approx_seconds: int) -> int:
    """Round bucket size to a friendly value (60s, 120s, 5m, 15m, 1h, 6h, 1d)."""
    nice = (60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400)
    for n in nice:
        if approx_seconds <= n:
            return n
    return 86400
