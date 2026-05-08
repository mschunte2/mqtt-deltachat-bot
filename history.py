"""Time-series storage for power consumption.

Single SQLite file under the bot's config dir. Tables:

  samples_raw    — every status/switch:0 captured verbatim
                   (lossless audit trail; raw aenergy_total_wh)
  aenergy_minute — absolute aenergy.total counter, one row per
                   minute, post-offset (the fine-tier source for
                   energy_consumed_in)
  power_minute   — average apower (W) per minute per device
  energy_hour    — absolute aenergy.total (Wh) snapshot per hour,
                   post-offset (forever-retained coarse tier)

Energy "kWh consumed in [a, b]" is computed as the difference
between two cumulative-counter readings — `total_at(b) - total_at(a)`.
Two-tier lookup: prefer aenergy_minute (≤1-min rounding) for each
boundary, fall back to energy_hour (≤1-hr rounding) for boundaries
older than the minute-tier retention. Both tables store the
post-offset effective counter — see plug.py's PlugTwin counter-reset
detection: when the plug's hardware aenergy.total is observed to go
backwards (manufacturer reset, replacement, firmware glitch), the
twin tracks a per-device offset so the time series stays continuous
across the discontinuity. samples_raw stores the *raw* counter
(untouched) so the CSV export still reflects the plug's webUI 1:1.

Retention has two knobs:
  RETENTION_DAYS              applies to aenergy_minute, power_minute
  SAMPLES_RAW_RETENTION_DAYS  applies to samples_raw (the bulky one)
  energy_hour                 always kept forever (≤150 KB/year/device)
  - 0       → keep forever
  - >0      → delete rows older than N days, once per day

Single shared connection guarded by a threading.Lock; writes serialize
on the lock, which is fine for the few-writes-per-minute traffic this
bot generates.

(The `events` table was dropped in v0.2; the `energy_minute` table
that stored Shelly's per-minute by_minute[] deltas was dropped from
the code in v0.2.2 — its DDL is gone but rows in users' existing
SQLite files persist as dead data.)
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
-- trail; aenergy_total_wh here is the RAW plug counter, untouched
-- by any reset-offset adjustment (so a CSV export still matches the
-- plug's webUI 1:1).
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

-- Per-minute aenergy.total snapshot — POST-OFFSET (continuous
-- across hardware counter resets). Fine-grained tier for
-- energy_consumed_via_total. Idempotent: INSERT OR REPLACE.
CREATE TABLE IF NOT EXISTS aenergy_minute (
  device     TEXT    NOT NULL,
  ts         INTEGER NOT NULL,    -- minute boundary in unix seconds
  aenergy_wh REAL    NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_aenergy_minute_ts ON aenergy_minute (ts);

-- Per-minute apower average (computed from raw samples). Kept for the
-- power chart in the app and for rule rehydration.
CREATE TABLE IF NOT EXISTS power_minute (
  device       TEXT    NOT NULL,
  ts           INTEGER NOT NULL,
  avg_apower_w REAL    NOT NULL,
  sample_count INTEGER NOT NULL,
  output       INTEGER,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_power_minute_ts ON power_minute (ts);

-- Per-hour aenergy.total snapshot — POST-OFFSET. Coarse tier;
-- forever-retained (~150 KB/year/device) so "Last 365 days" works
-- even if the user prunes aenergy_minute aggressively.
CREATE TABLE IF NOT EXISTS energy_hour (
  device     TEXT    NOT NULL,
  ts         INTEGER NOT NULL,
  aenergy_wh REAL    NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_energy_hour_ts ON energy_hour (ts);
"""


class History:
    def __init__(self, db_path: Path,
                 retention_days: int = 0,
                 samples_raw_retention_days: int = 0) -> None:
        self.db_path = db_path
        # Two retention knobs:
        # - retention_days: aenergy_minute, power_minute (per-minute series)
        # - samples_raw_retention_days: samples_raw (lossless, ~70 MB/yr/device)
        # energy_hour is forever-retained regardless.
        self.retention_days = max(0, int(retention_days))
        self.samples_raw_retention_days = max(0, int(samples_raw_retention_days))
        self._lock = threading.Lock()
        # Per-device current-minute accumulator:
        # {device: (minute_start_ts, [apower, ...], latest_output_or_None)}
        self._minute: dict[str, tuple[int, list[float], int | None]] = {}
        self._hour_seen: dict[str, int] = {}     # last hour-start written per device
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
            self._db.commit()
        log.info("history db at %s (retention_days=%s, "
                 "samples_raw_retention_days=%s)",
                 db_path,
                 "forever" if self.retention_days == 0 else self.retention_days,
                 ("forever" if self.samples_raw_retention_days == 0
                  else self.samples_raw_retention_days))

    # --- writes ----------------------------------------------------------

    def write_sample(self, device: str, ts: int,
                     apower: float | None,
                     aenergy_wh: float | None,
                     output: bool | None = None) -> None:
        """Called on every MQTT status update where apower or aenergy changed.

        `output` is the relay state at the time of the sample. When the
        minute boundary rolls over, the LATEST output value seen during
        that minute is what gets persisted alongside the avg apower.

        No-op once the connection has been closed — late writes can
        race the SIGTERM-triggered close() while the MQTT thread is
        still draining its inbox.
        """
        if self._closed:
            return
        if apower is not None and isinstance(apower, (int, float)):
            out_int = (1 if output is True else 0 if output is False else None)
            self._buffer_apower(device, ts, float(apower), out_int)
        if aenergy_wh is not None and isinstance(aenergy_wh, (int, float)):
            self._snapshot_aenergy(device, ts, float(aenergy_wh))
        self._maybe_prune(ts)

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

    def write_aenergy_minute(self, device: str, ts: int,
                              effective_aenergy_wh: float) -> None:
        """Snapshot the post-offset cumulative aenergy.total at the
        minute boundary `ts // 60 * 60`. Called from PlugTwin.on_mqtt
        with the offset-adjusted ('effective') value so the time
        series stays continuous across hardware counter resets.

        INSERT OR REPLACE — the latest write within a minute wins."""
        if self._closed:
            return
        minute_start = int(ts) - (int(ts) % 60)
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO aenergy_minute "
                "(device, ts, aenergy_wh) VALUES (?, ?, ?)",
                (device, minute_start, float(effective_aenergy_wh)),
            )
            self._db.commit()

    def backfill_aenergy_minute_from_samples(self) -> int:
        """Populate aenergy_minute from existing samples_raw rows so
        a fresh deploy doesn't have to wait for new MQTT data to fill
        the minute-tier. Idempotent (INSERT OR IGNORE).

        Note: backfilled values are taken from samples_raw, which
        stores the RAW pre-offset aenergy_total_wh. Pre-existing
        offsets aren't applied here. Subsequent live writes from
        PlugTwin.on_mqtt use the post-offset value via
        write_aenergy_minute. The two regimes only differ at the
        moment the offset accrues — see plug.py for the trade-off.

        Returns the count of rows inserted (excludes pre-existing).
        """
        if self._closed:
            return 0
        with self._lock:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO aenergy_minute (device, ts, aenergy_wh) "
                "SELECT device, (ts/60)*60 AS minute, MAX(aenergy_total_wh) "
                "  FROM samples_raw "
                " WHERE aenergy_total_wh IS NOT NULL "
                " GROUP BY device, minute"
            )
            inserted = cur.rowcount
            self._db.commit()
        if inserted > 0:
            log.info("backfilled %d aenergy_minute rows from samples_raw",
                     inserted)
        return inserted

    def flush_pending_minutes(self, now: int | None = None) -> None:
        """Force-flush minute buffers regardless of boundary. Called on
        clean shutdown so an interrupted minute isn't lost."""
        now = now if now is not None else int(time.time())
        for device in list(self._minute.keys()):
            self._flush_minute(device, now=now)

    # --- reads -----------------------------------------------------------

    def query_power(self, device: str, since_ts: int, until_ts: int,
                    max_points: int = 200,
                    ) -> tuple[int, list[tuple[int, float, int | None]]]:
        if self._closed:
            return (60, [])
        """Return (bucket_seconds, [(ts, avg_w, output), ...]) for the window.

        `output` for a bucket is MAX(output) over its underlying minute
        rows: 1 if the device was on for any minute, 0 if every minute was
        off, NULL if no minutes had a known output. (Treating "any-on as
        on" keeps brief usage visible in coarse buckets.)
        """
        if until_ts <= since_ts:
            return (60, [])
        window = until_ts - since_ts
        bucket = max(60, _round_bucket(window // max(1, max_points)))
        with self._lock:
            cur = self._db.execute(
                "SELECT (ts/?)*?, "
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
            (int(t), float(w), int(o) if o is not None else None)
            for t, w, o in rows if w is not None
        ])

    def query_energy(self, device: str, since_ts: int, until_ts: int
                     ) -> list[tuple[int, float]]:
        """Per-hour cumulative aenergy snapshots in the window.

        Caller can compute per-hour consumption as deltas between
        consecutive points.
        """
        if self._closed:
            return []
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, aenergy_wh FROM energy_hour "
                "WHERE device=? AND ts >= ? AND ts < ? "
                "ORDER BY ts ASC",
                (device, int(since_ts), int(until_ts)),
            )
            rows = cur.fetchall()
        return [(int(t), float(e)) for t, e in rows]

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
        """Latest cumulative aenergy reading at-or-before `target_ts`,
        in post-offset Wh. Two-tier lookup: prefer aenergy_minute, fall
        back to energy_hour. Used internally by energy_consumed_in;
        also exposed for callers that need a single-point lookup.

        Returns None if no row exists at-or-before target_ts in either
        tier (e.g. asking about a window before the bot was deployed).
        """
        if self._closed:
            return None
        target = int(target_ts)
        with self._lock:
            row_min = self._db.execute(
                "SELECT ts, aenergy_wh FROM aenergy_minute "
                "WHERE device=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                (device, target),
            ).fetchone()
            row_hr = self._db.execute(
                "SELECT ts, aenergy_wh FROM energy_hour "
                "WHERE device=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                (device, target),
            ).fetchone()
        return _pick_more_recent(row_min, row_hr)

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
        # Lower-bound: prefer at-or-before(since). Fall back to the
        # *earliest* available row if the window starts before our
        # data (caller marks this as partial via earliest_ts > since).
        earliest_ts: int | None
        lower_wh: float
        with self._lock:
            row_min = self._db.execute(
                "SELECT ts, aenergy_wh FROM aenergy_minute "
                "WHERE device=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                (device, since),
            ).fetchone()
            row_hr = self._db.execute(
                "SELECT ts, aenergy_wh FROM energy_hour "
                "WHERE device=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                (device, since),
            ).fetchone()
            best = _pick_more_recent_row(row_min, row_hr)
            if best is None:
                # No row at-or-before since: try the *earliest*
                # available row instead.
                row_min = self._db.execute(
                    "SELECT ts, aenergy_wh FROM aenergy_minute "
                    "WHERE device=? ORDER BY ts ASC LIMIT 1",
                    (device,),
                ).fetchone()
                row_hr = self._db.execute(
                    "SELECT ts, aenergy_wh FROM energy_hour "
                    "WHERE device=? ORDER BY ts ASC LIMIT 1",
                    (device,),
                ).fetchone()
                best = _pick_earliest_row(row_min, row_hr)
                if best is None:
                    return (0.0, None)
            earliest_ts = int(best[0])
            lower_wh = float(best[1])
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
                    self._db.execute(
                        "INSERT OR REPLACE INTO power_minute "
                        "(device, ts, avg_apower_w, sample_count, output) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (device, prev_start, avg, len(samples), prev_output),
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
            self._db.execute(
                "INSERT OR REPLACE INTO power_minute "
                "(device, ts, avg_apower_w, sample_count, output) "
                "VALUES (?, ?, ?, ?, ?)",
                (device, minute_start, avg, len(samples), output),
            )
            self._db.commit()

    def _snapshot_aenergy(self, device: str, ts: int, aenergy_wh: float) -> None:
        ts = int(ts)
        hour_start = ts - (ts % 3600)
        last = self._hour_seen.get(device)
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO energy_hour (device, ts, aenergy_wh) "
                "VALUES (?, ?, ?)",
                (device, hour_start, aenergy_wh),
            )
            self._db.commit()
        # Track the latest hour we've snapshotted; not used for correctness
        # (INSERT OR REPLACE handles re-snapshots within the same hour) but
        # useful for diagnostics.
        if last is None or hour_start > last:
            self._hour_seen[device] = hour_start

    def _maybe_prune(self, now: int) -> None:
        # Both knobs: 0 means forever (no prune for that group).
        # energy_hour is forever-retained regardless — it's tiny
        # (~150 KB/year/device) and is what makes "Last 365 days"
        # work after aggressive minute-tier pruning.
        if (self.retention_days <= 0
                and self.samples_raw_retention_days <= 0):
            return
        if now - self._last_prune_ts < 86400:
            return
        counts: dict[str, int] = {}
        with self._lock:
            if self.retention_days > 0:
                cutoff = now - self.retention_days * 86400
                for table in ("aenergy_minute", "power_minute"):
                    counts[table] = self._db.execute(
                        f"DELETE FROM {table} WHERE ts < ?", (cutoff,)
                    ).rowcount
            if self.samples_raw_retention_days > 0:
                cutoff = now - self.samples_raw_retention_days * 86400
                counts["samples_raw"] = self._db.execute(
                    "DELETE FROM samples_raw WHERE ts < ?", (cutoff,)
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


def _pick_more_recent(row_min, row_hr) -> float | None:
    """Two `(ts, value)` rows from the two tiers; return the value
    from whichever has the higher ts. None if both are None."""
    best = _pick_more_recent_row(row_min, row_hr)
    return float(best[1]) if best else None


def _pick_more_recent_row(row_min, row_hr):
    """Two-tier picker: return the (ts, value) row with the higher
    ts. Used for "latest at-or-before T" lookups."""
    if row_min is None:
        return row_hr
    if row_hr is None:
        return row_min
    return row_min if row_min[0] >= row_hr[0] else row_hr


def _pick_earliest_row(row_min, row_hr):
    """Two-tier picker: return the (ts, value) row with the lower
    ts. Used for the partial-window-fallback path in
    `energy_consumed_in` (window starts before any data)."""
    if row_min is None:
        return row_hr
    if row_hr is None:
        return row_min
    return row_min if row_min[0] <= row_hr[0] else row_hr


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
