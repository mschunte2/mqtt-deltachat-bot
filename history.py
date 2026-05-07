"""Time-series storage for power consumption + plug events.

Single SQLite file under the bot's config dir. Three tables:

  power_minute  — average apower (W) per minute per device
                  (every MQTT apower sample is buffered in-memory and
                  flushed to one row when the minute boundary rolls over)
  energy_hour   — cumulative aenergy.total (Wh) snapshot per hour per device
                  (one row per (device, hour); replaces previous row in
                  the same hour so the latest snapshot wins)
  events        — raw plug events (e.g. NotifyStatus on events/rpc)

Retention is configurable via RETENTION_DAYS:
  - 0       → keep forever
  - >0      → delete rows older than N days, once per day

Single shared connection guarded by a threading.Lock; writes serialize
on the lock, which is fine for the few-writes-per-minute traffic this
bot generates.
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
-- Every status/switch:0 message, captured verbatim. The richest source —
-- everything else can be re-derived from this.
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

-- Authoritative per-minute energy in mWh, lifted from Shelly's
-- aenergy.by_minute[] (indices 1 and 2 — fully-completed minutes).
-- Idempotent: re-reporting the same minute is a no-op.
CREATE TABLE IF NOT EXISTS energy_minute (
  device     TEXT    NOT NULL,
  ts         INTEGER NOT NULL,    -- minute boundary in unix seconds
  energy_mwh REAL    NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_energy_minute_ts ON energy_minute (ts);

-- Per-minute apower average (computed from raw samples). Kept for the
-- live sparkline and chart queries; could be regenerated from samples_raw.
CREATE TABLE IF NOT EXISTS power_minute (
  device       TEXT    NOT NULL,
  ts           INTEGER NOT NULL,
  avg_apower_w REAL    NOT NULL,
  sample_count INTEGER NOT NULL,
  output       INTEGER,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_power_minute_ts ON power_minute (ts);

-- Cumulative aenergy.total snapshot per hour — kept for backwards compat
-- with existing queries and for cross-checking with energy_minute.
CREATE TABLE IF NOT EXISTS energy_hour (
  device     TEXT    NOT NULL,
  ts         INTEGER NOT NULL,
  aenergy_wh REAL    NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_energy_hour_ts ON energy_hour (ts);

CREATE TABLE IF NOT EXISTS events (
  device  TEXT    NOT NULL,
  ts      INTEGER NOT NULL,
  suffix  TEXT    NOT NULL,
  kind    TEXT,
  payload TEXT,
  PRIMARY KEY (device, ts, suffix)
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts);
"""


class History:
    def __init__(self, db_path: Path, retention_days: int = 0) -> None:
        self.db_path = db_path
        self.retention_days = max(0, int(retention_days))
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
        log.info("history db at %s (retention_days=%s)",
                 db_path, "forever" if self.retention_days == 0 else self.retention_days)

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

    def record_status(self, device: str, ts: int, payload: dict[str, Any]) -> None:
        """Capture a status/switch:0 message verbatim (samples_raw) AND
        extract Shelly's authoritative per-minute energy (energy_minute).

        Called from engine.on_mqtt_message in addition to write_sample —
        write_sample handles the per-minute apower aggregation, this
        handles the lossless raw record + by_minute extraction.
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
                    _coerce_float(aenergy.get("total")),
                    _coerce_bool_int(payload.get("output")),
                    _coerce_float(temperature.get("tC")),
                    payload_json,
                ),
            )
            # Authoritative per-minute energy from aenergy.by_minute[1..2].
            # by_minute[0] is the in-progress minute (always 0); skip it.
            # by_minute[1] = previous full minute (minute_ts - 60),
            # by_minute[2] = two minutes ago      (minute_ts - 120).
            # minute_ts is minute-aligned per Shelly's spec.
            by_minute = aenergy.get("by_minute")
            minute_ts = aenergy.get("minute_ts")
            if (isinstance(by_minute, list) and len(by_minute) >= 3
                    and isinstance(minute_ts, (int, float))):
                base = int(minute_ts)
                for offset, idx in ((-60, 1), (-120, 2)):
                    val = _coerce_float(by_minute[idx])
                    if val is None:
                        continue
                    self._db.execute(
                        "INSERT OR REPLACE INTO energy_minute "
                        "(device, ts, energy_mwh) VALUES (?, ?, ?)",
                        (device, base + offset, val),
                    )
            self._db.commit()

    def write_event(self, device: str, ts: int, suffix: str, payload_text: str) -> None:
        if self._closed:
            return
        kind = None
        try:
            decoded = json.loads(payload_text)
            if isinstance(decoded, dict):
                kind = decoded.get("method") or decoded.get("kind")
                if isinstance(kind, str):
                    kind = kind[:64]
        except (json.JSONDecodeError, TypeError):
            pass
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO events (device, ts, suffix, kind, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                (device, int(ts), suffix, kind, payload_text[:65536]),
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
        """Cumulative aenergy.total (Wh) at the earliest snapshot >= target_ts.

        Returns None if no snapshot at or after target_ts exists. Used to
        compute "Wh consumed since T" via current_aenergy - aenergy_at(T).
        """
        if self._closed:
            return None
        with self._lock:
            row = self._db.execute(
                "SELECT aenergy_wh FROM energy_hour "
                "WHERE device=? AND ts >= ? ORDER BY ts ASC LIMIT 1",
                (device, int(target_ts)),
            ).fetchone()
        return float(row[0]) if row else None

    def energy_consumed_in(self, device: str, since_ts: int, until_ts: int
                           ) -> tuple[float, int | None]:
        """Wh consumed in the window — per-minute hybrid, aggregated in SQL.

        For each minute in the window prefer the authoritative
        energy_minute row (Shelly's own by_minute reading); only fall
        back to power_minute (apower-integrated) for minutes that don't
        appear in energy_minute. Done as one UNION ALL + SUM so SQLite
        does the work — no row-by-row Python loop, ~constant memory
        regardless of window size.

        Returns (wh, earliest_ts).
        """
        if self._closed or until_ts <= since_ts:
            return (0.0, None)
        since, until = int(since_ts), int(until_ts)
        with self._lock:
            row = self._db.execute(
                "SELECT SUM(wh), MIN(ts) FROM ("
                # Authoritative per-minute energy from Shelly
                "  SELECT ts, energy_mwh / 1000.0 AS wh "
                "    FROM energy_minute "
                "   WHERE device=? AND ts>=? AND ts<? "
                "  UNION ALL "
                # Fallback: apower-integrated only where energy_minute is missing
                "  SELECT pm.ts, pm.avg_apower_w / 60.0 AS wh "
                "    FROM power_minute pm "
                "   WHERE pm.device=? AND pm.ts>=? AND pm.ts<? "
                "     AND NOT EXISTS ("
                "       SELECT 1 FROM energy_minute em "
                "        WHERE em.device=pm.device AND em.ts=pm.ts"
                "     )"
                ")",
                (device, since, until, device, since, until),
            ).fetchone()
        if not row or row[0] is None:
            return (0.0, None)
        return (float(row[0]), int(row[1]) if row[1] is not None else None)

    def query_energy_minute(self, device: str, since_ts: int, until_ts: int
                             ) -> list[tuple[int, float]]:
        """Per-minute Wh-equivalent (returned as mWh from Shelly's by_minute)."""
        if self._closed:
            return []
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, energy_mwh FROM energy_minute "
                "WHERE device=? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (device, int(since_ts), int(until_ts)),
            )
            return [(int(t), float(e)) for t, e in cur.fetchall()]

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

    def query_events(self, device: str, since_ts: int, until_ts: int,
                     limit: int = 500) -> list[tuple[int, str, str, str]]:
        """Return [(ts, suffix, kind, payload), ...] for the window."""
        if self._closed:
            return []
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, suffix, kind, payload FROM events "
                "WHERE device=? AND ts >= ? AND ts < ? "
                "ORDER BY ts DESC LIMIT ?",
                (device, int(since_ts), int(until_ts), int(limit)),
            )
            rows = cur.fetchall()
        return [(int(t), s, k or "", p or "") for t, s, k, p in rows]

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
        if self.retention_days <= 0:
            return  # forever
        if now - self._last_prune_ts < 86400:
            return
        cutoff = now - self.retention_days * 86400
        counts = {}
        with self._lock:
            for table in ("samples_raw", "power_minute", "energy_minute",
                          "energy_hour", "events"):
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
