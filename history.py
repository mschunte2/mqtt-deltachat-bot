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
CREATE TABLE IF NOT EXISTS power_minute (
  device       TEXT    NOT NULL,
  ts           INTEGER NOT NULL,
  avg_apower_w REAL    NOT NULL,
  sample_count INTEGER NOT NULL,
  PRIMARY KEY (device, ts)
);
CREATE INDEX IF NOT EXISTS idx_power_minute_ts ON power_minute (ts);

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
        # Per-device current-minute accumulator: {device: (minute_start_ts, [apower, ...])}
        self._minute: dict[str, tuple[int, list[float]]] = {}
        self._hour_seen: dict[str, int] = {}     # last hour-start written per device
        self._last_prune_ts = 0

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        with self._lock:
            self._db.executescript(_SCHEMA)
            self._db.commit()
        log.info("history db at %s (retention_days=%s)",
                 db_path, "forever" if self.retention_days == 0 else self.retention_days)

    # --- writes ----------------------------------------------------------

    def write_sample(self, device: str, ts: int,
                     apower: float | None,
                     aenergy_wh: float | None) -> None:
        """Called on every MQTT status update where apower or aenergy changed."""
        if apower is not None and isinstance(apower, (int, float)):
            self._buffer_apower(device, ts, float(apower))
        if aenergy_wh is not None and isinstance(aenergy_wh, (int, float)):
            self._snapshot_aenergy(device, ts, float(aenergy_wh))
        self._maybe_prune(ts)

    def write_event(self, device: str, ts: int, suffix: str, payload_text: str) -> None:
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
                    max_points: int = 200) -> tuple[int, list[tuple[int, float]]]:
        """Return (bucket_seconds, [(ts, avg_w), ...]) for the window.

        For windows ≤ 24h: read minute granularity, downsample if needed.
        For windows > 24h: aggregate from minute table by hour-or-larger
        buckets to avoid sending tens of thousands of points.
        """
        if until_ts <= since_ts:
            return (60, [])
        window = until_ts - since_ts
        # Pick a bucket size such that points ≤ max_points and bucket ≥ 60s.
        bucket = max(60, _round_bucket(window // max(1, max_points)))
        with self._lock:
            cur = self._db.execute(
                "SELECT (ts/?)*?, "
                "       SUM(avg_apower_w * sample_count) / SUM(sample_count) "
                "FROM power_minute "
                "WHERE device=? AND ts >= ? AND ts < ? "
                "GROUP BY ts/? "
                "ORDER BY ts ASC",
                (bucket, bucket, device, int(since_ts), int(until_ts), bucket),
            )
            rows = cur.fetchall()
        return (bucket, [(int(t), float(w)) for t, w in rows if w is not None])

    def query_energy(self, device: str, since_ts: int, until_ts: int
                     ) -> list[tuple[int, float]]:
        """Per-hour cumulative aenergy snapshots in the window.

        Caller can compute per-hour consumption as deltas between
        consecutive points.
        """
        with self._lock:
            cur = self._db.execute(
                "SELECT ts, aenergy_wh FROM energy_hour "
                "WHERE device=? AND ts >= ? AND ts < ? "
                "ORDER BY ts ASC",
                (device, int(since_ts), int(until_ts)),
            )
            rows = cur.fetchall()
        return [(int(t), float(e)) for t, e in rows]

    def query_events(self, device: str, since_ts: int, until_ts: int,
                     limit: int = 500) -> list[tuple[int, str, str, str]]:
        """Return [(ts, suffix, kind, payload), ...] for the window."""
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

    def _buffer_apower(self, device: str, ts: int, apower: float) -> None:
        ts = int(ts)
        minute_start = ts - (ts % 60)
        with self._lock:
            cur = self._minute.get(device)
            if cur is None:
                self._minute[device] = (minute_start, [apower])
                return
            prev_start, samples = cur
            if minute_start > prev_start:
                # Flush previous minute, start new
                if samples:
                    avg = sum(samples) / len(samples)
                    self._db.execute(
                        "INSERT OR REPLACE INTO power_minute "
                        "(device, ts, avg_apower_w, sample_count) "
                        "VALUES (?, ?, ?, ?)",
                        (device, prev_start, avg, len(samples)),
                    )
                    self._db.commit()
                self._minute[device] = (minute_start, [apower])
            else:
                samples.append(apower)

    def _flush_minute(self, device: str, now: int) -> None:
        with self._lock:
            cur = self._minute.pop(device, None)
            if cur is None:
                return
            minute_start, samples = cur
            if not samples:
                return
            avg = sum(samples) / len(samples)
            self._db.execute(
                "INSERT OR REPLACE INTO power_minute "
                "(device, ts, avg_apower_w, sample_count) "
                "VALUES (?, ?, ?, ?)",
                (device, minute_start, avg, len(samples)),
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
        with self._lock:
            n1 = self._db.execute(
                "DELETE FROM power_minute WHERE ts < ?", (cutoff,)
            ).rowcount
            n2 = self._db.execute(
                "DELETE FROM energy_hour WHERE ts < ?", (cutoff,)
            ).rowcount
            n3 = self._db.execute(
                "DELETE FROM events WHERE ts < ?", (cutoff,)
            ).rowcount
            self._db.commit()
        self._last_prune_ts = now
        if n1 or n2 or n3:
            log.info("history prune: %d power_minute, %d energy_hour, %d events",
                     n1, n2, n3)

    def close(self) -> None:
        self.flush_pending_minutes()
        with self._lock:
            self._db.close()


def _round_bucket(approx_seconds: int) -> int:
    """Round bucket size to a friendly value (60s, 120s, 5m, 15m, 1h, 6h, 1d)."""
    nice = (60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400)
    for n in nice:
        if approx_seconds <= n:
            return n
    return 86400
