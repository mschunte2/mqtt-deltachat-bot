"""Single outbound assembly point: bot ground truth → app payload.

`build_for_chat(chat_id, class_name, registry, allowed_chats)` is the
ONE function that produces the snapshot Publisher pushes. It walks
the registry, filters by per-chat visibility, calls each visible
twin's `to_dict()`, and wraps with `{class, server_ts, devices}`.

The helpers below (`_power_history`, `_energy_summary`,
`_daily_energy_wh`, `_gap_fill`, `_local_*`) are imported by
PlugTwin.to_dict() — they live here because they all derive their
output from the History SQLite store and have no twin-state
dependencies.
"""

from __future__ import annotations

import logging
import time
from typing import Any

log = logging.getLogger("mqtt_bot.snapshot")

_MINUTE = 60
_HOUR = 3600
_DAY = 86400

_HISTORY_MINUTE_WINDOW = 24 * _HOUR     # last 24 h at 1-min buckets
_HISTORY_HOUR_WINDOW = 31 * _DAY        # last 31 d at 1-h buckets


def build_for_chat(chat_id: int, class_name: str,
                   registry, allowed_chats: set[int]) -> dict[str, Any] | None:
    """Build the per-(chat, class) payload. None if the chat sees no
    devices in this class — caller skips the push."""
    visible = [t for t in registry.all()
               if t.cls.name == class_name
               and t.can_chat_see(chat_id, allowed_chats)]
    if not visible:
        return None
    return {
        "class": class_name,
        "server_ts": int(time.time()),
        "devices": {t.name: t.to_dict() for t in visible},
    }


# --- helpers (used by PlugTwin.to_dict) ---------------------------------

def _power_history(history, device_name: str) -> dict[str, list]:
    """Two resolutions, gap-filled with `null` output for missing buckets."""
    now = int(time.time())
    minute = _gap_fill(
        history.query_power(device_name,
                            now - _HISTORY_MINUTE_WINDOW, now,
                            max_points=1440)[1],
        since=now - _HISTORY_MINUTE_WINDOW, until=now, bucket=_MINUTE,
    )
    hour = _gap_fill(
        history.query_power(device_name,
                            now - _HISTORY_HOUR_WINDOW, now,
                            max_points=750)[1],
        since=now - _HISTORY_HOUR_WINDOW, until=now, bucket=_HOUR,
    )
    return {"minute": minute, "hour": hour}


def _gap_fill(rows: list[tuple[int, float, int | None]],
              since: int, until: int, bucket: int) -> list[list]:
    """Dense series at `bucket` step. Missing buckets become
    [ts, 0.0, None] — None=offline, app paints grey."""
    by_ts = {int(ts): (float(w), out) for ts, w, out in rows}
    start = ((since + bucket - 1) // bucket) * bucket
    out: list[list] = []
    t = start
    while t < until:
        if t in by_ts:
            w, o = by_ts[t]
            out.append([t, w, o])
        else:
            out.append([t, 0.0, None])
        t += bucket
    return out


def _daily_energy_wh(history, device_name: str) -> list[tuple[int, float]]:
    return history.daily_energy_kwh(
        device_name, _local_midnight(int(time.time())), days=30,
    )


def _energy_summary(history, device_name: str,
                    current_wh: float | None,
                    *,
                    baseline_wh: float = 0.0,
                    reset_at_ts: int | None = None) -> dict[str, Any]:
    """kWh consumed in standard intervals. Each interval reports a
    `partial_since_ts` when our oldest sample arrived noticeably later
    than the requested start (app marks a `*` suffix).

    `baseline_wh` and `reset_at_ts` come from the twin and drive the
    new resettable Counter row in the app. Lifetime (current_total_wh)
    stays alongside it — Lifetime never resets, Counter does."""
    now = int(time.time())
    intervals = (
        ("kwh_last_hour",  now - _HOUR),
        ("kwh_last_24h",   now - _DAY),
        ("kwh_last_7d",    now - 7 * _DAY),
        ("kwh_last_30d",   now - 30 * _DAY),
        ("kwh_last_365d",  now - 365 * _DAY),
        ("kwh_today",      _local_midnight(now)),
        ("kwh_this_week",  _local_week_start(now)),
        ("kwh_this_month", _local_month_start(now)),
    )
    out: dict[str, Any] = {
        "current_total_wh":
            float(current_wh) if current_wh is not None else None,
        # Counter = lifetime - baseline. None when we don't yet have a
        # current reading; clamped to 0 so a one-time clock skew or a
        # plug counter rollover doesn't show negative.
        "kwh_since_reset": (
            max(0.0, (float(current_wh) - float(baseline_wh)) / 1000.0)
            if current_wh is not None else None
        ),
        "reset_at_ts": reset_at_ts,
    }
    PARTIAL_GAP = 90
    for key, since in intervals:
        wh, earliest = history.energy_consumed_in(device_name, since, now)
        partial_since = (
            earliest if (earliest is not None and earliest - since > PARTIAL_GAP)
            else None
        )
        out[key] = {"kwh": wh / 1000.0, "partial_since_ts": partial_since}
    return out


def _local_midnight(now_ts: int) -> int:
    lt = time.localtime(now_ts)
    return int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday,
                            0, 0, 0, 0, 0, -1)))


def _local_week_start(now_ts: int) -> int:
    midnight = _local_midnight(now_ts)
    lt = time.localtime(midnight)
    return midnight - lt.tm_wday * _DAY


def _local_month_start(now_ts: int) -> int:
    lt = time.localtime(now_ts)
    return int(time.mktime((lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1)))
