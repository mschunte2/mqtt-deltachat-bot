"""Persistence for per-twin user-Counter state.

Round-trips `{device: {baseline_wh, reset_at_ts}}` through a small
JSON file alongside `rules.json`. Hardware-counter-reset offsets
are NOT here — those live in `history.aenergy_offset_events` as a
queryable log.

Migration: older versions stored `aenergy_offset_wh` per device
in baselines.json. On load, a non-zero legacy offset is migrated
into a single `aenergy_offset_events` row at ts=0 (idempotent via
INSERT OR IGNORE) so the cumulative SUM keeps working forward.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

log = logging.getLogger("mqtt_bot.baselines")


def save(registry, path: Path | str) -> None:
    """Atomic write of every twin's user-Counter state."""
    p = Path(path)
    data = {
        t.name: {
            "baseline_wh": t.baseline_wh,
            "reset_at_ts": t.reset_at_ts,
        }
        for t in registry.all()
    }
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, p)
    except Exception:
        log.exception("persist baselines to %s failed", p)


def load_into(registry, history, path: Path | str) -> int:
    """Restore each twin's baseline state from baselines.json. Drops
    entries for unknown devices (with a WARN summary). Migrates any
    legacy `aenergy_offset_wh` field into the new
    `aenergy_offset_events` table at ts=0. Returns count restored."""
    p = Path(path)
    if not p.exists():
        return 0
    try:
        data = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        log.exception("failed to read %s; skipping load", p)
        return 0
    loaded = 0
    migrated_offsets = 0
    unknown: list[str] = []
    for name, entry in data.items():
        twin = registry.get(name)
        if twin is None:
            unknown.append(name)
            continue
        if not isinstance(entry, dict):
            continue
        twin.set_baseline(
            float(entry.get("baseline_wh", 0.0) or 0.0),
            entry.get("reset_at_ts"),
        )
        loaded += 1
        # One-shot migration: if this baselines.json was written by
        # an older version that tracked aenergy_offset_wh per device,
        # carry the offset forward by recording it as a single
        # aenergy_offset_events row at ts=0. Idempotent via the
        # (device, ts) primary key.
        legacy_offset = float(entry.get("aenergy_offset_wh", 0.0) or 0.0)
        if legacy_offset > 0 and history is not None:
            history.record_offset_event(name, 0, legacy_offset)
            migrated_offsets += 1
    log.info("loaded %d baselines from %s", loaded, p)
    if migrated_offsets:
        log.info("  migrated %d legacy aenergy_offset_wh into "
                 "aenergy_offset_events (one row each at ts=0)",
                 migrated_offsets)
    if unknown:
        log.warning("  baselines.json had entries for unknown device(s): %s — "
                    "ignored. Edit %s to remove them or rename them to "
                    "match devices.json.",
                    ", ".join(sorted(unknown)), p)
    return loaded
