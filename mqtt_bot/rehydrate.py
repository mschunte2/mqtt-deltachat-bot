"""Backfill idle-rule transient state from history on bot startup.

After ``rules.load_into`` reads ``rules.json``, ``_below_since`` is
empty on every restored idle rule. Without this backfill, every
``systemctl restart`` forces an idle rule to wait a fresh window
before it can fire.

Consumed rules need no backfill: their evaluator reads
``history.energy_consumed_in`` directly (the plug's authoritative
``aenergy.total`` counter is persisted in ``samples_raw``), so the
"observation buffer" lives in the database, not in memory.

Idle rules: read raw samples from ``samples_raw`` (NOT the
per-minute averages) — averages smooth cycling-load spikes that
the live evaluator catches, which would falsely satisfy the
"all below threshold" check and arm the rule to fire on the
first sample after restart. This was the v0.2.3 bug; rehydrate
uses raw samples to match the live evaluator's semantics.
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("mqtt_bot")


def rehydrate_rules_from_history(registry, history) -> None:
    """Backfill idle-rule below-since timestamps from ``history``.
    No-op when ``history`` is ``None`` (e.g. the legacy
    ``HISTORY_DB`` env var disabled it)."""
    if history is None:
        return
    now = int(time.time())
    for twin in registry.all():
        for job in twin.jobs_snapshot():
            if job.has_idle() and job.idle_field == "apower":
                since = now - job.idle_duration_s
                raw_rows = history.query_samples_raw(twin.name, since, now)
                if raw_rows and all(
                    (r[1] is not None and r[1] < job.idle_threshold)
                    for r in raw_rows
                ):
                    job._below_since = raw_rows[0][0]
                    log.info("rehydrated idle rule %s/%s — below "
                             "%.1fW since %d (continuous, %d raw samples)",
                             twin.name, job.rule_id,
                             job.idle_threshold, raw_rows[0][0],
                             len(raw_rows))
