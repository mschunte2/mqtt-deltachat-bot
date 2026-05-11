"""Backfill state-rule transient state from history on bot startup.

After ``rules.load_into`` reads ``rules.json``, transient counters
(``_below_since`` for idle, ``_avg_started_at`` for avg) are empty
on every restored rule. Without backfill, every ``systemctl restart``
forces these rules to wait a fresh window before they can fire.

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

Avg rules: read per-minute averages from ``power_minute`` and stamp
``_avg_started_at`` once MAX(avg_apower_w) over the window is below
threshold AND at least 90% of the expected minute buckets are
present. Matches the live evaluator's max-of-1min-averages semantics.
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("mqtt_bot")


def rehydrate_rules_from_history(registry, history) -> None:
    """Backfill idle/avg rule transient timestamps from ``history``.
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
            if job.has_avg() and job.avg_field == "apower":
                since = now - job.avg_window_s
                minute_rows = history.query_power_raw(twin.name, since, now)
                vals = [r[1] for r in minute_rows
                        if r[1] is not None] if minute_rows else []
                expected = max(1, job.avg_window_s // 60)
                if (vals and len(vals) >= expected * 0.9
                        and max(vals) < job.avg_threshold_w):
                    # Stamp at the window start so the warmup gate is
                    # already satisfied — the rule is immediately
                    # eligible to fire on the next state update.
                    job._avg_started_at = since
                    log.info("rehydrated avg rule %s/%s — max 1-min "
                             "avg %.1fW < %.1fW over %ds (%d minute buckets)",
                             twin.name, job.rule_id,
                             max(vals), job.avg_threshold_w,
                             job.avg_window_s, len(minute_rows))
