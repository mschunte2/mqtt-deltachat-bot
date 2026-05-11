"""Pure formatting helpers for chat-side replies (``/list``,
``/rules``, ``/<device> status``).

No module-level state. Each function takes its inputs explicitly
so tests can build fixtures without spinning up the rest of the bot.
"""

from __future__ import annotations

import time

from mqtt_bot.core.twin import PlugTwin
from mqtt_bot.util import durations


def format_device_line(twin: PlugTwin, multi_class: bool = False) -> str:
    """One-line summary of a device's current state. Used by the
    ``/list`` command and the per-device ``/status``.

    ``multi_class`` adds a ``[class_name]`` suffix when the bot
    serves more than one device family.
    """
    f = dict(twin.fields)
    online = f.get("online")
    output = f.get("output")
    apower = f.get("apower")
    aenergy = f.get("aenergy")
    bits: list[str] = [twin.name]
    bits.append("🟢" if online else "🔴" if online is False else "⚪")
    if isinstance(output, bool):
        bits.append("ON" if output else "OFF")
    elif output is None:
        bits.append("?")
    if isinstance(apower, (int, float)):
        bits.append(f"{apower:.0f}W")
    if isinstance(aenergy, (int, float)):
        bits.append(f"({aenergy / 1000.0:.2f} kWh)")
    if twin.cfg.description:
        bits.append(f"— {twin.cfg.description}")
    for job in twin.jobs_snapshot():
        action = job.target_action
        if job.deadline_ts:
            remaining = max(0, job.deadline_ts - int(time.time()))
            bits.append(f"[{action} in {durations.format(remaining)}]")
        elif job.has_idle():
            bits.append(f"[{action} on idle]")
        elif job.has_consumed():
            bits.append(f"[{action} on used<Wh]")
        elif job.has_avg():
            bits.append(f"[{action} on max-1min<W]")
    if multi_class:
        bits.append(f"[{twin.cls.name}]")
    return " ".join(bits)


def format_rule_lines(job) -> list[str]:
    """Line(s) for ``/rules``. Single-clause rules render inline;
    multi-clause (OR-combined) rules get an indented bullet list
    under an action header."""
    clauses = rule_clauses(job)
    suffix = " (once)" if job.once else ""
    if not clauses:
        return [job.target_action + suffix]
    if len(clauses) == 1:
        return [f"{job.target_action} {clauses[0]}{suffix}"]
    return [f"{job.target_action}:{suffix}"] + [f"  - {c}" for c in clauses]


def rule_clauses(job) -> list[str]:
    """Each enabled policy on a rule rendered as a clean clause.

    ``in 30m`` — timer rule.
    ``at 18:00 daily (in 4h)`` — TOD rule.
    ``when apower<5W for 60s`` — idle rule.
    ``when used<5Wh in 10m`` — consumed rule.
    """
    out: list[str] = []
    if job.deadline_ts:
        remaining = max(0, job.deadline_ts - int(time.time()))
        if job._time_mode == "tod" and job.time_of_day:
            h, m = job.time_of_day
            suffix = " daily" if job.recurring_tod else ""
            out.append(f"at {h:02d}:{m:02d}{suffix} "
                       f"(in {durations.format(remaining)})")
        else:
            out.append(f"in {durations.format(remaining)}")
    if job.has_idle():
        out.append(f"when {job.idle_field}<{job.idle_threshold:g}W "
                   f"for {durations.format(job.idle_duration_s)}")
    if job.has_consumed():
        out.append(f"when used<{job.consumed_threshold_wh:g}Wh "
                   f"in {durations.format(job.consumed_window_s)}")
    if job.has_avg():
        out.append(f"when max-1min-avg<{job.avg_threshold_w:g}W "
                   f"in {durations.format(job.avg_window_s)}")
    return out
