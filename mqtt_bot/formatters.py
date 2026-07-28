"""Pure formatting helpers for chat-side replies (``/list``,
``/rules``, ``/<device> status``).

No module-level state. Each function takes its inputs explicitly
so tests can build fixtures without spinning up the rest of the bot.
"""

from __future__ import annotations

import time

from mqtt_bot.core.twin import PlugTwin
from mqtt_bot.util import durations


#: A device line older than this is flagged with its age. Sized above
#: the plug's default 60s status_update_interval (and the recommended
#: 15s) with room for one missed update, so a healthy device never
#: shows the marker.
STALE_AFTER_S = 180


def format_device_line(twin: PlugTwin, multi_class: bool = False,
                       now: int | None = None) -> str:
    """One-line summary of a device's current state. Used by the
    ``/list`` command and the per-device ``/status``.

    ``multi_class`` adds a ``[class_name]`` suffix when the bot
    serves more than one device family.

    Lines carry their age once the data goes stale. Without it, a
    reading from six hours ago rendered byte-identical to one from four
    seconds ago: if the MQTT thread died at 03:00, `/list` at 09:00
    still said "kaffeete 🟢 ON 42W". `online` cannot substitute — it
    only flips when the broker delivers the plug's LWT, which needs the
    very connection under suspicion. The app has shown freshness since
    v0.2; chat was strictly worse at the most basic health question.
    """
    f = twin.fields_snapshot()
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
    now = int(time.time()) if now is None else int(now)
    last = twin.last_update_ts
    if not last:
        bits.append("⏳ no data yet")
    elif now - last > STALE_AFTER_S:
        bits.append(f"⚠️ stale, last seen {durations.format(now - last)} ago")
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


def format_diag(*, version: str, mqtt_alive: bool, mqtt_connected: bool,
                mqtt_last_message_age_s: float | None,
                sweeper_alive: bool, publisher_alive: bool,
                twins: list, registered_msgids: dict,
                allowed_chats, now: int | None = None) -> str:
    """Render `/diag`.

    Everything here was already in memory and unreachable from chat.
    Before this, a user could see the bot's *belief* about device state
    and the list of pending rules, but not whether MQTT was connected,
    when the last message arrived, whether the sweeper and publisher
    threads were still alive, which msgids were registered, or what code
    was running. "It stopped reacting" had no answer short of SSH.
    """
    now = int(time.time()) if now is None else int(now)

    def mark(ok: bool) -> str:
        return "✅" if ok else "❌"

    if mqtt_last_message_age_s is None:
        last_seen = "never"
    else:
        last_seen = f"{durations.format(int(mqtt_last_message_age_s))} ago"

    lines = [
        f"🩺 diagnostics · {version}",
        "",
        f"{mark(mqtt_alive)} mqtt thread   {'running' if mqtt_alive else 'DEAD'}",
        f"{mark(mqtt_connected)} mqtt broker   "
        f"{'connected' if mqtt_connected else 'DISCONNECTED'}",
        f"   last message  {last_seen}",
        f"{mark(sweeper_alive)} rules sweeper "
        f"{'running' if sweeper_alive else 'DEAD — timed rules will not fire'}",
        f"{mark(publisher_alive)} publisher     "
        f"{'running' if publisher_alive else 'DEAD — apps will not update'}",
        "",
        f"devices ({len(twins)}):",
    ]
    for twin in twins:
        last = getattr(twin, "last_update_ts", 0)
        age = (f"{durations.format(now - last)} ago" if last else "no data yet")
        rules = len(twin.jobs_snapshot())
        lines.append(f"  {twin.name}: last update {age}, {rules} rule(s)")
    if not twins:
        lines.append("  (none visible to this chat)")

    lines.append("")
    if registered_msgids:
        installed = ", ".join(f"{cls}=#{mid}"
                              for cls, mid in sorted(registered_msgids.items()))
        lines.append(f"apps registered here: {installed}")
    else:
        lines.append("apps registered here: none — run /apps to install")
    lines.append(f"authorised chats: {sorted(allowed_chats) or '(none set!)'}")
    return "\n".join(lines)


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
            # Recurrence is `once`, not `recurring_tod` — a TOD rule
            # with once=False re-arms to the next occurrence whether or
            # not the user typed "daily", so calling it anything else
            # misdescribes what the bot will actually do.
            suffix = " daily" if job.is_recurring() else ""
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
