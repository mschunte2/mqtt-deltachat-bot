# Change history

Newest first. Each entry links the plan it came from; plans are
archived under `change-history/plans/`.

Only `v0.1.5` and `v0.2.1` are real git tags. Entries are dated and
reference short SHAs — no invented version labels.

## 2026-07-28 — quality, correctness and debuggability sweep

Plan: [2026-07-28-quality-and-debuggability-sweep.md](plans/2026-07-28-quality-and-debuggability-sweep.md)

**Tiers 1, 2 (partial), 3 (partial) and 5 implemented.** Remaining
items are listed as queued in the plan; see *Still queued* below.

Preventive, not incident-driven: the sibling `portfolio-bot` had just
been through the same exercise. Findings came from auditing the live
`pi@gatekeeper` deployment plus a full read of the codebase.

Tests: 175 → 305.

### Tier 1 — data loss and bricking

- `io/atomic.py`: the three JSON state writers shared one fixed `.tmp`
  path with no lock and no fsync. `save_all` is reachable from three
  threads, so a collision published a spliced `rules.json` that the
  loader turned into "no rules at all". Reproduced against the old
  code. (`8a9021e`)
- `WebxdcIO` had no lock, and the `cd6645f` stale-msgid re-seed had
  broken its documented single-thread invariant — concurrent re-seeds
  raced `map_snapshot` and the registry file, which drops every chat's
  app registration. Routine now that retention ages containers out on
  a schedule. (`658d3f4`)
- One oversized timer killed the rules sweeper permanently and the
  poison rule was persisted, so the outage survived every restart.
  Bounded at the parser, clamped at the sweeper, and the whole loop
  body guarded. (`e8429be`)
- Restart silently dropped recurring timer rules and non-`daily` TOD
  rules: three code paths each defined "recurring" differently. Unified
  on `ScheduledJob.is_recurring()`/`rearm()`. (`767bb0e`)
- Bounded the CSV export and rule observation windows. The host has
  ~275 MB available against a 306k-row `samples_raw`, so these were
  live OOM risks. (`2d11d91`)

### Tier 2 — correctness, reliability, observability

- TOD deadlines advanced by `+86400`, which lands on the same calendar
  date on the 25-hour fall-back day — producing a past deadline and a
  2 Hz fire/re-arm storm for up to half an hour. Now calendar-based,
  with TZ-pinned tests. (`34372af`)
- A failed MQTT publish was acked 🆗 and chat was told the plug
  switched. Commands now go out at QoS 1 and failures are reported.
  (`687b4fa`)
- Crashes exited 0, so systemd logged `status=0/SUCCESS`. (`96679b5`)
- CSV export wrote 14 columns under a 13-column header; relay state was
  labelled `temperature_c`. (`cdd9a60`)
- MQTT loop-thread death was silent and unrecoverable; `History.close`
  raced writers and could hang SIGTERM until SIGKILL. (`8bee740`)
- `/diag`, a server version, and staleness markers on device lines —
  a six-hour-old reading used to render identically to a fresh one.
  (`d9e3091`)
- Every rule-skip reason is now logged; "my rule didn't fire" was
  unanswerable at any log level. (`1af29f3`)

### Tier 3 — security

- **Missing or non-numeric `ts` bypassed webxdc replay protection
  entirely** — no age bound, no log line, relay switched. Both
  SECURITY.md and CLAUDE.md documented the opposite. Found
  independently by two audits. (`4c717fd`)
- systemd unit had zero hardening and ran as the operator's
  sudo-capable login user; the crash-loop rate limit could never trip;
  `BOT_NAME` was interpolated unvalidated into a root-owned unit path.
  (`97c4250`)

### Tier 5 — documentation

Corrected two false security claims, the false dc.db-reclaim claim, a
`power_history` shape documented two contradictory ways, an undocumented
rule policy (`avg`), and several stale module names and counts.
(`b62b3c7`)

### Audited and clean

No secret was ever committed (the repo is public). Cross-chat isolation
is sound. No SQL injection. No path traversal in the export filename.
No credential logging. `send_apps` persists before deleting. No
`TwinDeps` callable is invoked while holding `PlugTwin._lock`.

### Still queued

Snapshot fan-out rework (T2.1 — the ~139 KB unthrottled push that drives
`dc.db` growth; needs an app change and a `/apps` round), app-policy type
and bounds validation, mosquitto topic ACLs and per-role credentials,
state-file modes, the remaining lock-scope and DST-bucket items, and
`test_bot.py` (bot.py is still import-hostile, so the auth and replay
layer has no direct coverage).

### Not deployed

Nothing in this sweep has been applied to `pi@gatekeeper`.
