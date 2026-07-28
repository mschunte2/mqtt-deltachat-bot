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
  with TZ-pinned tests. (`a2ddf66`)
- A failed MQTT publish was acked 🆗 and chat was told the plug
  switched. Commands now go out at QoS 1 and failures are reported.
  (`ac0ba60`)
- Crashes exited 0, so systemd logged `status=0/SUCCESS`. (`5eb55d9`)
- CSV export wrote 14 columns under a 13-column header; relay state was
  labelled `temperature_c`. (`a88d3dd`)
- MQTT loop-thread death was silent and unrecoverable; `History.close`
  raced writers and could hang SIGTERM until SIGKILL. (`f1db5d6`)
- `/diag`, a server version, and staleness markers on device lines —
  a six-hour-old reading used to render identically to a fresh one.
  (`b0a0025`)
- Every rule-skip reason is now logged; "my rule didn't fire" was
  unanswerable at any log level. (`c198066`)

### Tier 3 — security

- **Missing or non-numeric `ts` bypassed webxdc replay protection
  entirely** — no age bound, no log line, relay switched. Both
  SECURITY.md and CLAUDE.md documented the opposite. Found
  independently by two audits. (`9398db9`)
- systemd unit had zero hardening and ran as the operator's
  sudo-capable login user; the crash-loop rate limit could never trip;
  `BOT_NAME` was interpolated unvalidated into a root-owned unit path.
  (`70d9e18`)

### Tier 5 — documentation

Corrected two false security claims, the false dc.db-reclaim claim, a
`power_history` shape documented two contradictory ways, an undocumented
rule policy (`avg`), and several stale module names and counts.
(`08e99d0`)

### Audited and clean

No secret was ever committed (the repo is public). Cross-chat isolation
is sound. No SQL injection. No path traversal in the export filename.
No credential logging. `send_apps` persists before deleting. No
`TwinDeps` callable is invoked while holding `PlugTwin._lock`.

### Second pass (same day) — remaining tiers

- **Snapshot fan-out reworked** (`480e53e`). Edge broadcasts are
  coalesced (2 s) and performed on the publisher daemon rather than
  inline on paho's callback thread, and carry a compact `"state"`
  payload instead of ~139 KB of chart series. The heartbeat, refresh
  and `/apps` still send `"full"`. This is the fix for the `dc.db`
  growth above. **Requires `/apps` after deploy.**
- **Input validation at both untrusted boundaries** (`4741477`). App
  policy payloads raised TypeError past callers that caught only
  ValueError, and accepted negative durations that collapsed a
  30-minute safety window to one sample. `devices.json` params were
  unvalidated, so a quoted number silently stopped history recording
  and all rule evaluation.
- **Security batch** (`90660e5`). Broker topic ACLs and a separate
  device credential — previously any authenticated client could switch
  relays directly and forge status that drives rule evaluation.
  `refresh`/`telemetry` moved behind the action whitelist. One
  permission predicate. `/help` no longer leaks `HELP_MESSAGE` to
  strangers. State files 0600. `MQTT_PASS` off argv.
- **History ordering and calendar arithmetic** (`5f5e769`). History is
  written before the rules that read it; daily and weekly boundaries
  walk the calendar instead of adding 86400.
- **Lock-step guards and cleanups** (`767b429`). The app's action
  vocabulary is now asserted against the bot's whitelist, plus guards
  on the CSV widths, the duration ceilings, and the sweeper wait vs
  `threading.TIMEOUT_MAX`.

Tests: 175 → 371.

### Still queued

`test_bot.py`. bot.py remains import-hostile, so its routing glue has no
direct coverage. The security-critical decisions it used to make inline
have been extracted into pure, tested modules
(`commands.check_freshness`, `app_policy.build`,
`permissions.chats_for_device`, `csv_export`, `commands.KNOWN_APP_ACTIONS`),
which is the more valuable half of that work — but the refactor itself
is unfinished.

Also unaddressed: TLS to the broker (ACLs landed; transport is still
cleartext on the LAN), and the live host's `LOG_LEVEL=debug` /
`RETENTION_DAYS=0` settings.

### Not deployed

Nothing in this sweep has been applied to `pi@gatekeeper`. The systemd
unit changes the service confinement, and the payload change needs a
`/apps` round, so both want a watched restart rather than a blind one.
