# mqtt-bot — quality, correctness and debuggability sweep

## Context

`portfolio-bot` just went through an incident-driven sweep (`ea3353b` Tier 1,
`f2aec99` Tiers 2–5, merged `fb31395`). This applies the same exercise to
`mqtt-bot`, the sibling Delta Chat ↔ MQTT bridge that switches two
mains-voltage Shelly plugs (`kaffeete`, `km`) from `pi@gatekeeper`.

There is no reported incident here, so the trigger is preventive. Findings came
from auditing the live deployment plus a full read of `bot.py`, all 19 package
modules, the shell scripts, the unit template and the app.

The sweep found the codebase is **well-structured but under-defended at its
edges**. The pure core (twin engine, rules parser, templating, energy math) is
good and well tested. The defects cluster in three places the tests never
reach: `bot.py` (which has *no test file at all* and holds every permission and
replay gate), the daemon threads, and the persistence writers.

Two findings deserve naming up front because the whole plan pivots on them:

- **The documented replay protection on webxdc requests does not exist.**
  `bot.py:746-751` only checks freshness *if* `ts` is present and numeric. A
  missing, null, or string `ts` skips the check silently and switches the relay.
  Both `CLAUDE.md:483-485` and `SECURITY.md` state the opposite. Two independent
  audits found this separately. **(verified)**
- **Every state edge pushes a ~139 KB snapshot with no throttle**, which is what
  grew `dc.db` to 464 MB and puts ~750 SQL queries per device on the MQTT
  callback thread. **(verified on the live host)**

Tier scheme reused verbatim from the portfolio plan. Findings marked
**(verified)** were reproduced against the live host or confirmed by reading the
exact lines; the rest are single-pass reads.

| Tier | Meaning |
|---|---|
| 1 | Data loss / unit-bricking. Ships first. |
| 2 | Correctness, reliability, observability. |
| 3 | Security hardening. |
| 4 | Dead code, unification, consistency guards, test gaps. |
| 5 | Documentation drift (treated as a bug, not a chore). |

**Scope decisions already made:** the payload/protocol change *is* in scope
(accepting the `.xdc` rebuild and a `/apps` re-onboarding round), and live-host
configuration *is* in scope. Nothing is applied to `pi@gatekeeper` without an
explicit go-ahead, per change.

---

## Baseline (measured 2026-07-28)

**Live host.** Unit up 9 days, deployed HEAD `cd6645f` == local HEAD. No
warning-or-worse journal lines in 7 days. `history.sqlite` 146 MB with
`RETENTION_DAYS=0`. `dc.db` **464 MB** — and retention *is* correctly applied
(`delete_device_after=1209600`, `bot=1`), so the v0.2 work landed and works.
Byte breakdown: `msgs.param` 324 MB over 32 653 rows; `msgs_status_updates`
**53 MB over 382 rows (~139 KB each)**; blobdir only 908 KB. 30 125 of the
32 653 `msgs` rows sit in `chat_id=3` (trash) at `timestamp=0`. Journal
10 779 lines/24 h, journald total 2.0 GB, `LOG_LEVEL=debug`.

**Repo.** 175 tests over 16 files. `github.com/mschunte2/mqtt-deltachat-bot` is
**public**. No `TODO`/`FIXME` anywhere.

**Audited and clean** (recorded so the negative result is durable):
- **No secret ever committed.** `git log -p --all` over `devices.json` and
  `env.example` leaks no chat IDs, no MQTT password, no MACs. `.env/` was never
  tracked. **(verified)**
- **Cross-chat isolation is sound.** `chatid` always comes from
  `bot.rpc.get_message(...)`, never the payload; `class_for_msgid` is scoped by
  chat; every action path re-checks `twin.can_chat_see`. Revocation propagates
  correctly (`publisher.py:98-99`). No gap found.
- **No SQL injection.** Every device name and timestamp is a bound `?`. The two
  f-string SQL sites interpolate only from hard-coded literal tuples.
- **No path traversal** in the CSV export filename — though correct *by
  ordering* rather than by validation (see T4).
- **No credential logging.** `MQTT_PASS` never reaches a log statement.
- **No `TwinDeps` callable is ever invoked while holding `PlugTwin._lock`** —
  the "collect under lock, act outside" pattern is applied consistently, and
  there is no lock-order inversion with `History._lock`.
- `webxdc_io.send_apps` persists the new msgid map *before* deleting old
  messages — correct crash ordering, and tested.
- `rules.py:562-574` and `baselines.py:80-89` startup summaries are the best
  observability in the codebase and are the standard the rest should meet.

---

## Tier 1 — Data loss and bricking (ship first)

### T1.1 A single large timer permanently kills every timed rule, and survives restart **(verified)**
[rules.py:609-631](mqtt_bot/core/rules.py#L609-L631)

Only `t.tick_time(now)` is inside the `try`. The deadline computation
(line 612) and `self._wake.wait(timeout=wait_for)` (line 622) are not.
`threading.Event.wait` raises `OverflowError` above `threading.TIMEOUT_MAX`
(9223372036.0), and neither [durations.py:25](mqtt_bot/util/durations.py#L25)
nor `_policy_from_app` ([bot.py:544-546](bot.py#L544-L546)) bounds the value.

`/kaffeete off in 999999999999d` → sweeper thread exits silently → **every
timer and TOD rule on every device stops firing** for the process lifetime.
`start()` refuses to restart it (`if self._thread is not None: return`). And
`_save_rules()` already persisted the poison rule, which `load_into` restores
(deadline is in the future, so not expired) — so `systemctl restart`
reproduces the outage. Only hand-editing `rules.json` recovers.

Fix: bound `durations.parse` and `timer_minutes` at both entry points; clamp
`wait_for` to a sane ceiling; wrap the whole `_loop` body in try/except and log
(copy `Publisher._loop`'s pattern, [publisher.py:134-138](mqtt_bot/io/publisher.py#L134-L138)); reject
out-of-range deadlines in `load_into`.

### T1.2 `rules.json` atomic write is not thread-safe — corruption loses every rule
[rules.py:500-513](mqtt_bot/core/rules.py#L500-L513)

`tmp = p.with_suffix(".tmp")` is a single fixed path, and `deps.save_rules()`
is called from **three** threads with no serialization: the DC handler
(`twin.schedule`, `twin.cancel`), the sweeper (`twin.tick_time`), and the MQTT
callback (`twin._tick_state_rules`, on every status update where a state rule
fires). `Path.write_text` truncates; two writers interleave, and `os.replace`
publishes a mixed or truncated file. `load_into` then hits `JSONDecodeError`
and returns 0 — **all persisted rules silently gone on the next restart**.

Trigger is ordinary: a plug status update arriving while the user taps "add
rule". Same pattern in [baselines.py:34-38](mqtt_bot/io/baselines.py#L34-L38)
and `webxdc_io._save`.

Fix: extract one shared `atomic_write()` helper (portfolio-bot did exactly this
in `d7ff542`), give it a per-path lock and a unique temp name, and route all
three writers through it.

### T1.3 The `cd6645f` re-seed path broke `WebxdcIO`'s single-thread invariant
[webxdc_io.py:33-193](mqtt_bot/io/webxdc_io.py#L33-L193), [bot.py:221-248](bot.py#L221-L248)

[publisher.py:16-21](mqtt_bot/io/publisher.py#L16-L21) documents the invariant:
*"WebxdcIO holds a registry whose mutation points (send_apps) are confined to
the DC handler thread."* The stale-msgid re-seed added in `cd6645f` violates it —
`broadcast → push_to_msgid → on_missing → _reseed_stale_chat → send_apps`
mutates `self._map` from the Publisher daemon and the MQTT callback thread.

Two failures: `map_snapshot()` iterating `self._map.items()` concurrently →
`RuntimeError: dictionary changed size during iteration`; and two concurrent
`send_apps` writing the same `app_msgids.tmp` → corrupt registry, which `_load`
swallows into `{}` → **every chat's app registration lost, all apps dark until
a manual `/apps`**. The `_reseed_lock` at `bot.py:217` only dedupes per chat.

This is the same class of bug the `DC_DELETE_DEVICE_AFTER_DAYS` work makes
*routine*: containers age out on schedule, so multi-chat re-seeds will collide.

Fix: give `WebxdcIO` a lock covering `_map` mutation and `map_snapshot`; route
its write through the T1.2 helper.

### T1.4 Restart silently drops recurring timer rules and non-`daily` TOD rules **(verified)**
[rules.py:546-554](mqtt_bot/core/rules.py#L546-L554)

The re-arm gate is `if job.recurring_tod and job.time_of_day:` — but since
v0.1.5 the recurrence flag is `once`, not `recurring_tod`. There is no
`elif job.timer_seconds:` branch at all. So:

1. `/kaffeete off in 30m` (`once=False`, `timer_seconds=1800`) is re-armed
   forever at runtime but **dropped by any restart more than 30 min later**,
   with no chat notification.
2. `/kaffeete off at 18h` (`recurring_tod=False`, `once=False`) is treated as
   daily by `tick_time`, rendered as one-shot by `formatters.rule_clauses:82`,
   and dropped as one-shot by `load_into`. Three code paths, three notions of
   "recurring".

Fix: the predicate is `not job.once`, plus the missing timer re-arm branch.
Make `formatters` read the same predicate.

### T1.5 No `fsync` before `os.replace` in any of the three atomic writers
[rules.py:510](mqtt_bot/core/rules.py#L510), [baselines.py:37](mqtt_bot/io/baselines.py#L37), [webxdc_io.py:65](mqtt_bot/io/webxdc_io.py#L65)

`os.replace` is atomic w.r.t. the rename, not w.r.t. the tmp file's contents
reaching disk. A power cut on the Pi — the expected failure mode for an
always-on appliance controller — can leave a zero-length `rules.json`, which
`load_into` then discards wholesale. Folds into the T1.2 helper.

### T1.6 Unbounded export and idle-window queries can OOM the Pi
[bot.py:1001-1010](bot.py#L1001-L1010), [twin.py:453-457](mqtt_bot/core/twin.py#L453-L457)

`/kaffeete export 3650d` `fetchall()`s the whole table into RAM and then writes
a CSV of the same order (CLAUDE.md quotes ~70 MB/year at 15 s cadence) on a
1 GB Pi. Same shape in `to_dict`: an idle rule written as `off if idle 5W in
365d` makes **every snapshot build** run a 365-day `query_samples_raw`.

Fix: cap the export window and stream rows to the file; cap idle/consumed/avg
window lengths at policy-parse time (shares the T1.1 bound).

---

## Tier 2A — Correctness and reliability

### T2.1 Snapshot fan-out is unthrottled and full-size on every state edge **(verified)**

This is the dominant runtime cost and the sole driver of `dc.db` growth.

`publisher.broadcast(..., force=True)` fires on every twin state edge
([publisher.py:81-111](mqtt_bot/io/publisher.py#L81-L111)) and rebuilds
everything: `_power_history` emits 1440 minute + 750 hour + 365 day buckets of
`[ts,min,max,avg,output]` plus 365 daily-energy pairs, **per device**
([snapshot.py:50-95](mqtt_bot/core/snapshot.py#L50-L95), [snapshot.py:129-134](mqtt_bot/core/snapshot.py#L129-L134)).
Measured at ~139 KB per status update. No coalescing, no debounce, no rate limit.

Per device per snapshot that is ~750 SQL queries (`_daily_energy_wh` alone does
366 × `aenergy_at`), each taking and releasing `History._lock` — contending
directly with the writer — plus two JSON serializations, **all inline on paho's
network callback thread inside `on_mqtt`**. With `MQTT_KEEPALIVE=30`, a plug
that flaps can stall the loop past the `PINGREQ` deadline, causing a broker
disconnect, causing another edge. Note `ONLINE_FLAP_DEBOUNCE_S` deliberately
debounces the *chat post* but not the broadcast ([twin.py:546-548](mqtt_bot/core/twin.py#L546-L548)).

Every app open also calls `sendRefresh()` ([main.js:718](devices/shelly_plug/app/main.js#L718)),
so each open costs another permanent ~139 KB carrier.

Fix, three parts:
1. **Move the broadcast off the MQTT thread** — enqueue to the Publisher's
   daemon rather than building inline in `on_mqtt`.
2. **Coalesce** — collapse edges arriving within a short window into one push.
3. **Split the payload** (this is the protocol change): send live state + rules
   on edges, and the long `hour`/`day`/`daily_energy` series only on the
   heartbeat or on explicit request. Requires an app change, `.xdc` rebuild and
   a `/apps` round.

### T2.2 `delete_device_after` cannot reclaim the dominant cost — the documented mitigation is wrong **(verified)**

CLAUDE.md's *Maintenance: reclaiming dc.db space* says retention plus
`auto_vacuum=INCREMENTAL` returns pages "on its own — no `VACUUM` needed". On
the live host retention **is** active and the DB still reached 464 MB: 30 125
carrier rows sit in `chat_id=3` (trash) with `timestamp=0`, which a
timestamp-based prune never matches. T2.1 is the actual fix; the docs must stop
promising otherwise (T5), and the plan should include a one-off reclaim for the
existing 464 MB.

### T2.3 DST fall-back produces a past deadline and a 2 Hz fire/re-arm storm **(verified)**
[rules.py:354-362](mqtt_bot/core/rules.py#L354-L362)

`tom = time.localtime(now + 86400)` lands on the *same* calendar date when the
day is 25 hours long and `now` is between 00:00 and 00:59. Verified:
`next_tod_deadline(0, 15, 2026-10-25 00:30 CEST)` → `2026-10-25 00:15 CEST`,
in the past. The docstring's DST-safety claim covers the `mktime` call, which
is fine; the `+86400` rollover is not.

Consequence: `tick_time` fires, re-arms to the same past timestamp, `wait_for`
clamps to `0.5` — repeating twice per second for up to 30 minutes, each
iteration rewriting `rules.json` (~3600 times). For `on`/`off` targets
`_job_dormant` suppresses the repeated chat post once the device reaches state,
but the rewrite storm and CPU spin continue; for a `toggle` target
`_job_dormant` always returns `False`, so **the plug is toggled and the chat
spammed at 2 Hz for half an hour**.

Fix: advance by calendar date, not `+86400`. `test_rules.py:178-196` only
asserts "future and within 25 h" against the live wall clock, so it passes 364
days a year — the regression test must pin `TZ` and the date.

### T2.4 Failed MQTT publish is acked 🆗 and the chat is told the switch happened
[mqtt_client.py:71-74](mqtt_bot/io/mqtt_client.py#L71-L74), [twin.py:197-204](mqtt_bot/core/twin.py#L197-L204), [twin.py:823-838](mqtt_bot/core/twin.py#L823-L838)

`publish()` returns `None`; a non-success rc (e.g. `MQTT_ERR_NO_CONN` during a
reconnect) is logged as a warning and discarded. `dispatch` then returns
`(True, "")`, reacts 🆗 and broadcasts; `_fire_rule` posts *"🌑 switching off
kaffeete after 30m idle"*. There is no queue, no retry, no QoS > 0.

Broker restarts at the moment an auto-off fires → chat says it's off, reaction
says 🆗, **the espresso machine stays on**. For a safety feature on a mains
appliance this is the wrong failure mode, and it is both the worst security
finding and the worst debuggability finding.

Fix: propagate the rc; fail the dispatch with an explicit chat reply and ❌
reaction; use QoS 1 for command topics.

### T2.5 Unhandled exceptions exit with status 0 **(verified)**
[bot.py:272-292](bot.py#L272-L292)

`_on_shutdown` is registered as both an `atexit` hook and the SIGTERM handler,
and ends in `os._exit(0)`. `atexit` runs during normal finalisation — *including*
after an unhandled exception propagates out of `__main__`. So a crash reports
`code=exited, status=0/SUCCESS`, `journalctl -p err` shows nothing, and a clean
SIGTERM is indistinguishable from a hard crash. If the operator ever switches
to `Restart=on-failure`, crashes stop being restarted.

The `os._exit` reasoning is sound; the hardcoded `0` is the bug. Fix: pass an
exit code, `0` only on the SIGTERM path.

### T2.6 CSV export is off by one column — relay state is labelled `temperature_c` **(verified)**
[bot.py:1022-1045](bot.py#L1022-L1045)

Header has 13 columns; both data rows write 14. For `samples_raw` rows `output`
lands under the `temperature_c` header and `temperature_c` falls into an
unnamed 14th column. **Every export the user has ever run is mislabeled**, and
a spreadsheet import reads relay state as temperature. Fix: add the missing
`output`-for-raw header column and add a width-equality test.

### T2.7 MQTT loop-thread death is undetected and unrecoverable
[mqtt_client.py:58-67](mqtt_bot/io/mqtt_client.py#L58-L67)

The thread target is bare `loop_forever` with no wrapper and no `stop()`. If it
returns or raises, the bot never receives another MQTT message; nothing
monitors liveness. Reconnects themselves are handled well (`_handle_disconnect`
logs, paho retries) — it's thread *death* that is silent. Asymmetric with
`Publisher`/`RulesSweeper`; give it the same wrapper + a restart.

### T2.8 `History.close()` races the MQTT thread and can deadlock SIGTERM
[history.py:232, 629-640](mqtt_bot/io/history.py#L629-L640), [bot.py:272-292](bot.py#L272-L292)

`_closed` is set *after* `self._db.close()`, and every read/write does an
unsynchronized `if self._closed: return` before taking the lock — so the MQTT
thread can slip through and raise `ProgrammingError: Cannot operate on a closed
database`. The docstring and CLAUDE.md:922-923 both claim this is handled.

Worse: SIGTERM runs the handler on the **main** thread. If the main thread is
mid-`with self._lock` in `History` (e.g. a 7-day `query_samples_raw` in
`_handle_export`, which takes seconds), `close()` blocks forever on the
non-reentrant lock and `os._exit(0)` is never reached — the process hangs until
systemd's `SIGKILL` ~90 s later, which is exactly the hang `os._exit` was added
to fix.

### T2.9 Type-confused and negative policy values from the app
[bot.py:536-586](bot.py#L536-L586)

Callers catch only `ValueError`, so `{"idle": {"threshold": null}}` →
`float(None)` → uncaught `TypeError` escaping into the `RawEvent` hook. And
only `timer_minutes` is range-checked: `idle.duration_minutes`,
`consumed.window_minutes` and `avg.window_minutes` accept negatives.
`{"idle": {"duration_minutes": -5}}` collapses a 30-minute safety window to one
status update, and the chat ack renders `for 0s` because `durations.format`
clamps negatives — so the user cannot see anything is wrong.

### T2.10 Lock-scope defects
| Where | Problem |
|---|---|
| [twin.py:627-683](mqtt_bot/core/twin.py#L627-L683), [twin.py:322-374](mqtt_bot/core/twin.py#L322-L374) | `fires` is selected under the lock, side effects run outside it. A rule cancelled in that window still fires — user taps ×, gets "cancelled", plug switches a moment later. `_eval_consumed`/`_eval_avg` widen the window with SQL round-trips. |
| [twin.py:578-610](mqtt_bot/core/twin.py#L578-L610) | Threshold latch mutated with no lock held. Benign today (one writer), violates the module's stated invariant. |
| [formatters.py:23](mqtt_bot/formatters.py#L23) | `dict(twin.fields)` unlocked while `jobs_snapshot()` two lines later is correctly locked — the omission looks accidental. Concurrent resize → `RuntimeError`. |
| [twin.py:241-245](mqtt_bot/core/twin.py#L241-L245) vs [twin.py:497-498](mqtt_bot/core/twin.py#L497-L498) | `baseline_wh`/`reset_at_ts` written under the lock, read outside; a snapshot can pair a new baseline with an old reset timestamp. |

### T2.11 A non-numeric `params` value silently stops history and all rule evaluation
[twin.py:578-610](mqtt_bot/core/twin.py#L578-L610), [config.py:360-361](mqtt_bot/util/config.py#L360-L361)

`params` from `devices.json` are copied verbatim with no type validation, so
`"power_threshold_watts": "1500"` (a plausible typo — the neighbouring values
*are* strings) makes `value >= limit` raise `TypeError` on **every** status
update. That propagates out of `_evaluate_chat_events` → `on_mqtt`, so
`_tick_state_rules`, `_write_history` and `broadcast` never run: history stops
recording and rules stop evaluating, with only a repeating stacktrace to show
for it. Fix: validate `params` types in `config._parse_device` and fail
`--check-config`.

### T2.12 History is written *after* the rules that read it
[twin.py:151-171](mqtt_bot/core/twin.py#L151-L171)

Order is chat events → window resets → `_tick_state_rules` (which queries
history) → `_write_history`. Every state-rule evaluation sees history missing
the sample that just arrived: `_eval_avg`'s coverage gate systematically
undercounts by one bucket and `_eval_consumed`'s window lags one sample — a 10%
error at the default 60 s cadence against a 10-minute window.

### T2.13 DST-unsafe day arithmetic in energy buckets
[snapshot.py:189-192](mqtt_bot/core/snapshot.py#L189-L192), [history.py:523-528](mqtt_bot/io/history.py#L523-L528)

`midnight - lt.tm_wday * _DAY` and `oldest_start + d * 86400` land on 23:00 or
01:00 across a DST boundary, shifting "this week" and the daily bars by an hour.
`query_power`'s `GROUP BY ts/bucket` also buckets the `day` series on **UTC**
days, not local ones.

---

## Tier 2B — Observability

The user's stated debugging scenario — *"my rule didn't fire"* — is currently
unanswerable from chat **or** from the journal at any log level.

### T2.14 Every rule-skip reason is unlogged
Only the positive path logs ([twin.py:824](mqtt_bot/core/twin.py#L824)). Silent
skips: dormant/state-based ([twin.py:636-641](mqtt_bot/core/twin.py#L636-L641)),
dormant/time-based ([twin.py:340](mqtt_bot/core/twin.py#L340)), the 60 s restart
grace ([twin.py:645-647](mqtt_bot/core/twin.py#L645-L647)), avg warm-up
([twin.py:748](mqtt_bot/core/twin.py#L748)), avg coverage < 90 %
([twin.py:756](mqtt_bot/core/twin.py#L756)), consumed warm-up
([twin.py:790](mqtt_bot/core/twin.py#L790)), consumed coverage
([twin.py:796](mqtt_bot/core/twin.py#L796)), idle `_below_since` reset
([twin.py:724](mqtt_bot/core/twin.py#L724)).

Dormancy is the most common real cause and the user has no way to learn it. The
avg-coverage gate is the hardest to guess: a rule silently refuses to fire
because the plug was offline for >10 % of the window. Fix: one DEBUG line per
skip reason, and surface live evaluation state in `/rules` (the app already has
`current_max_w`/`current_wh`/`current_window_minutes` at
[twin.py:451-472](mqtt_bot/core/twin.py#L451-L472); chat does not).

### T2.15 Staleness is invisible in chat — one-line fix
[formatters.py:16-53](mqtt_bot/formatters.py#L16-L53) never reads
`twin.last_update_ts`, though it is maintained at
[twin.py:139](mqtt_bot/core/twin.py#L139) and shipped to the app. If the MQTT
thread dies at 03:00, `/list` at 09:00 renders `kaffeete 🟢 ON 42W` — identical
to a healthy four-second-old reading. `online` cannot help: it only flips via
the plug's LWT, which needs the very connection under suspicion. The app *does*
render freshness ([main.js:314](devices/shelly_plug/app/main.js#L314)), so chat
is strictly worse than the app at the most basic health question.

### T2.16 No diagnostic command, and no server-side version
Full surface is `/id /help /list /apps /rules /refresh /all` + per-device verbs.
No `/diag`, `/health`, global `/status`, or `/version`. A user cannot see
whether MQTT is connected, when the last message arrived, whether the sweeper
and publisher threads are alive, which msgids are registered, or what code is
running — all one-line reads from existing structures. There is no `__version__`
anywhere, while the *app* stamps `APP_BUILD_TS` and reports it in telemetry, so
the client build is identifiable and the server build is not.

Fix: add `/diag` (thread liveness, MQTT connect state, last-message age per
device, registered msgids, effective `ALLOWED_CHATS`) and a git-SHA
`__version__` in the startup banner and `/diag`.

### T2.17 Silent-failure surfaces and log-level mis-assignments
- `RulesSweeper` outer loop unguarded (T1.1) — the only subsystem whose death
  is both silent and total. `Publisher._loop` is the correct pattern.
- `History` post-`close()` no-ops in **nine** methods with zero log output.
- [bot.py:230-232](bot.py#L230-L232) — the `_reseed_stale_chat` "can't re-seed"
  early return sits *above* the log line, so the abandoned heal is invisible;
  with 14-day retention this path is hit in normal operation.
- Replay rejections at INFO ([bot.py:750](bot.py#L750), [bot.py:791](bot.py#L791))
  and they name neither device nor action → should be WARNING with both.
- Unknown webxdc action at DEBUG ([bot.py:458](bot.py#L458)) → invisible at the
  default level, so "the app's buttons stopped working after an upgrade"
  produces an empty journal.
- [bot.py:28](bot.py#L28) — `getattr(logging, _LOG_LEVEL, logging.INFO)` resolves
  *any* module attribute: `NOTSET` → 0 → log everything. Use an explicit dict
  and warn on miss.
- **Host:** `LOG_LEVEL=debug` currently dumps every raw Delta Chat event
  ([bot.py:717-719](bot.py#L717-L719)) — chat message text and full ~139 KB
  snapshot payloads — into a journal already at 2.0 GB, 10 779 lines/day. Set
  the host to `info`, and split `mqtt_bot`-only debug from root debug so the
  useful mode stops being the one that leaks chat contents.

---

## Tier 3 — Security hardening

| # | Issue | Where |
|---|---|---|
| T3.1 | **Missing/non-numeric `ts` bypasses webxdc replay protection entirely** — no check, no log, relay switches. Contradicts `CLAUDE.md:483-485` and `SECURITY.md`. An old `.xdc` (the codebase explicitly expects these to persist) or a status update replayed after a long offline period executes an unbounded-age `on`/`off`. **(verified)** | [bot.py:746-751](bot.py#L746-L751) |
| T3.2 | Broker listens on `0.0.0.0:1883`, **no TLS, no `acl_file`, one shared credential** for bot and plugs. Credentials cross the LAN in cleartext on every plug reconnect. Any authenticated client publishes straight to the command topic, bypassing `ALLOWED_CHATS` entirely; forged `status/switch:0` drives `twin.fields`, chat posts, `samples_raw`, **and rule evaluation** (60 s of fake `apower:0` triggers a real `off if idle`). Forged `aenergy.total:0` writes a permanent row to the retention-exempt `aenergy_offset_events`. **(verified on host)** | [setup-mosquitto.sh:42-52](setup-mosquitto.sh#L42-L52) |
| T3.3 | systemd unit has **zero hardening** and runs as the operator's sudo-capable login user (`User=@USER@` from `SUDO_USER`). Any RCE in paho / `deltachat-rpc-server` / the JSON parsers yields SSH keys and the Delta Chat profile tar. **(verified on host)** | [deltabot.service.template:8-18](systemd-unit/deltabot.service.template#L8-L18) |
| T3.4 | Secrets group/world-readable: `.env/env` 0664, the Delta Chat profile tar 0644. `SECURITY.md` calls the tar "encrypted" — only true if a backup passphrase was set, which nothing verifies. | `.env/` |
| T3.5 | State files inherit umask (0664 in a 0775 dir): `rules.json`, `app_msgids.json`, `baselines.json`, `history.sqlite`. Per-minute home power readings are a high-resolution occupancy signal. Fix with `os.umask(0o077)` at startup + explicit modes. | [webxdc_io.py:58-66](mqtt_bot/io/webxdc_io.py#L58-L66) et al. |
| T3.6 | `telemetry` and `refresh` are handled **before** the `_KNOWN` action whitelist. `app_telemetry` is retention-exempt and each request emits a 15-field INFO line — a looping app fills the SD card. `refresh` triggers an unthrottled full snapshot build + push. | [bot.py:436-460](bot.py#L436-L460) |
| T3.7 | Chat broadcast re-implements the permission predicate instead of calling `permissions.chat_can_see`. `SECURITY.md` claims "there is no bypass path" — inaccurate for the highest-volume outbound surface. They agree today; they will not the moment a deny-list or mute is added. | [bot.py:131](bot.py#L131) |
| T3.8 | `/help` bypasses the allow-list *and* the replay window, emitting operator-supplied `HELP_MESSAGE` to any stranger. `SECURITY.md` says `/help` has "no leakage" — true for the device list, not for `HELP_MESSAGE`. | [bot.py:778-780](bot.py#L778-L780) |
| T3.9 | `install-systemd-unit.sh` interpolates `BOT_NAME`-derived values raw into a root-owned unit via `sed` with `|` delimiter; a `/` in `BOT_NAME` writes outside the intended path. `BOT_NAME` is never validated, unlike device names (`config.py:18` `NAME_RE`). | [install-systemd-unit.sh:46-49](install-systemd-unit.sh#L46-L49) |
| T3.10 | Bare `int()` on `ALLOWED_CHATS` / `PUBLISH_INTERVAL_S` → unhandled `ValueError` before the logger exists, feeding a silent crash loop. `RETENTION_DAYS` is the only one written defensively — copy that. | [config.py:382](mqtt_bot/util/config.py#L382), [bot.py:108-109](bot.py#L108-L109) |
| T3.11 | `Restart=always` + `RestartSec=5` vs systemd's `StartLimitBurst=5`/`StartLimitIntervalSec=10s` → the rate limit can never trip. A config error restarts every ~5 s forever; the unit never enters `failed`, so `systemctl is-failed` stays clean. | [deltabot.service.template:14-15](systemd-unit/deltabot.service.template#L14-L15) |
| T3.12 | `mosquitto_passwd -b` puts `MQTT_PASS` on the command line, readable in `/proc/*/cmdline`. Low on a single-user Pi; stdin avoids it. | [setup-mosquitto.sh:67](setup-mosquitto.sh#L67) |

---

## Tier 4 — Dead code, guards, and the test gap

**The test gap is the structural reason T3.1 could exist while both documents
described the correct behaviour.** 175 tests cover the pure layers genuinely
well and are blind to the entire security boundary.

**Zero coverage:** `bot.py` (1116 lines — no test file exists;
`tests/__init__.py:11` notes it is import-hostile because module-level
construction touches the filesystem), `mqtt_bot/commands.py` (the parser *and*
the three replay constants), `mqtt_bot/formatters.py`,
`mqtt_bot/io/mqtt_client.py`, `RulesSweeper._loop`, all concurrency, all
timezone-pinned time behaviour, and `load_into` expiry/re-arm.

- **Make `bot.py` importable** (defer the module-level construction behind a
  factory) and add `test_bot.py` covering: both replay windows including the
  missing/string `ts` path, `_policy_from_app` bounds and type confusion, the
  `_KNOWN` whitelist, the msgid-registry gate, `/id`+`/help` bypass, and CSV
  column-width equality. This is the highest-value item in the tier.
- **TZ-pinned tests** for `next_tod_deadline` on the fall-back day, and for the
  week/day bucket arithmetic.
- **Add** `test_commands.py`, `test_formatters.py`, `test_mqtt_client.py`.
- **Unify:** one `atomic_write()` helper for the four writers (T1.2/T1.5);
  one permission predicate (T3.7).
- **Delete:** `commands.sanitize` is dead — nothing calls it, and
  `request["device"]` reaches chat unbounded via `f"unknown device: {…}"`, so an
  app instance can make the bot post a megabyte-long message. Either wire
  `sanitize` in or drop it and bound at the boundary.
- **Fix cheaply:** `history.py` uses `Any` without importing it (harmless only
  under `from __future__ import annotations`) and imports `defaultdict`
  unused; `query_power`'s docstring sits *below* an early return so
  `__doc__` is `None`; `rehydrate.py:31` uses the logger name `mqtt_bot`
  instead of `mqtt_bot.rehydrate`, blurring startup attribution.
- **Bound the leaks:** `Publisher._last_hash` never evicts (`/apps` mints a new
  msgid each time); `WebxdcIO._map` never prunes departed chats;
  `app_telemetry` is exempt from `_maybe_prune`.
- **Decide:** `app_telemetry` — `ANDROID-COLDSTART.md` closed the investigation
  as "nothing actionable" but argues the columns are worth keeping as a
  regression guard. The app still emits on every boot **(verified)**. Keep it
  and put it under retention, or remove the ingest path with the app change in
  T2.1. Recommend: keep, add retention.
- **Note, don't fix:** the CSV export path is safe *by ordering* —
  `durations.parse` and `NAME_RE` run before `mkstemp`, but `mkstemp(prefix=)`
  does not itself reject `/`. Add a comment so a future reorder doesn't
  reintroduce traversal.
- **Consistency guards** (cheap lock-step tests, portfolio-bot style): the
  CSV header width vs both row widths; `_KNOWN` ⊇ the actions `main.js` emits;
  `samples_raw`'s second-resolution PK vs the documented 1 s minimum cadence.
- **Untracked `BAD-APPS/`** holds two unrelated third-party `.xdc` files — they
  are evidence for `ANDROID-COLDSTART.md`. Either gitignore them or move them
  under a documented path.

**Recommended against:** a per-device MQTT credential scheme. The plugs are
configured through their own web UI, so per-device passwords mean hand-editing
each plug on every rotation, and topic ACLs (T3.2) get most of the benefit for
one config file.

---

## Tier 5 — Documentation (drift is a bug)

`CLAUDE.md` and `README.md` have drifted materially, and two of the drifts are
load-bearing security claims that the code contradicts.

**Security claims that are false:**
- `CLAUDE.md:483-485` and `SECURITY.md`: "The `ts` field on app requests is
  required. Apps without a `ts` field are rejected" — the code does the
  opposite (T3.1).
- `SECURITY.md`: "Both checks happen in `permissions.chat_can_see` — the single
  function called from every routing site… There is no bypass path" — not
  accurate for chat broadcast (T3.7).
- `SECURITY.md`: "Recommend running Mosquitto on the same host… so the broker
  link doesn't traverse the LAN" — the script's actual default is
  `listener 1883 0.0.0.0`, and its own comment says so (T3.2).
- `SECURITY.md` calls the profile tar "encrypted" — unverified (T3.4).

**Operational claims that are false:**
- CLAUDE.md *Maintenance: reclaiming dc.db space* — retention plus
  `auto_vacuum=INCREMENTAL` does **not** reclaim the growth; measured 464 MB
  with retention active (T2.2).
- `history.py:225-228` and CLAUDE.md:922-923 claim the post-`close()` read
  short-circuit prevents the shutdown `ProgrammingError`; it doesn't (T2.8).
- `publisher.py:16-21` states `send_apps` is confined to the DC handler thread;
  `cd6645f` broke that (T1.3).

**Stale structure and counts:**
- CLAUDE.md still says "`PlugTwin` lives in `plug.py`" and README references
  `plug.py` — it is `mqtt_bot/core/twin.py`, as CLAUDE.md itself says elsewhere.
- CLAUDE.md *Known limitations* still lists "**No scheduler persistence.** Bot
  restart drops pending auto-off / auto-on jobs" — contradicted by its own
  *Persistence* section.
- CLAUDE.md *Known limitations* still discusses the "engine state cache" lock;
  `engine.py` was deleted in v0.2 and CI now asserts it cannot come back.
- `power_history` is documented with **two different entry shapes**:
  `[[ts, w, 1|0|null]]` in the protocol section vs
  `[ts, min_w, max_w, avg_w, output]` in the snapshot section. The second is
  correct.
- Chart windows documented as "1h / 6h / 12h / 24h / 31d" in both files; 6 h
  was dropped and 7 d / 365 d added.
- Test counts: README says 130, CLAUDE.md says 130 in one place and 135 in
  another; actual is **175**.
- CLAUDE.md's testing section names `scheduler.parse_policy` and `engine` tests —
  modules that no longer exist.
- **The `avg` rule policy is documented nowhere.** It exists in
  [rules.py:65-217](mqtt_bot/core/rules.py#L65-L217) and is user-reachable, but
  neither CLAUDE.md's policy list (timer/tod/idle/consumed) nor README's command
  reference mentions it. Add it to both.

---

## Sequencing

Each item is its own change with tests, in tier order. Deploy only on explicit
go-ahead, per change.

1. **Tier 1** — the two data-loss classes first (T1.2/T1.3/T1.5 share the
   `atomic_write` helper, so they land together), then T1.1, T1.4, T1.6.
   Bot-side only; a restart is enough, no `/apps` needed.
2. **Tier 2A + 2B** — T2.4 (fail-open ack) and T2.5 (exit code) are small and
   high-value; take them before the bigger ones. T2.1 lands last in this group
   because it carries the app change, `.xdc` rebuild and `/apps` round.
3. **Tier 3** — repo-side changes (T3.1, T3.6–T3.12) ship with the code; the
   host-side changes (T3.2 mosquitto ACL/TLS, T3.3 hardening + dedicated user,
   T3.4/T3.5 file modes) are applied to `pi@gatekeeper` separately and each
   needs its own go-ahead, since T3.3 changes the service user and T3.2 requires
   reconfiguring both plugs.
4. **Tier 4** — `test_bot.py` first (it retro-covers Tiers 1–3), then the rest.
5. **Tier 5** — docs last, so they describe what actually shipped.

**Host config, applied separately:** `LOG_LEVEL=debug` → `info`;
`RETENTION_DAYS=0` → a bounded value; one-off `dc.db` reclaim after T2.1 is
deployed and the carrier volume drops.

## Verification

- `python3 -m unittest discover tests` — 175 green now; every verified finding
  above gets a named regression test, so the count should rise well past 200.
- `python3 bot.py --check-config` — must still pass without the venv, and must
  now reject non-numeric `params` (T2.11).
- `./build-xdc.sh devices/shelly_plug` and `node -c` on the app, per CI.
- **T2.3** needs a `TZ=Europe/Berlin`-pinned test at `2026-10-25 00:30`
  asserting the returned deadline is strictly in the future.
- **T3.1** needs cases for `ts` absent, `null`, `"1714000000"`, `True`, and a
  correctly-fresh int.
- **T2.6** needs a header-width-equals-row-width assertion for both row kinds.
- **T1.4** needs a `load_into` case with an expired `once=False` timer rule and
  an expired non-`daily` TOD rule, asserting both are re-armed, not dropped.
- **T1.1** needs a bounds test on `durations.parse` / `timer_minutes` feeding a
  real `RulesSweeper` iteration.
- On the host after T2.1 deploys: watch `msgs_status_updates` row growth and
  `du -h dc.db` over a few days; the per-edge carrier rate should fall sharply.
  Confirm rules still fire with `/rules` plus the new `/diag`.

## Housekeeping

- Create `change-history/` in this repo (portfolio-bot has one) and archive this
  plan as `change-history/plans/2026-07-28-quality-and-debuggability-sweep.md`,
  with a `CHANGELOG.md` entry per shipped tier group linking back to it.
- Add a provenance entry to `CLAUDE.md` by date + short SHA, per the repo's own
  rule about not inventing version labels.
- Save a memory noting that `mqtt-bot`'s security boundary lives entirely in the
  untested `bot.py`, so any future change there needs a test written first.
