# Hypothesis — Android cold-start cost from pre-load `setUpdateListener`

**Date:** 2026-05-18
**Status:** **REFUTED** (2026-05-18, see Result section below).
Reverted to synchronous-listener boot in build `1779494800`.

## Symptom

Opening the Shelly Plug webxdc app on Android in Delta Chat takes
roughly 10 s of black-screen before any UI appears. On Linux (Delta
Chat Desktop) the same xdc opens in well under a second. The user
reports this is "every open", not just the first one after `/apps`.

## What the telemetry rules out

The `app_telemetry` table (added in commit `02d41d1`, augmented in
`3922fa9` and `adf7b70`) captures four `performance.now()` snapshots
relative to webview navigation start:

- `nav_to_head_ms`  — inline `<script>` at the very top of `<head>`
- `nav_to_script_ms` — first executable line of `main.js`
- `nav_to_render_ms` — just before the first `render()` call
- `nav_to_paint_ms`  — `requestAnimationFrame` after `render()`

Even on a row whose `start_serial` and `cache_size_bytes` differ from
the other rows (strong evidence it's a separate localStorage scope =
a separate client, most likely Android), all four snapshots are under
300 ms. So nothing we measure between navigation-start and first
paint accounts for the observed 10 s.

Two possibilities remain:

1. The 10 s is **before** `performance.timeOrigin` (pre-navigation
   webview spawn). Not measurable from inside the page.
2. The 10 s is **after** first paint but **before** our 2-s
   `setTimeout` fires the telemetry — i.e. inside an API call we
   make synchronously during boot but don't measure.

## Comparative evidence (BAD-APPS folder)

We compared the source of three xdcs that all run inside the same
Delta Chat for Android client:

| xdc | Android open | Pre-load `setUpdateListener`? | Pre-rendered body? |
|---|---|---|---|
| `arcanecircle-chess-v2.4.0`        | fast | **no** (called inside `onload = …`) | no (`<div id="root">`) |
| `webratte-event-countdown-v1.1.1`  | slow | yes (inline `<script>` at end of `<body>`) | yes |
| `mqtt-bot/shelly_plug` (this repo) | slow | yes (`main.js` at end of `<body>`) | yes |

Bundle size, file count, and HTML body weight don't track with speed
(chess is the largest at ~108 KB zipped, ~350 KB unpacked). The
distinguishing feature is **when `webxdc.setUpdateListener` is
registered**.

Chess registers the listener inside `window.onload = function() { … }`:

```js
onload=function(){
  et.mount(document.getElementById("root"),{view:…}),
  window.webxdc.setUpdateListener(ut,0)
}
```

Both slow apps register it during synchronous script evaluation,
before `window.load` fires.

## Hypothesis

`webxdc.setUpdateListener(handler, startSerial)` registration is the
boundary at which the Delta Chat for Android client materialises its
view of the chat's webxdc updates table (scan + replay-buffer
population). On Android, the materialisation does enough synchronous
I/O across the JS↔native bridge that it blocks for several seconds.

When registration happens **before** the first paint, the user sees
those seconds as black screen.
When it happens **after** `onload` (chess's pattern), the page is
already rendered, the block is invisible, and inbound updates simply
arrive a few seconds later than they would have.

Why this matches the Linux/Android asymmetry: Delta Chat Desktop and
the Android client are different binaries with very different
storage and IPC backends. A bridge call that costs 100 ms on desktop
SQLite can plausibly cost seconds on Android's content provider /
IPC marshalling, especially after a fresh process spawn.

## Test (this commit)

Apply the chess pattern to our app:

- Cache hydration from `localStorage` stays synchronous (no webxdc
  API touched).
- The first `render()` stays synchronous so cached data paints
  immediately on script eval.
- `setUpdateListener` and `sendRefresh()` move into a
  `window.addEventListener('load', …)` handler.
- New telemetry fields capture the cost of this previously-hidden
  step:
  - `nav_to_load_ms`     — when `window.load` fires
  - `nav_to_listener_ms` — when `setUpdateListener(...)` returns

If the hypothesis is right, the user should see:

- Subjective: app paints cached data ~instantly on Android.
- Objective: `nav_to_listener_ms − nav_to_load_ms` is large
  (multiple seconds) on Android, small on Linux.

If the hypothesis is wrong, both deltas will be small everywhere and
the Android delay must live in pre-navigation webview spawn (which is
outside our reach; no JS-side mitigation possible).

## How to evaluate

After deploying and reopening the app a few times on both Linux and
Android:

```sh
ssh pi@gatekeeper 'sqlite3 -header -column \
  ~/.config/mqtt-bot/history.sqlite "
SELECT datetime(ts,\"unixepoch\",\"localtime\") AS t,
       cache_size_bytes AS cache_b,
       start_serial     AS srl,
       nav_to_paint_ms  AS paint,
       nav_to_load_ms   AS load,
       nav_to_listener_ms AS lst,
       nav_to_listener_ms - nav_to_load_ms AS listener_block_ms
FROM app_telemetry
WHERE app_build_ts >= 1779494700
ORDER BY ts DESC LIMIT 20"'
```

Rows with the same `cache_size_bytes` / `start_serial` cluster are
the same device. The desktop client's `listener_block_ms` should
remain in the low hundreds; an Android row with `listener_block_ms`
≫ 1 000 confirms the hypothesis.

## Related observations

- The fact that the user-reported "10 s every open" persists even
  with `startSerial > 0` (our cache-serial fix from commit
  `02d41d1`) suggests the Android client's listener-registration
  cost is not proportional to the *number* of replayed updates —
  it's proportional to a fixed scan/materialisation cost that fires
  regardless of `startSerial`. If that's confirmed, the
  `startSerial` optimisation only helps the desktop client.
- An upstream Delta Chat issue would be the right follow-up if the
  fix here works — it would let other webxdc developers benefit
  without contorting their app structure.

## Result (2026-05-18, build `1779494700`)

**Hypothesis refuted.** Reverted in build `1779494800`.

Two observations from the field test:

1. **The deferred-boot pattern broke the data flow.** With
   `setUpdateListener` registered inside the `window.load` handler,
   the inbound handler never fired on a fresh `/apps` install. The
   user saw the rendered chrome (header, buttons, energy grid, rules
   section) but `state.devices` stayed `{}`: empty device picker,
   placeholder "—" everywhere, "Refresh" button had no effect.
   No `app_telemetry` rows were generated on the Pi, indicating that
   the deferred `setTimeout` never fired either. Either the
   `window.load` event isn't reliably reaching us in the Delta Chat
   webview, or it does but `webxdc.sendUpdate` from within the load
   handler isn't reaching the bot. The chess app survives this
   pattern because its app is self-contained and doesn't need
   round-trip data from a remote bot; ours does.

2. **The Android delay is non-deterministic.** The user reports the
   same app *sometimes* opens fast on Android and sometimes takes
   ~8 s, with no behavioural change between opens. After explicit
   delete + reinstall via `/apps`, the cold start was still ~8 s.
   That non-determinism rules out a fixed cost we could pin to
   `setUpdateListener` registration (which would be either always
   slow or always fast for identical chat state). It's consistent
   with the OS deciding to keep or kill the Android System WebView
   process between opens — when warm: fast; when cold: 8 s of
   spawn before navigation start. `performance.now()` cannot
   observe time before navigation start, so this cost is invisible
   to any in-page instrumentation.

### What the test actually showed us

- Whatever takes 8 s on Android lives **before** the page begins
  parsing, not inside `setUpdateListener`. Otherwise the deferred
  pattern would have shifted the delay to *after* first paint
  rather than leaving the user with permanently-empty state.
- The earlier conclusion from telemetry — that
  `nav_to_paint_ms` is consistently sub-300 ms even on the
  suspected-Android row — was already pointing at pre-navigation
  cost. The chess comparison provided a tempting alternative
  explanation that turned out to be a coincidence (chess differs
  from us in many ways; "post-`onload` listener" was just one of
  them, and not the one that matters).

### Next steps

- Nothing actionable in this codebase. Pre-navigation WebView
  spawn is owned by Android System WebView + Delta Chat.
- An upstream Delta Chat for Android issue is the right home for
  this finding — a warm WebView pool (or even a single retained
  instance) would benefit every webxdc app, not just ours.
- The telemetry columns added during this investigation
  (`nav_to_head_ms`, `nav_to_script_ms`, `nav_to_render_ms`,
  `nav_to_paint_ms`) stay in `app_telemetry`. They're cheap to
  collect and useful as a regression guard. The deferred-boot
  columns `nav_to_load_ms` and `nav_to_listener_ms` remain in the
  schema (idempotent ALTER history is easier to read than to
  rewrite); new rows will leave them NULL.
