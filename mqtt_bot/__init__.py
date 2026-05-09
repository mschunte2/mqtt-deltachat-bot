"""mqtt_bot — declarative Delta Chat ↔ MQTT bridge.

Three sub-packages mirror the architectural rule list in CLAUDE.md:

- ``mqtt_bot.core`` — per-device digital twin, rules engine,
  state extraction, snapshot assembly. The "what to do" layer.
- ``mqtt_bot.io`` — modules with side effects (DB, MQTT, FS,
  webxdc). The "talk to the outside world" layer.
- ``mqtt_bot.util`` — pure helpers: config loader, duration
  parser, template substitution, permission gate. No I/O,
  deterministic.

Plus three orchestration helpers at the package root:

- ``mqtt_bot.commands``    — pure text-command parser.
- ``mqtt_bot.formatters``  — chat-reply display helpers.
- ``mqtt_bot.rehydrate``   — rule transient-state backfill on
  bot startup.

The entry point ``bot.py`` lives at the project root (so
``python -m bot`` and the systemd unit don't change). It wires
everything together.
"""
