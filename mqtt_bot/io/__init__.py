"""I/O layer: anything with a side effect.

History (SQLite), baselines (JSON), MQTT client (paho wrapper),
webxdc I/O (Delta Chat RPC), publisher (the single outbound
stream that pushes snapshots to apps). Each module owns its
side-effecting resource exclusively; the rest of the codebase
imports them as injected dependencies via ``TwinDeps``.
"""
