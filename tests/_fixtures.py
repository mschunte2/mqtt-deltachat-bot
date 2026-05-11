"""Shared fixtures used by multiple test modules.

- ``CLASS_JSON_OK``: a class definition shaped like the result of
  ``config.load()`` reading ``devices/<class>/class.json``. Covers
  every field the engine consumes; tests override per-case.
- ``_build_twin``: constructs a single ``PlugTwin`` against the
  fixture class + a stub ``TwinDeps``. Returns ``(twin, calls, cfg)``
  where ``calls`` is a dict that records every side-effecting
  invocation the twin makes.
- ``_FakeHistory``: minimal ``History`` stand-in for snapshot tests
  that need ``query_power`` / ``aenergy_at`` / etc. to return
  deterministic empties.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from mqtt_bot.util import config as cfg_mod  # noqa: E402 — sys.path set by tests/__init__.py
from mqtt_bot.core import twin as plug_mod  # noqa: E402


CLASS_JSON_OK = {
    "name": "tplug",
    "app_id": "tplug",
    "description": "test plug",
    "subscribe": [
        {"suffix": "online", "format": "text"},
        {"suffix": "status/switch:0", "format": "json"},
    ],
    "commands": {
        "on":  {"suffix": "command/switch:0", "payload": "on"},
        "off": {"suffix": "command/switch:0", "payload": "off"},
    },
    "state_fields": {
        "online":  {"from_suffix": "online", "extract": "bool_text"},
        "output":  {"from_suffix": "status/switch:0", "json_path": "output"},
        "apower":  {"from_suffix": "status/switch:0", "json_path": "apower"},
        "aenergy": {"from_suffix": "status/switch:0",
                    "json_path": "aenergy.total"},
    },
    "chat_events": [
        {"type": "on_change", "field": "output",
         "values": {"true": "💡 {name} ON", "false": "💡 {name} OFF"}},
        {"type": "threshold", "field": "apower",
         "limit_param": "power_threshold_watts",
         "duration_param": "power_threshold_duration_s",
         "above": "⚠️ {name} {value:.0f}W for {seconds}s",
         "below": "✅ {name} cleared"},
    ],
    "auto_off": {
        "command": "off",
        "default_idle_field": "apower",
        "default_idle_threshold": 5.0,
        "default_idle_duration": 60,
        "default_consumed_field": "apower",
        "default_consumed_threshold_wh": 5.0,
        "default_consumed_window_s": 600,
        "default_avg_field": "apower",
        "default_avg_threshold_w": 5.0,
        "default_avg_window_s": 600,
        "trigger_messages": {
            "timer": "🕐 {name} timer", "tod": "📅 {name} {hh}:{mm}",
            "idle": "💤 {name} idle", "consumed": "🔋 {name} consumed",
            "avg": "📉 {name} avg {value:.1f}W",
        },
    },
    "auto_on": {
        "command": "on",
        "trigger_messages": {
            "tod": "📅 {name} on at {hh}:{mm}",
        },
    },
}


def _build_twin(class_overrides=None, params=None, allowed_chats=(12,),
                history=None):
    """Build a single ``PlugTwin`` against an in-memory class+device
    config and a stub ``TwinDeps``.

    Returns ``(twin, calls, cfg)`` where ``calls`` is a dict recording
    every side-effect invocation: published MQTT, posted chat lines,
    broadcasts, save_rules count, react reactions, baseline saves.
    """
    cls_def = json.loads(json.dumps(CLASS_JSON_OK))
    if class_overrides:
        cls_def.update(class_overrides)
    tmp = Path(tempfile.mkdtemp())
    cls_dir = tmp / "devices" / "tplug"
    cls_dir.mkdir(parents=True)
    (cls_dir / "class.json").write_text(json.dumps(cls_def))
    instance = {"devices": [{
        "name": "kitchen", "class": "tplug",
        "topic_prefix": "p/kitchen",
        "allowed_chats": list(allowed_chats),
        **(params or {}),
    }]}
    inst_path = tmp / "devices.json"
    inst_path.write_text(json.dumps(instance))
    cfg = cfg_mod.load(devices_dir=tmp / "devices", instances_file=inst_path)

    calls = {"published": [], "posted": [], "broadcasts": [],
             "saves": 0, "reactions": []}
    deps = plug_mod.TwinDeps(
        mqtt_publish=lambda t, p: calls["published"].append((t, p)),
        post_to_chats=lambda dev, txt: calls["posted"].append((dev.name, txt)),
        broadcast=lambda name=None: calls["broadcasts"].append(name),
        save_rules=lambda: calls.__setitem__("saves", calls["saves"] + 1),
        save_baselines=lambda: calls.__setitem__(
            "baseline_saves", calls.get("baseline_saves", 0) + 1),
        react=lambda mid, e: calls["reactions"].append((mid, e)),
        history=history,
        client_id="tester",
    )
    twin = plug_mod.PlugTwin(
        cls=cfg.classes["tplug"], cfg=cfg.devices["kitchen"], deps=deps,
    )
    return twin, calls, cfg


class _FakeHistory:
    """Minimal ``History`` stand-in. Defaults return empty/None;
    tests that need specific values set the corresponding attribute
    or override the method:

        h = _FakeHistory()
        h.consumed_wh = 42.0          # what energy_consumed_in returns
        h.consumed_earliest_offset = 0  # earliest_ts = since + this offset
        h.samples_raw_rows = [...]    # what query_samples_raw returns
    """

    def __init__(self):
        self.consumed_wh: float = 0.0
        self.consumed_earliest_offset: int | None = None  # None → no data
        self.samples_raw_rows: list = []

    def query_power(self, *_a, **_kw):
        return (60, [])

    def daily_energy_kwh(self, *_a, **kw):
        return [(0, 0.0)] * int(kw.get("days", 30))

    def energy_consumed_in(self, _device, since_ts, _until_ts):
        if self.consumed_earliest_offset is None:
            return (0.0, None)
        return (self.consumed_wh,
                int(since_ts) + self.consumed_earliest_offset)

    def aenergy_at(self, *_a, **_kw):
        return None

    def query_samples_raw(self, *_a, **_kw):
        return self.samples_raw_rows

    def record_offset_event(self, *_a, **_kw):
        pass

    def write_sample(self, *_a, **_kw):
        pass

    def record_status(self, *_a, **_kw):
        pass
