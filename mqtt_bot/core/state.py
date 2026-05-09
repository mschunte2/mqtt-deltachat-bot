"""Per-device state cache + extraction from MQTT payloads.

Pure module: callers feed in raw payloads, get back a dict of updated
fields. The engine owns the cache itself and side-effects on transitions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from ..util.config import DeviceClass


@dataclass
class DeviceState:
    """Latest known values per state field. None = never observed."""
    fields: dict[str, Any] = field(default_factory=dict)
    last_update_ts: int = 0

    def get(self, name: str) -> Any:
        return self.fields.get(name)

    def set(self, name: str, value: Any) -> None:
        self.fields[name] = value


def extract(cls: DeviceClass, suffix: str, payload: bytes | str) -> dict[str, Any]:
    """Return a dict of {field_name: new_value} for every state_field
    sourced from this suffix. Silently skips fields whose extractor fails
    on this particular payload (the next message will retry).
    """
    matches = {n: f for n, f in cls.state_fields.items() if f.from_suffix == suffix}
    if not matches:
        return {}

    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    out: dict[str, Any] = {}

    # Resolve JSON once if any field needs it.
    parsed_json: Any = None
    needs_json = any(f.json_path for f in matches.values())
    if needs_json:
        try:
            parsed_json = json.loads(text)
        except json.JSONDecodeError:
            parsed_json = None

    for fname, fdef in matches.items():
        if fdef.extract == "bool_text":
            v = text.strip().lower()
            if v in {"true", "1", "on"}:
                out[fname] = True
            elif v in {"false", "0", "off"}:
                out[fname] = False
            # any other value: skip (likely a stale or malformed payload)
        elif fdef.json_path:
            if parsed_json is None:
                continue
            val = _walk(parsed_json, fdef.json_path)
            if val is not None:
                out[fname] = val
    return out


def _walk(obj: Any, dotted: str) -> Any:
    cur: Any = obj
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur
