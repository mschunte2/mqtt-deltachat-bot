"""Load and validate device-class components + the user's instance file.

Layout (auto-discovered):
    devices/<class>/class.json   -- one per device class component
    devices.json                 -- user instances (referenced by `class` name)

Pure module: reads files, returns dataclasses, no other side effects.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,15}$")
SUFFIX_FORMATS = {"text", "json"}


# --- Class-level pieces ---------------------------------------------------

@dataclass(frozen=True)
class SubscribeEntry:
    suffix: str
    format: str
    optional: bool = False


@dataclass(frozen=True)
class Command:
    suffix: str
    payload: str


@dataclass(frozen=True)
class StateFieldDef:
    from_suffix: str
    extract: str | None = None      # "bool_text" or None (json)
    json_path: str | None = None


@dataclass(frozen=True)
class ChatEventRule:
    """Single discriminated dataclass (type field).

    type="on_change":   uses field, values
    type="threshold":   uses field, limit_param, duration_param, above, below
    """
    type: str
    field: str
    values: dict[str, str] = field(default_factory=dict)
    limit_param: str | None = None
    duration_param: str | None = None
    above: str | None = None
    below: str | None = None


@dataclass(frozen=True)
class AutoOffConfig:
    command: str
    default_idle_field: str
    default_idle_threshold: float
    default_idle_duration: int
    default_consumed_field: str
    default_consumed_threshold_wh: float
    default_consumed_window_s: int
    default_avg_field: str
    default_avg_threshold_w: float
    default_avg_window_s: int
    trigger_messages: dict[str, str]


@dataclass(frozen=True)
class AutoOnConfig:
    command: str
    trigger_messages: dict[str, str]


@dataclass(frozen=True)
class DeviceClass:
    name: str
    app_id: str
    description: str
    subscribe: tuple[SubscribeEntry, ...]
    commands: dict[str, Command]
    state_fields: dict[str, StateFieldDef]
    chat_events: tuple[ChatEventRule, ...]
    auto_off: AutoOffConfig | None
    auto_on: AutoOnConfig | None


# --- Device + Config ------------------------------------------------------

@dataclass(frozen=True)
class Device:
    name: str
    class_name: str
    topic_prefix: str
    description: str
    allowed_chats: tuple[int, ...]
    params: dict[str, Any]


@dataclass(frozen=True)
class Config:
    classes: dict[str, DeviceClass]
    devices: dict[str, Device]

    def device_class(self, device: Device) -> DeviceClass:
        return self.classes[device.class_name]


# --- Loader ---------------------------------------------------------------

class ConfigError(Exception):
    pass


def load(devices_dir: str | Path = "devices",
         instances_file: str | Path = "devices.json") -> Config:
    """Discover class.json files under devices_dir; load instances from
    instances_file; cross-validate.
    """
    classes = _discover_classes(Path(devices_dir))
    if not classes:
        raise ConfigError(
            f"no device classes found under {devices_dir}/. "
            f"Each class lives in its own subdirectory with a class.json."
        )

    raw = _read_json(Path(instances_file))
    raw_devices = raw.get("devices")
    if not isinstance(raw_devices, list) or not raw_devices:
        raise ConfigError(f"{instances_file}: 'devices' must be a non-empty array")

    devices: dict[str, Device] = {}
    seen_prefix: dict[str, str] = {}
    for ddef in raw_devices:
        d = _parse_device(ddef, classes)
        if d.name in devices:
            raise ConfigError(f"duplicate device name: {d.name!r}")
        if d.topic_prefix in seen_prefix:
            raise ConfigError(
                f"duplicate topic_prefix {d.topic_prefix!r} on devices "
                f"{seen_prefix[d.topic_prefix]!r} and {d.name!r}"
            )
        seen_prefix[d.topic_prefix] = d.name
        devices[d.name] = d

    return Config(classes=classes, devices=devices)


def _read_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError as ex:
        raise ConfigError(f"file not found: {p}") from ex
    except json.JSONDecodeError as ex:
        raise ConfigError(f"{p}: invalid JSON: {ex}") from ex


def _discover_classes(devices_dir: Path) -> dict[str, DeviceClass]:
    out: dict[str, DeviceClass] = {}
    if not devices_dir.is_dir():
        return out
    for entry in sorted(devices_dir.iterdir()):
        if not entry.is_dir():
            continue
        class_path = entry / "class.json"
        if not class_path.is_file():
            continue
        raw = _read_json(class_path)
        cls_name = raw.get("name") or entry.name
        if cls_name in out:
            raise ConfigError(f"duplicate device class name: {cls_name!r}")
        out[cls_name] = _parse_class(cls_name, raw)
    return out


# --- Class parsing helpers ------------------------------------------------

def _parse_class(name: str, raw: dict) -> DeviceClass:
    if not isinstance(raw, dict):
        raise ConfigError(f"class {name}: must be an object")

    subscribe = tuple(
        _parse_subscribe(name, i, e) for i, e in enumerate(raw.get("subscribe", []))
    )
    if not subscribe:
        raise ConfigError(f"class {name}: subscribe must be non-empty")

    cmds_raw = raw.get("commands") or {}
    if not isinstance(cmds_raw, dict) or not cmds_raw:
        raise ConfigError(f"class {name}: commands must be non-empty")
    commands = {k: _parse_command(name, k, v) for k, v in cmds_raw.items()}

    fields_raw = raw.get("state_fields") or {}
    if not isinstance(fields_raw, dict):
        raise ConfigError(f"class {name}: state_fields must be an object")
    state_fields = {k: _parse_field(name, k, v) for k, v in fields_raw.items()}

    events = tuple(
        _parse_event(name, i, e) for i, e in enumerate(raw.get("chat_events", []))
    )

    auto_off_raw = raw.get("auto_off")
    auto_off = _parse_auto_off(name, auto_off_raw, commands) if auto_off_raw else None

    auto_on_raw = raw.get("auto_on")
    auto_on = _parse_auto_on(name, auto_on_raw, commands) if auto_on_raw else None

    return DeviceClass(
        name=name,
        app_id=str(raw.get("app_id", name)),
        description=str(raw.get("description", "")),
        subscribe=subscribe,
        commands=commands,
        state_fields=state_fields,
        chat_events=events,
        auto_off=auto_off,
        auto_on=auto_on,
    )


def _parse_subscribe(cls: str, idx: int, raw: Any) -> SubscribeEntry:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.subscribe[{idx}]: must be object")
    suffix = raw.get("suffix")
    fmt = raw.get("format")
    if not isinstance(suffix, str) or not suffix:
        raise ConfigError(f"{cls}.subscribe[{idx}]: suffix required")
    if fmt not in SUFFIX_FORMATS:
        raise ConfigError(f"{cls}.subscribe[{idx}]: format must be one of {SUFFIX_FORMATS}")
    return SubscribeEntry(suffix=suffix, format=fmt, optional=bool(raw.get("optional", False)))


def _parse_command(cls: str, name: str, raw: Any) -> Command:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.commands.{name}: must be object")
    suffix = raw.get("suffix")
    payload = raw.get("payload")
    if not isinstance(suffix, str) or not suffix:
        raise ConfigError(f"{cls}.commands.{name}: suffix required")
    if not isinstance(payload, str):
        raise ConfigError(f"{cls}.commands.{name}: payload required")
    return Command(suffix=suffix, payload=payload)


def _parse_field(cls: str, name: str, raw: Any) -> StateFieldDef:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.state_fields.{name}: must be object")
    from_suffix = raw.get("from_suffix")
    if not isinstance(from_suffix, str) or not from_suffix:
        raise ConfigError(f"{cls}.state_fields.{name}: from_suffix required")
    extract = raw.get("extract")
    json_path = raw.get("json_path")
    if extract is None and not json_path:
        raise ConfigError(f"{cls}.state_fields.{name}: needs extract or json_path")
    if extract is not None and extract not in {"bool_text"}:
        raise ConfigError(f"{cls}.state_fields.{name}: unknown extract {extract!r}")
    return StateFieldDef(from_suffix=from_suffix, extract=extract, json_path=json_path)


def _parse_event(cls: str, idx: int, raw: Any) -> ChatEventRule:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.chat_events[{idx}]: must be object")
    t = raw.get("type")
    fld = raw.get("field")
    if t not in {"on_change", "threshold"}:
        raise ConfigError(f"{cls}.chat_events[{idx}]: type must be on_change|threshold")
    if not isinstance(fld, str):
        raise ConfigError(f"{cls}.chat_events[{idx}]: field required")
    if t == "on_change":
        values = raw.get("values") or {}
        if not isinstance(values, dict) or not values:
            raise ConfigError(f"{cls}.chat_events[{idx}]: values required")
        return ChatEventRule(type=t, field=fld, values=dict(values))
    limit_param = raw.get("limit_param")
    duration_param = raw.get("duration_param")
    above = raw.get("above")
    below = raw.get("below")
    if not all(isinstance(x, str) and x for x in (limit_param, duration_param, above, below)):
        raise ConfigError(
            f"{cls}.chat_events[{idx}]: threshold needs limit_param, duration_param, above, below"
        )
    return ChatEventRule(
        type=t, field=fld,
        limit_param=limit_param, duration_param=duration_param,
        above=above, below=below,
    )


def _parse_auto_off(cls: str, raw: Any, commands: dict[str, Command]) -> AutoOffConfig:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.auto_off: must be object")
    cmd = raw.get("command")
    if cmd not in commands:
        raise ConfigError(f"{cls}.auto_off.command={cmd!r} not in commands")
    msgs = raw.get("trigger_messages") or {}
    if not isinstance(msgs, dict):
        raise ConfigError(f"{cls}.auto_off.trigger_messages must be object")
    required = {"timer", "tod", "idle", "consumed", "avg"}
    missing = required - set(msgs.keys())
    if missing:
        raise ConfigError(f"{cls}.auto_off.trigger_messages missing: {sorted(missing)}")
    return AutoOffConfig(
        command=cmd,
        default_idle_field=str(raw.get("default_idle_field", "apower")),
        default_idle_threshold=float(raw.get("default_idle_threshold", 5)),
        default_idle_duration=int(raw.get("default_idle_duration", 60)),
        default_consumed_field=str(raw.get("default_consumed_field", "apower")),
        default_consumed_threshold_wh=float(raw.get("default_consumed_threshold_wh", 5)),
        default_consumed_window_s=int(raw.get("default_consumed_window_s", 600)),
        default_avg_field=str(raw.get("default_avg_field", "apower")),
        default_avg_threshold_w=float(raw.get("default_avg_threshold_w", 5)),
        default_avg_window_s=int(raw.get("default_avg_window_s", 600)),
        trigger_messages=dict(msgs),
    )


def _parse_auto_on(cls: str, raw: Any, commands: dict[str, Command]) -> AutoOnConfig:
    if not isinstance(raw, dict):
        raise ConfigError(f"{cls}.auto_on: must be object")
    cmd = raw.get("command")
    if cmd not in commands:
        raise ConfigError(f"{cls}.auto_on.command={cmd!r} not in commands")
    msgs = raw.get("trigger_messages") or {}
    if not isinstance(msgs, dict):
        raise ConfigError(f"{cls}.auto_on.trigger_messages must be object")
    required = {"tod"}
    missing = required - set(msgs.keys())
    if missing:
        raise ConfigError(f"{cls}.auto_on.trigger_messages missing: {sorted(missing)}")
    return AutoOnConfig(command=cmd, trigger_messages=dict(msgs))


# --- Device parsing -------------------------------------------------------

_RESERVED_DEVICE_KEYS = {"name", "class", "topic_prefix", "description", "allowed_chats"}


def _parse_device(raw: Any, classes: dict[str, DeviceClass]) -> Device:
    if not isinstance(raw, dict):
        raise ConfigError("devices entry must be object")
    name = raw.get("name")
    cls = raw.get("class")
    prefix = raw.get("topic_prefix")
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ConfigError(f"device name invalid: {name!r} (must match {NAME_RE.pattern})")
    if cls not in classes:
        raise ConfigError(f"device {name!r}: class {cls!r} not in known classes "
                          f"({sorted(classes)})")
    if not isinstance(prefix, str) or not prefix:
        raise ConfigError(f"device {name!r}: topic_prefix required")
    chats_raw = raw.get("allowed_chats") or []
    if not isinstance(chats_raw, list) or not all(isinstance(c, int) for c in chats_raw):
        raise ConfigError(f"device {name!r}: allowed_chats must be list of int")
    params = {k: v for k, v in raw.items()
              if k not in _RESERVED_DEVICE_KEYS and not k.startswith("_")}
    return Device(
        name=name,
        class_name=cls,
        topic_prefix=prefix.rstrip("/"),
        description=str(raw.get("description", "")),
        allowed_chats=tuple(int(c) for c in chats_raw),
        params=params,
    )


# --- Env helpers ----------------------------------------------------------

def parse_allowed_chats(env_value: str | None) -> set[int]:
    if not env_value:
        return set()
    out: set[int] = set()
    for part in env_value.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(int(part))
    return out
