"""Tiny templating: substitute {identifier} or {identifier:fmt}.

Unlike str.format_map, literal braces in the template that aren't followed
by a Python identifier are left as-is. This matters because device-class
JSON payloads contain literal { } characters (e.g. an MQTT RPC payload
like '{"id":1,"src":"{client_id}",...,"params":{"id":0}}'). Only the
{client_id} placeholder should be substituted; the rest must survive.

Missing keys render as empty strings.
"""

from __future__ import annotations

import re
from typing import Any

# {name} or {name:fmt} where name is a Python identifier and fmt is
# anything up to the next closing brace (no nesting allowed).
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::([^{}]*))?\}")


def render(template: str, ctx: dict[str, Any]) -> str:
    def _sub(m: re.Match[str]) -> str:
        key = m.group(1)
        spec = m.group(2)
        if key not in ctx:
            return ""
        value = ctx[key]
        if spec:
            try:
                return format(value, spec)
            except (TypeError, ValueError):
                return str(value)
        return str(value)
    return _PLACEHOLDER_RE.sub(_sub, template)
