"""Test-suite package init.

Run with: ``python3 -m unittest discover tests``

Two pieces of setup live here so they happen before any test module
imports a production module:

1. ``sys.path`` is extended with the project root so ``import config``,
   ``import plug``, etc. resolve.
2. ``deltachat2`` and ``paho.mqtt.client`` are stubbed in
   ``sys.modules`` because some production modules import them at top
   level, and neither is a test dependency. Tests inject their own
   fakes over ``MqttClient._client``; the stub only has to satisfy the
   import and the two module-level names the wrapper reads.
"""

from __future__ import annotations

import sys
import types as _types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


class _MsgData:
    """Stand-in for ``deltachat2.MsgData`` — accepts the same kwargs
    the bot uses (``text=``, ``file=``) and stores them as attributes
    so test stubs can inspect calls."""

    def __init__(self, text=None, file=None):
        self.text = text
        self.file = file


_dc_stub = _types.ModuleType("deltachat2")
_dc_stub.MsgData = _MsgData
sys.modules.setdefault("deltachat2", _dc_stub)


def _install_paho_stub() -> None:
    """Minimal paho.mqtt.client so mqtt_client.py imports."""
    if "paho.mqtt.client" in sys.modules:
        return

    class _Client:
        """Inert stand-in. Tests replace `MqttClient._client` with their
        own fake right after construction, so this only has to absorb
        the calls __init__ makes."""

        def __init__(self, *a, **kw):
            pass

        def __getattr__(self, _name):
            return lambda *a, **kw: None

    client_mod = _types.ModuleType("paho.mqtt.client")
    client_mod.Client = _Client
    client_mod.MQTT_ERR_SUCCESS = 0
    client_mod.CallbackAPIVersion = _types.SimpleNamespace(VERSION1=1)

    mqtt_mod = _types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod
    paho_mod = _types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod

    sys.modules.setdefault("paho", paho_mod)
    sys.modules.setdefault("paho.mqtt", mqtt_mod)
    sys.modules["paho.mqtt.client"] = client_mod


try:                                     # prefer the real package if present
    import paho.mqtt.client  # noqa: F401
except ImportError:
    _install_paho_stub()
