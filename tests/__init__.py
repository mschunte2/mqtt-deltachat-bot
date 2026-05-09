"""Test-suite package init.

Run with: ``python3 -m unittest discover tests``

Two pieces of setup live here so they happen before any test module
imports a production module:

1. ``sys.path`` is extended with the project root so ``import config``,
   ``import plug``, etc. resolve.
2. ``deltachat2`` is stubbed in ``sys.modules`` because some production
   modules (``plug.py``, ``bot.py``) import it at top level, and the
   real package isn't a test dependency.
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
