"""Server-side build identity.

The webxdc app has stamped `APP_BUILD_TS` since the cold-start
investigation, so the *client* build in any chat is identifiable
exactly — while the bot reported nothing at all. When a user says "it
stopped working after the upgrade", answering that needed SSH plus
`git rev-parse`.

Resolved from git at import time, with the result cached: the tree is
present on every deployment (the documented update flow is `git pull`
+ `build-xdc.sh` on the target), and a stamped file would need a build
step this project deliberately doesn't have. Falls back to "unknown"
rather than raising — never let identity reporting break startup.
"""

from __future__ import annotations

import subprocess
from functools import lru_cache
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(
            ("git", "-C", str(_ROOT), *args),
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


@lru_cache(maxsize=1)
def version() -> str:
    """Human-readable build id, e.g. `v0.2.1-43-g8bee740` or
    `8bee740-dirty`, falling back to "unknown" outside a git checkout."""
    described = _git("describe", "--tags", "--always", "--dirty")
    if described:
        return described
    sha = _git("rev-parse", "--short", "HEAD")
    return sha or "unknown"


__all__ = ["version"]
