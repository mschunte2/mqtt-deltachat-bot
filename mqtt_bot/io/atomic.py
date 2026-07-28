"""Crash- and thread-safe file replacement.

The bot persists four small JSON documents (`rules.json`,
`app_msgids.json`, `baselines.json`, and anything added later). Each was
originally written as::

    tmp = path.with_suffix(".tmp")
    tmp.write_text(payload)
    os.replace(tmp, path)

which has three defects this module fixes:

1. **A fixed temp path is not thread-safe.** `save_rules` is reachable
   from the Delta Chat handler thread (schedule/cancel), the rules
   sweeper (tick_time) and the MQTT callback thread (state rules firing
   on a status update). Two writers sharing one `.tmp` truncate each
   other, and `os.replace` then publishes a spliced or half-written
   document. The loaders treat a `JSONDecodeError` as "empty", so a
   collision silently discards every persisted rule. We give each write
   a unique temp name *and* serialise writers per target path.

2. **`os.replace` is atomic for the rename, not for the data.** Without
   an `fsync` the tmp file's contents may still be in the page cache
   when the rename lands. A power cut on an always-on Pi — the expected
   failure mode here — can leave a correctly-named, zero-length file.
   We fsync the file before renaming and the directory afterwards.

3. **No control over the resulting mode.** Callers holding chat ids or
   home power readings want 0600 rather than whatever the umask says.

Not pure (that is the point); lives under `io/` accordingly.
"""

from __future__ import annotations

import os
import tempfile
import threading
from pathlib import Path

# One lock per target path, so two writers to rules.json serialise while a
# writer to app_msgids.json proceeds in parallel. Keyed by the resolved
# parent + name: the set of persisted files is small and fixed at startup,
# so this dict does not grow without bound.
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.Lock:
    key = str(path)
    with _locks_guard:
        lock = _locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _locks[key] = lock
        return lock


def write_text(path: Path | str, data: str, *, mode: int | None = None) -> None:
    """Replace `path` with `data`, atomically and durably.

    Serialised against concurrent `write_text` calls for the same path.
    `mode`, when given, is applied to the file before it is moved into
    place, so the content is never briefly visible under a laxer mode.
    Leaves no temp file behind on either the success or the failure path;
    on failure the previous contents of `path` are untouched.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with _lock_for(p):
        # Same directory as the target: os.replace cannot cross filesystems.
        fd, tmp_name = tempfile.mkstemp(dir=str(p.parent),
                                        prefix=f".{p.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            if mode is not None:
                os.chmod(tmp_name, mode)
            os.replace(tmp_name, p)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

        # Persist the directory entry itself, so the rename survives a
        # power cut too. Best-effort: not every platform permits it.
        try:
            dir_fd = os.open(str(p.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass


#: Mode for the bot's persisted state. These files are not world
#: business: `app_msgids.json` holds chat ids, `rules.json` holds what
#: the household has automated, and `history.sqlite` alongside them is a
#: per-minute power series — a high-resolution occupancy signal. They
#: previously inherited the umask (0664 in a 0775 directory on the live
#: host), so any second account on the Pi could read them.
STATE_FILE_MODE = 0o600
