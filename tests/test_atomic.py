"""Tests for the shared atomic-write helper.

The concurrency test is the point of this module: before `atomic.write_text`
existed, `rules.py`, `baselines.py` and `webxdc_io.py` each wrote to a single
fixed `<name>.tmp` path from up to three threads (DC handler, rules sweeper,
MQTT callback). Two interleaved writers produced a mixed or truncated file,
and the loaders turn a JSONDecodeError into "no rules at all".
"""

import json
import os
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

from mqtt_bot.io import atomic


class TestAtomicWrite(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)
        self.addCleanup(self._dir.cleanup)

    def test_writes_content(self):
        p = self.dir / "x.json"
        atomic.write_text(p, '{"a": 1}')
        self.assertEqual(json.loads(p.read_text()), {"a": 1})

    def test_overwrites_existing(self):
        p = self.dir / "x.json"
        p.write_text("stale")
        atomic.write_text(p, "fresh")
        self.assertEqual(p.read_text(), "fresh")

    def test_creates_parent_directory(self):
        p = self.dir / "nested" / "deeper" / "x.json"
        atomic.write_text(p, "hi")
        self.assertEqual(p.read_text(), "hi")

    def test_leaves_no_temp_files(self):
        p = self.dir / "x.json"
        for i in range(5):
            atomic.write_text(p, str(i))
        self.assertEqual([f.name for f in self.dir.iterdir()], ["x.json"])

    def test_applies_mode_when_given(self):
        p = self.dir / "x.json"
        atomic.write_text(p, "secret", mode=0o600)
        self.assertEqual(os.stat(p).st_mode & 0o777, 0o600)

    def test_inherits_umask_when_mode_is_none(self):
        p = self.dir / "x.json"
        atomic.write_text(p, "plain")
        # Not asserting an exact value (umask varies); just that we did not
        # silently force 0600 on callers that did not ask for it.
        self.assertTrue(os.stat(p).st_mode & 0o777)

    def test_failed_replace_leaves_original_intact_and_no_temp(self):
        p = self.dir / "x.json"
        atomic.write_text(p, "good")

        class Boom(Exception):
            pass

        def boom(*_a, **_kw):
            raise Boom("rename failed")

        with unittest.mock.patch.object(atomic.os, "replace", boom):
            with self.assertRaises(Boom):
                atomic.write_text(p, "bad")

        self.assertEqual(p.read_text(), "good")
        self.assertEqual([f.name for f in self.dir.iterdir()], ["x.json"])

    def test_concurrent_writers_never_produce_a_corrupt_file(self):
        """The regression test for the rules.json data-loss bug.

        Many threads write differently-sized valid JSON documents to the
        same path while a reader parses it continuously. Every read must
        yield one writer's complete document — never a splice of two.
        """
        p = self.dir / "rules.json"
        payloads = [
            json.dumps({"jobs": [{"i": i, "pad": "x" * (i * 700)}]})
            for i in range(1, 9)
        ]
        atomic.write_text(p, payloads[0])

        stop = threading.Event()
        errors: list[Exception] = []

        def writer(body):
            try:
                for _ in range(60):
                    atomic.write_text(p, body)
            except Exception as ex:  # pragma: no cover - failure path
                errors.append(ex)

        def reader():
            while not stop.is_set():
                try:
                    text = p.read_text()
                except FileNotFoundError:
                    errors.append(AssertionError("path vanished mid-write"))
                    return
                try:
                    json.loads(text)
                except json.JSONDecodeError as ex:
                    errors.append(
                        AssertionError(f"torn read ({ex}): {text[:80]!r}"))
                    return

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        writers = [threading.Thread(target=writer, args=(b,)) for b in payloads]
        for t in writers:
            t.start()
        for t in writers:
            t.join()
        stop.set()
        for t in readers:
            t.join()

        self.assertEqual(errors, [])
        self.assertIn(json.loads(p.read_text())["jobs"][0]["i"], range(1, 9))
        self.assertEqual([f.name for f in self.dir.iterdir()], ["rules.json"])



class TestStateFileModes(unittest.TestCase):
    """State files inherited the umask (0664 in a 0775 directory on the
    live host). app_msgids.json holds chat ids, rules.json holds what
    the household has automated, and history.sqlite next to them is a
    per-minute power series — a high-resolution occupancy signal."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)
        self.addCleanup(self._dir.cleanup)

    def test_state_file_mode_is_owner_only(self):
        self.assertEqual(atomic.STATE_FILE_MODE, 0o600)

    def test_rules_json_is_written_owner_only(self):
        from mqtt_bot.core import rules as sched
        from mqtt_bot.core.twins import TwinRegistry
        from tests._fixtures import _build_twin

        twin, _, _ = _build_twin()
        twin.schedule("off", sched.ScheduledPolicy(timer_seconds=600), 12)
        p = self.dir / "rules.json"
        sched.save_all(TwinRegistry([twin]), p)
        self.assertEqual(os.stat(p).st_mode & 0o777, 0o600)

    def test_baselines_json_is_written_owner_only(self):
        from mqtt_bot.core.twins import TwinRegistry
        from mqtt_bot.io import baselines
        from tests._fixtures import _build_twin

        twin, _, _ = _build_twin()
        p = self.dir / "baselines.json"
        baselines.save(TwinRegistry([twin]), p)
        self.assertEqual(os.stat(p).st_mode & 0o777, 0o600)

    def test_app_msgids_is_written_owner_only(self):
        from mqtt_bot.io.webxdc_io import WebxdcIO
        state = self.dir / "state"
        devices = self.dir / "devices"
        state.mkdir()
        (devices / "shelly_plug").mkdir(parents=True)
        (devices / "shelly_plug" / "shelly_plug.xdc").write_bytes(b"PK\x03\x04")

        class _Rpc:
            def send_msg(self, *a, **kw):
                return 4242

            def delete_messages_for_all(self, *a, **kw):
                pass

        io = WebxdcIO(state, devices)
        io.send_apps(type("B", (), {"rpc": _Rpc()})(), accid=1, chat_id=12,
                     classes_visible={"shelly_plug"})
        self.assertEqual(os.stat(state / "app_msgids.json").st_mode & 0o777,
                         0o600)

    def test_history_db_is_owner_only(self):
        from mqtt_bot.io import history as history_mod
        h = history_mod.History(self.dir / "h.sqlite", retention_days=0)
        self.addCleanup(h.close)
        self.assertEqual(os.stat(self.dir / "h.sqlite").st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
