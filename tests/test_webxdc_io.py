"""Tests for WebxdcIO — app_msgids registry + /apps delivery ordering.

The ordering test guards a real outage we hit in the field: a reboot
landed inside ``send_apps`` after the old app message was deleted but
before the new msgid was persisted, leaving app_msgids.json pointing at
a now-deleted message. Every snapshot push then failed forever
("Message Msg#N does not exist") until the next successful /apps.

Invariant: the new pointer must be durable on disk *before* the old
message is deleted, so the worst a crash can leave is a harmless
lingering duplicate — never a dangling pointer.
"""

import json
import tempfile
import unittest
from pathlib import Path

from mqtt_bot.io.webxdc_io import WebxdcIO


class _FakeRpc:
    def __init__(self, on_delete=None):
        self._next = 18000
        self.deleted = []
        self._on_delete = on_delete

    def send_msg(self, accid, chat_id, msg_data):
        self._next += 1
        return self._next

    def delete_messages_for_all(self, accid, msgids):
        if self._on_delete is not None:
            self._on_delete(list(msgids))
        self.deleted.extend(msgids)


class _FakeBot:
    def __init__(self, rpc):
        self.rpc = rpc


def _make_class(devices_dir: Path, cls: str) -> None:
    d = devices_dir / cls
    d.mkdir(parents=True)
    (d / f"{cls}.xdc").write_bytes(b"PK\x03\x04 fake xdc")


class TestSendAppsOrdering(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.state_dir = root / "state"
        self.state_dir.mkdir()
        self.devices_dir = root / "devices"
        self.devices_dir.mkdir()
        _make_class(self.devices_dir, "shelly_plug")
        self.app_msgids = self.state_dir / "app_msgids.json"

    def tearDown(self):
        self._tmp.cleanup()

    def _seed(self, mapping: dict) -> None:
        self.app_msgids.write_text(json.dumps(mapping))

    def test_new_msgid_persisted_before_old_deleted(self):
        # A prior install is already on disk for chat 12.
        self._seed({"12": {"shelly_plug": 15381}})

        # Capture what app_msgids.json holds at the instant the old
        # message is deleted — that is the crash window.
        on_disk_at_delete = {}

        def on_delete(msgids):
            on_disk_at_delete["map"] = json.loads(self.app_msgids.read_text())

        rpc = _FakeRpc(on_delete=on_delete)
        io = WebxdcIO(self.state_dir, self.devices_dir)
        sent, retracted = io.send_apps(
            _FakeBot(rpc), accid=1, chat_id=12,
            classes_visible={"shelly_plug"},
        )

        self.assertEqual(sent, ["shelly_plug"])
        new = io.map_snapshot()[12]["shelly_plug"]
        self.assertNotEqual(new, 15381)
        self.assertEqual(rpc.deleted, [15381])
        # At delete time the new pointer was ALREADY durable on disk.
        self.assertEqual(on_disk_at_delete["map"],
                         {"12": {"shelly_plug": new}})

    def test_retract_persisted_before_delete(self):
        # Tracked class whose xdc is no longer servable -> retract.
        self._seed({"12": {"gone_class": 5000}})

        on_disk_at_delete = {}

        def on_delete(msgids):
            on_disk_at_delete["map"] = json.loads(self.app_msgids.read_text())

        rpc = _FakeRpc(on_delete=on_delete)
        io = WebxdcIO(self.state_dir, self.devices_dir)
        # gone_class is not visible/available -> it should be retracted;
        # shelly_plug is visible -> it should be sent.
        sent, retracted = io.send_apps(
            _FakeBot(rpc), accid=1, chat_id=12,
            classes_visible={"shelly_plug"},
        )

        self.assertEqual(retracted, ["gone_class"])
        self.assertIn(5000, rpc.deleted)
        # The retracted entry was gone from disk before its delete.
        self.assertNotIn("gone_class",
                         on_disk_at_delete["map"].get("12", {}))

    def test_save_failure_does_not_lose_old_pointer(self):
        # If persistence itself fails, we must NOT delete the old
        # message — better a stale pointer to a live message than a
        # pointer to a deleted one.
        self._seed({"12": {"shelly_plug": 15381}})
        rpc = _FakeRpc()
        io = WebxdcIO(self.state_dir, self.devices_dir)

        def boom():
            raise OSError("disk full")

        io._save = boom  # type: ignore[method-assign]
        with self.assertRaises(OSError):
            io.send_apps(_FakeBot(rpc), accid=1, chat_id=12,
                         classes_visible={"shelly_plug"})
        # Old message survives because the save never committed.
        self.assertEqual(rpc.deleted, [])


if __name__ == "__main__":
    unittest.main()
