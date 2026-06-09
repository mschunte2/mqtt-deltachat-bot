"""Tests for the Publisher (single outbound stream)."""

import unittest

from mqtt_bot.io import publisher as publisher_mod


class TestPublisher(unittest.TestCase):
    def test_broadcast_iterates_msgid_map(self):
        sent = []
        builds = []

        def fake_build(chat, cls):
            builds.append((chat, cls))
            return {"class": cls, "devices": {}, "server_ts": 0}

        msgids = {12: {"tplug": 1001}, 14: {"tplug": 2002}}
        pub = publisher_mod.Publisher(
            build=fake_build,
            msgids=lambda: msgids,
            send=lambda c, m, p: (sent.append((c, m, p)), True)[1],
            interval_s=300,
        )
        pub.broadcast()
        self.assertEqual(builds, [(12, "tplug"), (14, "tplug")])
        self.assertEqual([(c, m) for c, m, _ in sent], [(12, 1001), (14, 2002)])

    def test_periodic_loop_refreshes_unchanged_snapshot(self):
        """The periodic tick must re-push even when the snapshot body is
        unchanged, so the app's 'data N ago' indicator (derived from
        server_ts) stays fresh on a quiet/steady-state device. Edge
        broadcasts dedup by content hash; the heartbeat must not."""
        sends = []

        def fake_build(chat, cls):
            return {"class": cls, "devices": {"k": {"x": 1}}, "server_ts": 0}

        pub = publisher_mod.Publisher(
            build=fake_build,
            msgids=lambda: {12: {"tplug": 1001}},
            send=lambda c, m, p: (sends.append((c, m)), True)[1],
            interval_s=300,
        )
        # Prime the dedup hash as a prior identical push would.
        pub.broadcast(force=True)
        self.assertEqual(len(sends), 1)
        # A non-forced broadcast dedups identical content (skipped).
        pub.broadcast()
        self.assertEqual(len(sends), 1)

        # Drive exactly one periodic loop iteration.
        class _OneShotStop:
            def __init__(self):
                self._n = 0

            def wait(self, _interval):
                self._n += 1
                return self._n > 1  # run body once, then stop

            def set(self):
                pass

        pub._stop = _OneShotStop()
        pub._loop()
        self.assertEqual(len(sends), 2)  # heartbeat re-pushed identical body

    def test_push_unicast_skips_when_build_returns_none(self):
        sent = []
        pub = publisher_mod.Publisher(
            build=lambda c, cl: None,
            msgids=lambda: {},
            send=lambda c, m, p: (sent.append((c, m, p)), True)[1],
            interval_s=300,
        )
        ok = pub.push_unicast(12, 1001, "tplug")
        self.assertFalse(ok)
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
