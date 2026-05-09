"""Tests for the Publisher (single outbound stream)."""

import unittest

import publisher as publisher_mod


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
