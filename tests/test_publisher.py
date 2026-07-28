"""Tests for the Publisher (single outbound stream)."""

import threading
import time
import unittest

from mqtt_bot.io import publisher as publisher_mod


class TestPublisher(unittest.TestCase):
    def test_broadcast_iterates_msgid_map(self):
        sent = []
        builds = []

        def fake_build(chat, cls, include_history=True):
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
        """The heartbeat must re-push even when the snapshot body is
        unchanged, so the app's 'data N ago' indicator (derived from
        server_ts) stays fresh on a quiet device. Edge broadcasts dedup
        by content hash; the heartbeat must not."""
        sends = []

        def fake_build(chat, cls, include_history=True):
            return {"class": cls, "devices": {"k": {"x": 1}}, "server_ts": 0}

        pub = publisher_mod.Publisher(
            build=fake_build,
            msgids=lambda: {12: {"tplug": 1001}},
            send=lambda c, m, p: (sends.append((c, m)), True)[1],
            interval_s=300,
        )
        pub.broadcast(force=True)
        self.assertEqual(len(sends), 1)
        pub.broadcast()                      # identical content -> deduped
        self.assertEqual(len(sends), 1)
        pub.broadcast(force=True)            # heartbeat semantics
        self.assertEqual(len(sends), 2)

    def test_interval_clamp_bounds(self):
        """The periodic interval is clamped to [60s, 24h]. The 24h ceiling
        lets an idle-heartbeat cadence of hours through (3h = 10800s);
        the 60s floor keeps a misconfigured tiny value sane."""
        def mk(interval_s):
            return publisher_mod.Publisher(
                build=lambda c, cl, include_history=True: None,
                msgids=lambda: {},
                send=lambda c, m, p: True,
                interval_s=interval_s,
            )
        self.assertEqual(mk(10800)._interval, 10800)   # 3h honored (was clamped to 900)
        self.assertEqual(mk(999999)._interval, 86400)  # ceiling 24h
        self.assertEqual(mk(1)._interval, 60)          # floor 60s

    def test_push_unicast_skips_when_build_returns_none(self):
        sent = []
        pub = publisher_mod.Publisher(
            build=lambda c, cl, include_history=True: None,
            msgids=lambda: {},
            send=lambda c, m, p: (sent.append((c, m, p)), True)[1],
            interval_s=300,
        )
        ok = pub.push_unicast(12, 1001, "tplug")
        self.assertFalse(ok)
        self.assertEqual(sent, [])


class TestEdgeCoalescing(unittest.TestCase):
    """Twins used to build and push inline on the MQTT callback thread,
    once per state edge, with the full ~139 KB payload. A cycling
    appliance produced several pushes a minute and each left a
    permanently-retained carrier in dc.db — the growth that took it to
    464 MB on the live host."""

    def _publisher(self, interval_s=3600, coalesce_s=0.05):
        self.sends = []
        self.builds = []

        def build(chat, cls, include_history=True):
            self.builds.append((chat, cls, include_history))
            return {"class": cls, "kind": "full" if include_history else "state",
                    "devices": {"k": {"n": len(self.builds)}}, "server_ts": 0}

        pub = publisher_mod.Publisher(
            build=build,
            msgids=lambda: {12: {"tplug": 1001}},
            send=lambda c, m, p: (self.sends.append(p), True)[1],
            interval_s=interval_s,
        )
        pub.COALESCE_S = coalesce_s
        self.addCleanup(self._shutdown, pub)
        return pub

    def _shutdown(self, pub):
        pub.stop()
        if pub._thread is not None:
            pub._thread.join(timeout=5)

    def _wait_for_sends(self, n, timeout=5):
        deadline = time.monotonic() + timeout
        while len(self.sends) < n and time.monotonic() < deadline:
            time.sleep(0.01)

    def test_a_burst_of_edges_becomes_one_push(self):
        pub = self._publisher()
        pub.start()
        for _ in range(10):
            pub.request_broadcast("kaffeete", only_class="tplug")
        self._wait_for_sends(1)
        time.sleep(0.3)          # let any stragglers arrive
        self.assertEqual(len(self.sends), 1,
                         f"10 edges produced {len(self.sends)} pushes")

    def test_edge_push_omits_the_history_series(self):
        pub = self._publisher()
        pub.start()
        pub.request_broadcast("kaffeete", only_class="tplug")
        self._wait_for_sends(1)
        self.assertEqual(self.sends[0]["kind"], "state")
        self.assertEqual(self.builds[0][2], False,
                         "edge broadcast asked for the full payload")

    def test_refresh_still_sends_the_full_payload(self):
        pub = self._publisher()
        pub.push_unicast(12, 1001, "tplug")
        self.assertEqual(self.sends[0]["kind"], "full")
        self.assertEqual(self.builds[0][2], True)

    def test_request_broadcast_does_not_build_on_the_calling_thread(self):
        """The whole point: no SQL and no JSON on paho's network thread."""
        pub = self._publisher(coalesce_s=5.0)
        pub.start()
        pub.request_broadcast("kaffeete", only_class="tplug")
        self.assertEqual(self.builds, [],
                         "payload was built synchronously by the caller")

    def test_separate_classes_each_get_their_own_push(self):
        pub = self._publisher()
        pub._msgids = lambda: {12: {"tplug": 1001, "other": 1002}}
        pub.start()
        pub.request_broadcast("kaffeete", only_class="tplug")
        pub.request_broadcast("lamp", only_class="other")
        self._wait_for_sends(2)
        self.assertEqual({p["class"] for p in self.sends}, {"tplug", "other"})

    def test_stop_ends_the_thread(self):
        pub = self._publisher()
        pub.start()
        self.assertTrue(pub.is_alive())
        pub.stop()
        pub._thread.join(timeout=5)
        self.assertFalse(pub.is_alive())

    def test_heartbeat_sends_the_full_payload(self):
        pub = self._publisher(interval_s=60)
        pub._interval = 0.05           # bypass the 60s floor for the test
        pub.start()
        self._wait_for_sends(1)
        self.assertEqual(self.sends[0]["kind"], "full")


if __name__ == "__main__":
    unittest.main()
