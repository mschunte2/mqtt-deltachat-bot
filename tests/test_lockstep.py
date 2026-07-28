"""Cross-component consistency guards.

Cheap assertions that two things which must agree still do. Each of
these guards a class of bug that has actually bitten this project or its
sibling: a constant defined twice and drifting, a vocabulary the app
emits that the bot silently drops, a row shape that outgrew its header.

bot.py is import-hostile (module-level construction touches the
filesystem and builds live objects), so where a guard needs something
bot.py owns, that thing has been moved into a pure module — which is
the point: anything worth asserting is worth making testable.
"""

import json
import re
import unittest
from pathlib import Path

from mqtt_bot import commands, csv_export
from mqtt_bot.core import rules as sched
from mqtt_bot.util import durations

_ROOT = Path(__file__).resolve().parent.parent
_APP_JS = _ROOT / "devices" / "shelly_plug" / "app" / "main.js"


class TestAppAndBotAgreeOnActions(unittest.TestCase):
    """An action the app emits but the bot doesn't know is silently
    dropped: the button appears to do nothing and, until this sweep,
    logged nothing at the default level either."""

    def _actions_emitted_by_app(self) -> set[str]:
        src = _APP_JS.read_text()
        # send({ action: 'on' }) and sendUpdate({... action: 'refresh' ...})
        return set(re.findall(r"action:\s*'([a-z-]+)'", src))

    def test_app_emits_only_actions_the_bot_accepts(self):
        emitted = self._actions_emitted_by_app()
        self.assertTrue(emitted, "found no actions in main.js — regex stale?")
        unknown = emitted - commands.KNOWN_APP_ACTIONS
        self.assertEqual(unknown, set(),
                         f"app emits action(s) the bot will drop: {unknown}")

    def test_cancel_actions_are_known(self):
        for action in ("cancel-auto-off", "cancel-auto-on",
                       "cancel-schedule"):
            self.assertIn(action, commands.KNOWN_APP_ACTIONS)

    def test_direct_verbs_are_all_app_actions_too(self):
        """The chat and app surfaces should offer the same switching
        verbs; a gap means one surface can do something the other
        can't, silently."""
        self.assertTrue(commands.DIRECT_VERBS <= commands.KNOWN_APP_ACTIONS)


class TestCsvHeaderMatchesRows(unittest.TestCase):
    """The header had 13 columns while both row kinds wrote 14, so
    relay state was labelled temperature_c in every export ever run."""

    def test_power_minute_row_width(self):
        row = csv_export.power_minute_row("d", (1, 2.0, 1, 3))
        self.assertEqual(len(row), len(csv_export.HEADER))

    def test_samples_raw_row_width(self):
        row = csv_export.samples_raw_row(
            "d", (1, 2.0, 3.0, 4.0, 5.0, 6.0, 1, 7.0))
        self.assertEqual(len(row), len(csv_export.HEADER))


class TestBoundsAgreeAcrossSurfaces(unittest.TestCase):
    """The chat path and the app path must not disagree about what is
    an acceptable duration — a policy is a policy regardless of which
    surface created it."""

    def test_app_and_chat_duration_ceilings_match(self):
        from mqtt_bot import app_policy
        self.assertEqual(app_policy.MAX_MINUTES * 60, durations.MAX_SECONDS)

    def test_rule_window_cap_is_within_the_duration_cap(self):
        self.assertLessEqual(sched.MAX_RULE_WINDOW_S, durations.MAX_SECONDS)

    def test_export_window_is_within_the_duration_cap(self):
        self.assertLessEqual(commands.EXPORT_MAX_WINDOW_S,
                             durations.MAX_SECONDS)

    def test_sweeper_wait_is_under_threading_timeout_max(self):
        """The bug this whole guard exists for: a wait above
        threading.TIMEOUT_MAX raises OverflowError and kills the only
        thread that fires timed rules."""
        import threading
        self.assertLess(sched.MAX_SWEEP_WAIT_S, threading.TIMEOUT_MAX)

    def test_app_window_is_tighter_than_the_text_window(self):
        self.assertLess(commands.MAX_APP_AGE_SECONDS,
                        commands.MAX_AGE_SECONDS)


class TestStalenessThresholdIsUsable(unittest.TestCase):
    def test_staleness_marker_outlasts_the_default_poll_interval(self):
        """Shelly's default status_update_interval is 60s and we
        recommend 15s. A staleness threshold at or below that would
        mark every healthy device stale."""
        from mqtt_bot import formatters
        self.assertGreater(formatters.STALE_AFTER_S, 60)


class TestDeviceClassContract(unittest.TestCase):
    """Every shipped class.json must name itself after its directory —
    devices.json references classes by name, and a mismatch is a
    startup failure with a confusing message."""

    def test_class_name_matches_directory(self):
        for class_json in sorted((_ROOT / "devices").glob("*/class.json")):
            with self.subTest(path=str(class_json.relative_to(_ROOT))):
                doc = json.loads(class_json.read_text())
                self.assertEqual(doc.get("name"), class_json.parent.name)


if __name__ == "__main__":
    unittest.main()
