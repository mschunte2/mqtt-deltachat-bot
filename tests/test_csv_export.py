"""Column layout for the CSV export.

The header had 13 columns and both data rows wrote 14. For samples_raw
rows every value sat one place right of its header from `output`
onward: relay state was labelled `temperature_c`, and the temperature
fell into an unnamed 14th column. A spreadsheet import read relay state
as a temperature reading, and every export the user had ever run was
affected.

The width assertions below are the guard: they fail on any future edit
that adds a value to one row kind and forgets the header.
"""

import unittest

from mqtt_bot import csv_export


_POWER_ROW = (1714000000, 42.5, 1, 4)
_SAMPLE_ROW = (1714000060, 41.25, 233.4, 0.1832, 50.01, 12345.678, 1, 38.5)


class TestColumnAlignment(unittest.TestCase):
    def test_power_minute_row_matches_header_width(self):
        row = csv_export.power_minute_row("kaffeete", _POWER_ROW)
        self.assertEqual(len(row), len(csv_export.HEADER))

    def test_samples_raw_row_matches_header_width(self):
        row = csv_export.samples_raw_row("kaffeete", _SAMPLE_ROW)
        self.assertEqual(len(row), len(csv_export.HEADER))

    def test_every_value_lands_under_its_own_header(self):
        cells = dict(zip(csv_export.HEADER,
                         csv_export.samples_raw_row("kaffeete", _SAMPLE_ROW)))
        self.assertEqual(cells["output"], 1)
        self.assertEqual(cells["temperature_c"], "38.5")
        self.assertEqual(cells["apower_w"], "41.250")
        self.assertEqual(cells["aenergy_total_wh"], "12345.678")
        self.assertEqual(cells["voltage_v"], "233.40")

    def test_output_shares_one_column_across_row_kinds(self):
        """Relay state means the same thing in both kinds, so it belongs
        in one column — otherwise a reader has to know which kind a row
        is before it can find the value."""
        p = dict(zip(csv_export.HEADER,
                     csv_export.power_minute_row("k", _POWER_ROW)))
        s = dict(zip(csv_export.HEADER,
                     csv_export.samples_raw_row("k", _SAMPLE_ROW)))
        self.assertEqual(p["output"], 1)
        self.assertEqual(s["output"], 1)

    def test_header_has_no_duplicate_names(self):
        self.assertEqual(len(csv_export.HEADER), len(set(csv_export.HEADER)))


class TestNullHandling(unittest.TestCase):
    def test_missing_sample_values_render_empty(self):
        row = csv_export.samples_raw_row(
            "k", (1714000060, None, None, None, None, None, None, None))
        cells = dict(zip(csv_export.HEADER, row))
        for col in ("apower_w", "voltage_v", "current_a", "freq_hz",
                    "aenergy_total_wh", "temperature_c", "output"):
            self.assertEqual(cells[col], "", col)

    def test_missing_output_on_power_row_renders_empty(self):
        row = csv_export.power_minute_row("k", (1714000000, 0.0, None, 1))
        self.assertEqual(dict(zip(csv_export.HEADER, row))["output"], "")

    def test_output_zero_is_not_confused_with_missing(self):
        """0 is 'relay off', which must not render like 'no data'."""
        row = csv_export.samples_raw_row(
            "k", (1714000060, 0.0, 233.0, 0.0, 50.0, 1.0, 0, 20.0))
        self.assertEqual(dict(zip(csv_export.HEADER, row))["output"], 0)


if __name__ == "__main__":
    unittest.main()
