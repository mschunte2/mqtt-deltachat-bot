"""Row shaping for `/<device> export <window>`.

Pure: takes DB tuples, returns lists of CSV cells. Lives outside bot.py
so the column layout is testable — the layout is exactly what went
wrong before, and nothing caught it.

Both row kinds share one wide table with a `kind` discriminator:
per-minute aggregates fill `avg_apower_w`/`sample_count`, raw status
updates fill the instantaneous columns. `output` is the relay state and
is common to both, so it has a single column rather than one per kind.
"""

from __future__ import annotations

import datetime as _dt

HEADER = [
    "unix_ts", "iso_time", "device", "kind",
    "avg_apower_w", "output", "sample_count",
    "apower_w", "voltage_v", "current_a", "freq_hz",
    "aenergy_total_wh", "temperature_c",
]


def _iso(ts: int) -> str:
    return _dt.datetime.fromtimestamp(int(ts)).isoformat()


def _num(value, fmt: str) -> str:
    return "" if value is None else format(value, fmt)


def power_minute_row(device: str, row) -> list:
    """(ts, avg_apower_w, output, sample_count) -> one CSV row."""
    ts, apower, output, count = row
    return [
        ts, _iso(ts), device, "power_minute",
        f"{apower:.3f}",
        "" if output is None else output,
        count,
        "", "", "", "", "", "",
    ]


def samples_raw_row(device: str, row) -> list:
    """(ts, apower, voltage, current, freq, aenergy, output, temp) -> row.

    `output` goes in the shared column 6, not appended at the end. It
    used to be written after `aenergy_total_wh`, which pushed every
    value one place right of its header: relay state was labelled
    `temperature_c` and the temperature landed in an unnamed 14th
    column. Every export ever produced was mislabelled that way, and a
    spreadsheet import read relay state as a temperature.
    """
    ts, ap, v, c, f_hz, ae, out, tc = row
    return [
        ts, _iso(ts), device, "samples_raw",
        "",
        "" if out is None else out,
        "",
        _num(ap, ".3f"), _num(v, ".2f"), _num(c, ".4f"), _num(f_hz, ".2f"),
        _num(ae, ".3f"), _num(tc, ".1f"),
    ]
