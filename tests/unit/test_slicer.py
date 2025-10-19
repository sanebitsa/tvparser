from __future__ import annotations

from typing import TYPE_CHECKING

from tvparser import slicer

if TYPE_CHECKING:
    from pathlib import Path

SAMPLE_CSV = """ts,o,h,l,c,v
1728252000,2671.7,2672.0,2667.9,2670.3,435
1728252060,2670.2,2670.4,2669.2,2669.3,56
1728252120,2669.4,2670.9,2669.4,2670.9,95
1728252180,2670.9,2672.6,2670.7,2671.8,103
1728252240,2671.6,2671.6,2670.7,2670.7,62
1728252300,2670.8,2671.0,2669.7,2669.9,76
1728252360,2669.9,2670.2,2669.6,2669.6,36
1728252420,2669.4,2670.0,2669.4,2669.6,21
1728252480,2669.6,2670.5,2669.6,2670.3,29
"""


def _write_sample(path: Path) -> None:
    path.write_text(SAMPLE_CSV)


def test_slice_csv_window_basic(tmp_path: Path) -> None:
    """
    Select rows from ts=1728252000 through ts=1728252240 (inclusive).
    Expect five rows: 2000,2060,2120,2180,2240.
    """
    inp = tmp_path / "gc_1min.csv"
    out = tmp_path / "out.csv"
    _write_sample(inp)

    start = 1728252000
    end = 1728252240

    n = slicer.slice_csv_window(inp, start, end, out, ts_column="ts")
    assert n == 5

    txt = out.read_text()
    # header + 5 data lines -> 6 lines
    assert len(txt.splitlines()) == 6

    # basic content checks: first and last timestamp present
    lines = out.read_text().splitlines()
    assert lines[1].startswith("1728252000,")
    assert lines[-1].startswith("1728252240,")


def test_slice_csv_window_no_rows(tmp_path: Path) -> None:
    """Window outside range should produce zero rows and an output file."""
    inp = tmp_path / "gc_1min.csv"
    out = tmp_path / "out_empty.csv"
    _write_sample(inp)

    # window before sample
    n = slicer.slice_csv_window(inp, 1600000000, 1600001000, out)
    assert n == 0
    # CSV with header only
    lines = out.read_text().splitlines()
    assert len(lines) == 1
