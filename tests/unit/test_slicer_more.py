from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tvparser import slicer

if TYPE_CHECKING:
    from pathlib import Path

SAMPLE = """ts,o
1000,1
1060,2
1120,3
"""


def _read_lines(path: Path) -> int:
    txt = path.read_text(encoding="utf-8")
    return len(txt.splitlines())


def test_slice_writes_header_only_for_empty_sel(tmp_path: Path) -> None:
    """If no rows match window, output should contain header only."""
    inp = tmp_path / "in.csv"
    outp = tmp_path / "out.csv"
    inp.write_text(SAMPLE)

    # choose a window outside sample range -> no rows match
    n = slicer.slice_csv_window(inp, 2000, 3000, outp, ts_column="ts")
    assert n == 0
    # header only -> one line
    assert _read_lines(outp) == 1


def test_slice_raises_on_missing_ts_column(tmp_path: Path) -> None:
    """If ts column missing, ValueError is raised."""
    p = tmp_path / "bad.csv"
    p.write_text("time,open\n1,2\n")
    with pytest.raises(ValueError):
        slicer.slice_csv_window(p, 0, 10, tmp_path / "out.csv", ts_column="ts")
