from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
from pandas.testing import assert_series_equal

from tvparser import cli, core

BASE_CSV = """time,open,high,low,close,Volume
1736722800,2716.8,2717.9,2715.1,2716.1,292
1736722860,2716.1,2716.4,2714,2715.1,165
1736722920,2715.1,2715.8,2714.5,2714.5,87
1736722980,2714.8,2716.6,2714.2,2716.4,69
1736723040,2716.3,2717.5,2715.3,2717,109
1736723100,2716.9,2717.8,2716.6,2717,66
1736723160,2717,2717.3,2715.7,2716,61
1736723220,2716.2,2716.7,2716.1,2716.1,45
1736723280,2716.3,2717.2,2716.2,2717,28
1736723340,2716.9,2717.5,2716.9,2717.3,27
"""


def _make_test_input(path: Path) -> None:
    """
    Write a test CSV file with:
      - the canonical 10 good rows (BASE_CSV)
      - a couple of invalid rows (missing time, non-numeric time)
      - an exact duplicate row (should be removed by dedupe)
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # duplicate an existing good row (exact duplicate)
    duplicate_row = "1736722920,2715.1,2715.8,2714.5,2714.5,87\n"

    # invalid rows: missing time and non-numeric time
    missing_time = ",2716.8,2717.9,2715.1,2716.1,292\n"
    non_numeric_time = "not_a_time,1,2,3,4,5\n"

    content = BASE_CSV + duplicate_row + missing_time + non_numeric_time
    path.write_text(content)


def _expected_times() -> List[int]:
    """Return the expected (clean) time values in ascending order."""
    return [
        1736722800,
        1736722860,
        1736722920,
        1736722980,
        1736723040,
        1736723100,
        1736723160,
        1736723220,
        1736723280,
        1736723340,
    ]


def _expected_close_series() -> pd.Series:
    """Return expected 'close' values aligned with expected times."""
    vals = [2716.1, 2715.1, 2714.5, 2716.4, 2717.0,
            2717.0, 2716.0, 2716.1, 2717.0, 2717.3]
    return pd.Series(vals, name="close")


def test_core_merge_on_test_file(tmp_path: Path) -> None:
    """
    Integration: write a test CSV (with invalid/duplicate rows),
    run core.merge_frames on the file path and assert the cleaned result
    matches the canonical rows.
    """
    data_dir = Path("tests") / "data"
    in_path = data_dir / "gc_011224_020524_1m.csv"

    # create the test input file (overwrites if exists)
    _make_test_input(in_path)

    # run the merger (reads the file via io.read_csv)
    merged = core.merge_frames(
        [str(in_path)],
        dedupe_strategy="last",
        drop_incomplete=True,
        sort_order="asc",
    )

    # basic checks
    assert list(merged["time"]) == _expected_times()
    assert merged["time"].is_monotonic_increasing

    # compare 'close' values with tolerance
    expected_close = _expected_close_series()
    # select merged close as plain floats for stable comparison
    merged_close = merged["close"].astype(float).reset_index(drop=True)
    assert_series_equal(merged_close, expected_close, check_dtype=False)


def test_cli_end_to_end_reads_and_writes(tmp_path: Path) -> None:
    """
    End-to-end: run cli.main with --input pointing at the test CSV and
    --output to tmp_path. Read the output CSV and assert it matches the
    expected cleaned rows.
    """
    data_dir = Path("tests") / "data"
    in_path = data_dir / "gc_011224_020524_1m.csv"
    _make_test_input(in_path)

    out_file = tmp_path / "out.csv"
    argv = ["--input", str(in_path), "--output", str(out_file)]

    rc = cli.main(argv)
    assert rc == 0
    assert out_file.exists()

    out_df = pd.read_csv(out_file)
    # cli writes with lowercase or original column names;
    # normalize columns for robust comparison
    out_df = out_df.rename(columns={c: c.lower() for c in out_df.columns})

    assert list(out_df["time"]) == _expected_times()
    out_close = out_df["close"].astype(float).reset_index(drop=True)
    expected_close = _expected_close_series()
    assert_series_equal(out_close, expected_close, check_dtype=False)
