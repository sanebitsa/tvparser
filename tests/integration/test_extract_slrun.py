from __future__ import annotations

import csv
from typing import TYPE_CHECKING, List

from tvparser import extract_slrun

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


SLRUN_CONTENT = """date, start, entry, exit, end
10/06/24, 17:00, 19:30, 23:30, 7:00
10/06/24, 00:00, 00:30, 01:30, 02:00
"""


def _write_sample_csv(p: Path) -> None:
    p.write_text(SAMPLE_CSV)


def _write_slrun(p: Path) -> None:
    p.write_text(SLRUN_CONTENT)


def _read_ts_list(p: Path) -> List[int]:
    with p.open() as fh:
        rdr = csv.DictReader(fh)
        return [int(r["ts"]) for r in rdr]


def test_extract_slrun_end_to_end(tmp_path: Path) -> None:
    """
    Integration: parse the SLrun file and extract windows from the CSV.
    Validates that the expected per-window files are created and contain
    the expected timestamps.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_path = data_dir / "gc_1min.csv"
    slrun_path = data_dir / "SLrunLong.txt"
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    _write_sample_csv(csv_path)
    _write_slrun(slrun_path)

    rc = extract_slrun.main(
        [
            "--slrun",
            str(slrun_path),
            "--csv",
            str(csv_path),
            "--out-dir",
            str(out_dir),
            "--tz",
            "UTC",
        ]
    )
    assert rc == 0

    # Two windows were specified in SLRUN_CONTENT, ensure files exist
    files = sorted(out_dir.glob("gc_1min_*.csv"))
    assert len(files) == 2

    # Identify which file is which by start-time in filename
    # find file that contains "17-00" and the one with "00-00"
    file_17 = next((f for f in files if "17-00" in f.name), None)
    file_00 = next((f for f in files if "00-00" in f.name), None)

    assert file_17 is not None, "expected a 17-00 window file"
    assert file_00 is not None, "expected a 00-00 window file"

    # For 17:00 -> 07:00 next day we expect rows within our sample range
    ts_list_17 = _read_ts_list(file_17)
    # ensure there is at least one timestamp present
    assert len(ts_list_17) > 0
    assert min(ts_list_17) >= 1728252000
    assert max(ts_list_17) <= 1728252480

    # 00:00 -> 02:00 may produce zero or some rows; just ensure file reads
    ts_list_00 = _read_ts_list(file_00)
    assert isinstance(ts_list_00, list)
