# tests/unit/test_io.py
from pathlib import Path

import pandas as pd
import pytest

SAMPLE_CSV = """time,open,high,low,close,Volume
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


def test_read_csv_basic(tmp_path: Path):
    f = tmp_path / "sample.csv"
    f.write_text(SAMPLE_CSV)
    from tvparser import io

    df = io.read_csv(str(f))
    # basic sanity: rows and columns
    assert len(df) == 10
    assert set(df.columns.str.lower()) >= {
      "time", "open", "high", "low", "close", "volume"}


def test_read_csv_missing_file_raises(tmp_path: Path):
    missing = tmp_path / "nope.csv"
    from tvparser import io

    with pytest.raises(FileNotFoundError):
        io.read_csv(str(missing))


def test_write_csv_roundtrip(tmp_path: Path):
    from tvparser import io

    # create a small dataframe and write it,
    # then read back with pandas directly
    df = pd.DataFrame(
        {
            "time": [1, 2],
            "open": [10.0, 11.0],
            "high": [12.0, 13.0],
            "low": [9.5, 10.5],
            "close": [11.0, 12.0],
            "volume": [100, 200],
        }
    )
    out = tmp_path / "out.csv"
    io.write_csv(df, str(out))

    df2 = pd.read_csv(str(out))
    assert list(df2["time"]) == [1, 2]
    assert list(df2["volume"]) == [100, 200]


def test_discover_input_files_dir_and_glob(tmp_path: Path):
    # Create files a.csv, b.csv and others
    (tmp_path / "a.csv").write_text("time,open\n")
    (tmp_path / "b.csv").write_text("time,open\n")
    (tmp_path / "ignore.txt").write_text("x\n")

    from tvparser import io

    files = io.discover_input_files(str(tmp_path))
    # should find csvs only, sorted
    assert isinstance(files, list)
    assert all(f.endswith(".csv") for f in files)
    assert files == sorted(files)

    # also test glob pattern (parent.glob) case
    pattern = str(tmp_path / "a.csv")
    files2 = io.discover_input_files(pattern)
    assert isinstance(files2, list)
    assert any("a.csv" in p for p in files2)
