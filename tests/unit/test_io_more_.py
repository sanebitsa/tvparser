# tests/unit/test_io_more.py
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tvparser import io


def test_read_csv_missing_raises(tmp_path: Path) -> None:
    """read_csv should raise FileNotFoundError for missing path."""
    missing = tmp_path / "this_file_does_not_exist.csv"
    with pytest.raises(FileNotFoundError):
        io.read_csv(str(missing))


def test_read_csv_accepts_pathlib_and_str(tmp_path: Path) -> None:
    """read_csv should accept both str and pathlib.Path args."""
    p = tmp_path / "sample.csv"
    p.write_text("time,open\n1,10\n2,20\n")

    df_str = io.read_csv(str(p))
    assert len(df_str) == 2

    df_path = io.read_csv(p)
    assert len(df_path) == 2


def test_write_parquet_creates_parent_dir_monkeypatched(
    tmp_path: Path, monkeypatch
) -> None:
    """
    write_parquet should create missing parent directories and write file.

    Monkeypatch pandas.DataFrame.to_parquet to avoid requiring a parquet
    engine (pyarrow/fastparquet) in test environments.
    """
    # prepare a small DataFrame
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
    out = tmp_path / "nested" / "sub" / "out.parquet"
    assert not out.parent.exists()

    # monkeypatch to avoid real parquet engines; write a small marker file
    def fake_to_parquet(self, path, *args, **kwargs):
        p = Path(path)
        if not p.parent.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
        # write a tiny placeholder
        p.write_bytes(b"PARQUET_PLACEHOLDER")
        return None

    # apply monkeypatch on DataFrame.to_parquet
    monkeypatch.setattr(
        pd.DataFrame, "to_parquet", fake_to_parquet, raising=False
    )

    # call under test
    io.write_parquet(df, str(out))

    assert out.exists()
    assert out.stat().st_size > 0
