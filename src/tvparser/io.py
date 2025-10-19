# src/tvparser/io.py
from __future__ import annotations

from pathlib import Path
from typing import List, Union

import pandas as pd


def read_csv(path: Union[str, Path]) -> pd.DataFrame:
    """
    Read CSV (or compressed CSV via pandas) and return DataFrame.

    Accepts either a str or Path. Raises FileNotFoundError if the file
    does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    # Convert to str to satisfy pandas type stubs and to be explicit.
    return pd.read_csv(str(p))


def write_csv(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Write DataFrame to CSV. Creates parent directories as needed.
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(p), index=False)


def write_parquet(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Write DataFrame to Parquet. Uses pandas' engine (pyarrow / fastparquet).
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(p), index=False)


def discover_input_files(path_or_dir: Union[str, Path]) -> List[str]:
    """
    Discover CSV files for a given directory or path/glob.

    - If path_or_dir is a directory: returns sorted list of '*.csv' inside it.
    - Otherwise: treat as path or glob and return sorted matches.
    """
    p = Path(path_or_dir)
    if p.is_dir():
        files = sorted([str(x) for x in p.glob("*.csv")])
    else:
        # treat as a path or glob (use parent.glob for patterns)
        files = sorted([str(x) for x in p.parent.glob(p.name)])
    return files
