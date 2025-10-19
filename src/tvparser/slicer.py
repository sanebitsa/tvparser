from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


def slice_csv_window(
    csv_path: Union[str, Path],
    start_ts: int,
    end_ts: int,
    out_path: Union[str, Path],
    ts_column: str = "ts",
) -> int:
    """
    Read `csv_path`, pick rows where ts_column is between start_ts and
    end_ts (inclusive), write them to out_path as CSV, and return count.
    """
    inp = Path(csv_path)
    outp = Path(out_path)

    if not inp.exists():
        raise FileNotFoundError(f"input CSV not found: {inp}")

    df = pd.read_csv(inp)
    if ts_column not in df.columns:
        raise ValueError(f"timestamp column '{ts_column}' not found")

    # Ensure timestamps are numeric integers (coerce then drop na)
    df[ts_column] = pd.to_numeric(df[ts_column], errors="coerce")
    df = df.dropna(subset=[ts_column])
    df[ts_column] = df[ts_column].astype("Int64").astype(int)

    sel = df[(df[ts_column] >= int(start_ts))
             & (df[ts_column] <= int(end_ts))]

    # Ensure parent dir exists
    outp.parent.mkdir(parents=True, exist_ok=True)

    # Write selection using same columns/order as original
    sel.to_csv(outp, index=False)
    return int(len(sel))
