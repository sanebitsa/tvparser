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
    Read `csv_path`, select rows with ts in [start_ts, end_ts], write to
    `out_path` and return number of rows written.

    Always writes the header (even if zero rows).
    """
    inp = Path(csv_path)
    outp = Path(out_path)

    if not inp.exists():
        raise FileNotFoundError(f"input CSV not found: {inp}")

    df = pd.read_csv(inp)
    if ts_column not in df.columns:
        raise ValueError(f"timestamp column '{ts_column}' not found")

    # coerce numeric and drop NA timestamps
    df[ts_column] = pd.to_numeric(df[ts_column], errors="coerce")
    df = df.dropna(subset=[ts_column])
    df[ts_column] = df[ts_column].astype("Int64").astype(int)

    sel = df[(df[ts_column] >= int(start_ts))
             & (df[ts_column] <= int(end_ts))]

    outp.parent.mkdir(parents=True, exist_ok=True)

    # ensure header is written even if sel is empty
    if sel.empty:
        # write header only
        df.iloc[0:0].to_csv(outp, index=False)
        return 0

    sel.to_csv(outp, index=False)
    return int(len(sel))
