from __future__ import annotations

import re
from numbers import Number
from typing import TYPE_CHECKING, Any, Dict, List

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path


def _to_camel(s: str) -> str:
    s = re.sub(r"[^\w\s]", "", s).strip()
    if not s:
        return s
    parts = re.split(r"[_\s]+", s)
    head = parts[0].lower()
    tail = "".join(p.title() for p in parts[1:])
    return head + tail


def _coerce_time_series(s: pd.Series) -> pd.Series:
    """
    Coerce a Series of timestamps to integer seconds.

    Heuristic: if median > 1e12 treat as ms and divide by 1000.
    """
    num = pd.to_numeric(s, errors="coerce")
    if not num.isna().all():
        median_val = float(num.median(skipna=True))
        if median_val > 1e12:
            num = (num // 1000).astype("Int64")
        else:
            num = num.astype("Int64")
        if not num.isna().all():
            return num.astype("int64")
    # fall back: return original numeric-converted series
    return num


def canonical_records_from_csv(
    csv_path: str | Path,
    *,
    camel_case: bool = True,
    time_col: str = "time",
    float_round: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Read CSV, apply canonicalization similar to csv2json, and return
    a list of plain Python records suitable for comparison.

    - camel_case: convert column names to camelCase
    - time_col: name of the timestamp column to coerce to seconds
    - float_round: optional number of decimals to round floats to
    """
    df = pd.read_csv(csv_path)

    if camel_case:
        df = df.rename(columns={c: _to_camel(c) for c in df.columns})

    if time_col in df.columns:
        df[time_col] = _coerce_time_series(df[time_col])

    records: List[Dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        out: Dict[str, Any] = {}
        for k, v in row.items():
            if pd.isna(v):
                out[k] = None
                continue

            # Numbers (numpy ints/floats included) are instances of Number
            if isinstance(v, Number) and not isinstance(v, bool):
                if isinstance(v, float):
                    if float_round is not None:
                        out[k] = round(float(v), float_round)
                    else:
                        out[k] = float(v)
                else:
                    out[k] = int(v)
                continue

            # Fallback: keep as-is (strings, etc.)
            out[k] = v
        records.append(out)

    return records
