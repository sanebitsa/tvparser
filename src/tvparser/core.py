from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

LOGGER = logging.getLogger("tvparser.core")

REQUIRED_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

COLUMN_ALIASES: Dict[str, str] = {
    "timestamp": "time",
    "datetime": "time",
    "date": "time",
    "t": "time",
    "o": "open",
    "open": "open",
    "h": "high",
    "high": "high",
    "l": "low",
    "low": "low",
    "c": "close",
    "close": "close",
    "v": "volume",
    "vol": "volume",
    "volume": "volume",
}

# default indicators we'll auto-detect if indicators is None
_INDICATOR_DEFAULTS = ["ema", "vwap", "atr"]


class TVParserError(Exception):
    """Base exception for tvparser core errors."""


class MissingColumnsError(TVParserError):
    """Raised when required columns are missing."""


def _canonicalize_columns(columns: Iterable[str]) -> Dict[str, str]:
    """Map existing column names to canonical names (case-insensitive)."""
    mapping: Dict[str, str] = {}
    for col in columns:
        low = col.strip().lower()
        if low in COLUMN_ALIASES:
            mapping[col] = COLUMN_ALIASES[low]
        else:
            mapping[col] = low
    return mapping


def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse duplicate-named columns into a single column.

    For columns that share the same name (after rename), coalesce values
    left-to-right using bfill(axis=1) and keep the first column.
    """
    cols = list(df.columns)
    groups: Dict[str, List[int]] = {}

    # single pass: build groups of indices per column name
    for i, c in enumerate(cols):
        groups.setdefault(c, []).append(i)

    new_cols: Dict[str, pd.Series] = {}
    for name, indices in groups.items():
        if len(indices) == 1:
            new_cols[name] = df.iloc[:, indices[0]]
        else:
            sub = df.iloc[:, indices]
            coalesced = sub.bfill(axis=1).iloc[:, 0]
            new_cols[name] = coalesced

    return pd.DataFrame(new_cols, index=df.index)


def _detect_and_normalize_time_series(
    series: pd.Series,
) -> Tuple[pd.Series, bool]:
    """Coerce a time-like series to numeric, convert ms->s if detected."""
    numeric = pd.to_numeric(series, errors="coerce")

    if numeric.isna().all():
        return numeric, False

    # Use median as a robust central tendency to detect ms vs s.
    median_val = float(numeric.median(skipna=True))
    # Millisecond timestamps for modern dates are ~1e12 (e.g. 1_736_722_800_000)
    if median_val > 1e12:
        converted = (numeric // 1000).astype("Int64")
        return converted, True

    return numeric.astype("Int64"), False


def _coerce_indicator_columns(
    df: pd.DataFrame, indicators: Optional[Iterable[str]] = None
) -> pd.DataFrame:
    """
    Coerce indicator columns to Float64 where present.

    - If indicators is None, auto-detect common indicators listed in
      _INDICATOR_DEFAULTS.
    - If indicators is provided, coerce only those names that exist.
    """
    if df is None or df.empty:
        return df

    if indicators is None:
        to_check = [c for c in _INDICATOR_DEFAULTS if c in df.columns]
    else:
        to_check = [c for c in indicators if c in df.columns]

    for col in to_check:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    return df


def normalize(
    df: pd.DataFrame,
    drop_incomplete: bool = True,
    indicators: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Normalize a raw DataFrame into canonical typed columns.

    - canonical columns: time, open, high, low, close, volume
    - time is integer seconds (auto-convert ms->s)
    - OHLC -> Float64, volume -> Int64
    - optional: coerce indicator columns (ema, vwap, atr, or the
      names passed via `indicators`) to Float64.
    """
    if df is None:
        raise ValueError("normalize: df must not be None")

    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    work = df.copy(deep=False)

    rename_map = _canonicalize_columns(work.columns)
    work = work.rename(columns=rename_map)

    # collapse cases where multiple original columns mapped to the same name
    work = _collapse_duplicate_columns(work)

    missing = [c for c in REQUIRED_COLUMNS if c not in work.columns]
    if missing:
        raise MissingColumnsError(f"Missing required columns: {missing}")

    time_ser, converted = _detect_and_normalize_time_series(work["time"])
    work["time"] = time_ser

    if converted:
        LOGGER.debug("normalize: converted timestamps from ms to s")

    for col in ("open", "high", "low", "close"):
        work[col] = pd.to_numeric(work[col], errors="coerce").astype("Float64")

    work["volume"] = pd.to_numeric(work["volume"], errors="coerce").astype("Int64")

    if drop_incomplete:
        before = len(work)
        work = work.dropna(subset=REQUIRED_COLUMNS)
        after = len(work)
        LOGGER.debug("normalize: dropped %d rows", before - after)

    # coerce indicator columns if requested (or auto-detect)
    work = _coerce_indicator_columns(work, indicators)

    if "time" in work.columns and not work["time"].isna().all():
        work["time"] = work["time"].astype("int64")

    work = work.reset_index(drop=True)
    return work


def deduplicate(df: pd.DataFrame, strategy: str = "last") -> pd.DataFrame:
    """Deduplicate by time according to given strategy."""
    if df is None:
        raise ValueError("deduplicate: df must not be None")

    if df.empty:
        return df

    if "time" not in df.columns:
        raise MissingColumnsError("time required for deduplication")

    if strategy == "last":
        return df.drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)

    if strategy == "first":
        return df.drop_duplicates(subset=["time"], keep="first").reset_index(drop=True)

    if strategy == "max_volume":
        idx = df.groupby("time")["volume"].idxmax()
        idx = idx.dropna().astype(int)
        result = df.loc[idx].sort_index().reset_index(drop=True)
        return result

    raise ValueError(f"Unknown dedupe strategy: {strategy}")


def merge_frames(
    frames: Iterable[Union[pd.DataFrame, "Path", str]],
    *,
    dedupe_strategy: str = "last",
    drop_incomplete: bool = True,
    sort_order: str = "asc",
    indicators: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Merge frames (or file paths) into a single canonical DataFrame.

    New optional `indicators` parameter is forwarded to normalize so callers
    can request explicit indicator coercion. Default None -> auto detect.
    """
    dfs: List[pd.DataFrame] = []
    from . import io as io_module  # local import avoids circular import

    for item in frames:
        if item is None:
            continue
        if isinstance(item, pd.DataFrame):
            raw = item
        else:
            # allow both Path and str; io.read_csv accepts a path-like
            raw = io_module.read_csv(str(item))
        # forward indicators setting into normalize
        norm = normalize(raw, drop_incomplete=drop_incomplete, indicators=indicators)
        if not norm.empty:
            dfs.append(norm)

    if not dfs:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    combined = pd.concat(dfs, ignore_index=True)
    deduped = deduplicate(combined, strategy=dedupe_strategy)

    asc = sort_order == "asc"
    if "time" in deduped.columns and not deduped.empty:
        deduped = deduped.sort_values("time", ascending=asc).reset_index(drop=True)

    return deduped


def summarize(df: pd.DataFrame) -> Dict[str, Optional[int]]:
    """Return a small summary dict for a DataFrame."""
    if df is None or df.empty:
        return {
            "rows": 0,
            "start_time": None,
            "end_time": None,
            "duplicates": 0,
        }

    rows = int(len(df))
    start_time = (
        int(df["time"].min())
        if "time" in df.columns and not df["time"].isna().all()
        else None
    )
    end_time = (
        int(df["time"].max())
        if "time" in df.columns and not df["time"].isna().all()
        else None
    )
    duplicates = int(df["time"].duplicated().sum()) if "time" in df.columns else 0

    return {
        "rows": rows,
        "start_time": start_time,
        "end_time": end_time,
        "duplicates": duplicates,
    }
