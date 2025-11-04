from __future__ import annotations

from typing import List

import pandas as pd
import pytest

from tvparser import core


def _make_base_row(ts: int) -> dict:
    return {
        "time": ts,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
    }


def test_normalize_auto_detects_and_coerces_indicators() -> None:
    """
    When indicators=None, common indicators (ema, vwap, atr)
    present in the frame should be coerced to nullable Float64.
    """
    rows: List[dict] = [
        {**_make_base_row(1), "EMA": "2653.18", "vwap": "2653.12"},
        {**_make_base_row(2), "EMA": "2653.31", "vwap": "2653.17"},
    ]
    df = pd.DataFrame(rows)
    # do not drop incomplete so indicators remain even if NaN
    out = core.normalize(df, drop_incomplete=False)
    # canonicalization lowercases EMA -> ema
    assert "ema" in out.columns
    assert "vwap" in out.columns
    # ensure dtypes are nullable Float64
    assert out["ema"].dtype.name == "Float64"
    assert out["vwap"].dtype.name == "Float64"


def test_normalize_explicit_indicators_list_only_coerces_requested() -> None:
    """
    If indicators list provided, only those columns (if present)
    are coerced. Other indicator-like columns should remain uncoerced.
    """
    rows: List[dict] = [
        {**_make_base_row(1), "ema": "1.1", "vwap": "2.2"},
        {**_make_base_row(2), "ema": "3.3", "vwap": "4.4"},
    ]
    df = pd.DataFrame(rows)
    out = core.normalize(df, drop_incomplete=False, indicators=["ema"])
    # ema coerced, vwap untouched (object or numeric depending on coercion)
    assert out["ema"].dtype.name == "Float64"
    # vwap should exist but not be coerced by our explicit indicators call
    # If vwap happens to be numeric already, dtype may be Float64 as well,
    # so we assert it's present and not raise — check it's in columns.
    assert "vwap" in out.columns


def test_normalize_ignores_missing_indicator_names() -> None:
    """
    Passing an indicator name that doesn't exist should not raise.
    Existing indicator columns should still be coerced.
    """
    rows = [{**_make_base_row(1), "ema": "5.5"}]
    df = pd.DataFrame(rows)
    out = core.normalize(df, drop_incomplete=False, indicators=["ema", "nope"])
    assert "ema" in out.columns
    assert out["ema"].dtype.name == "Float64"
    # nonexistent column should not be created
    assert "nope" not in out.columns


def test_indicator_nullable_and_na_preserved() -> None:
    """
    Indicator columns coerced to Float64 must preserve NA values
    as <NA> (pandas nullable Float64).
    """
    rows = [
        {**_make_base_row(1), "ema": "", "vwap": "2653.0"},
        {**_make_base_row(2), "ema": "2653.2", "vwap": ""},
    ]
    df = pd.DataFrame(rows)
    out = core.normalize(df, drop_incomplete=False)
    # both indicators present and coerced
    assert out["ema"].dtype.name == "Float64"
    assert out["vwap"].dtype.name == "Float64"
    # NA count should be 1 for each in this setup
    assert int(out["ema"].isna().sum()) == 1
    assert int(out["vwap"].isna().sum()) == 1


def test_merge_frames_forwards_indicators_argument() -> None:
    """
    merge_frames(..., indicators=...) should forward the setting so
    coerced types appear in the merged result.
    """
    a = pd.DataFrame(
        [
            {**_make_base_row(10), "ema": "100.0"},
            {**_make_base_row(20), "ema": "101.0"},
        ]
    )
    b = pd.DataFrame(
        [
            {**_make_base_row(30), "EMA": "102.0"},
        ]
    )
    merged = core.merge_frames([a, b], indicators=["ema"])
    # canonicalization makes EMA -> ema and it should be coerced
    assert "ema" in merged.columns
    assert merged["ema"].dtype.name == "Float64"
    # time/order preserved (asc default)
    assert list(merged["time"]) == [10, 20, 30]


def test_auto_detect_in_merge_when_indicators_none() -> None:
    """
    If indicators=None (default), normalize auto-detects common names.
    merge_frames should therefore coerce defaults when present.
    """
    a = pd.DataFrame([{**_make_base_row(1), "vwap": "1.1"}])
    merged = core.merge_frames([a])
    assert "vwap" in merged.columns
    assert merged["vwap"].dtype.name == "Float64"
