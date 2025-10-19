from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from tvparser import core


def test_normalize_converts_milliseconds_to_seconds() -> None:
    """
    If times look like milliseconds (large values), they should be
    converted to seconds in normalize().
    """
    # milliseconds timestamps (add three zeros)
    rows: List[Dict[str, Any]] = [
        {
            "time": 1736722800000,
            "open": 1,
            "high": 2,
            "low": 0,
            "close": 1,
            "Volume": 1,
        },
        {
            "time": 1736722860000,
            "open": 2,
            "high": 3,
            "low": 1,
            "close": 2,
            "Volume": 2,
        },
    ]
    df = pd.DataFrame(rows)
    out = core.normalize(df, drop_incomplete=True)
    assert list(out["time"]) == [1736722800, 1736722860]


def test_collapse_duplicate_named_columns_preserves_values() -> None:
    """
    When input has both 'Volume' and 'volume' columns, collapse should
    pick first non-null value per row.
    """
    rows = [
        {
            "time": 1,
            "open": 1,
            "high": 2,
            "low": 0,
            "close": 1,
            "Volume": 10,
            "volume": None
        },
        {
            "time": 2,
            "open": 2,
            "high": 3,
            "low": 1,
            "close": 2,
            "Volume": None,
            "volume": 20,
        },
    ]
    df = pd.DataFrame(rows)
    out = core.normalize(df, drop_incomplete=True)
    # ensure single 'volume' column present and values coalesced left-to-right
    assert "volume" in out.columns
    assert (
        list(out["volume"].astype(int))
        == [10, 20]
    )


def test_deduplicate_max_volume_handles_na_volumes() -> None:
    """
    If some rows have NA volumes, max_volume should ignore NA groups.
    """
    rows = [
        {
            "time": 100,
            "open": 1,
            "high": 2,
            "low": 0,
            "close": 1,
            "Volume": None,
        },
        {
            "time": 100,
            "open": 1.1,
            "high": 2.1,
            "low": 0.1,
            "close": 1.1,
            "Volume": 5,
        },
        {
            "time": 200,
            "open": 2,
            "high": 3,
            "low": 1,
            "close": 2,
            "Volume": None,
        },
    ]
    df = pd.DataFrame(rows)
    norm = core.normalize(df, drop_incomplete=False)
    # max_volume should keep the one with volume 5 for time=100
    deduped = core.deduplicate(norm, strategy="max_volume")
    assert int(
        deduped.loc[deduped["time"] == 100, "volume"].iloc[0]
    ) == 5
