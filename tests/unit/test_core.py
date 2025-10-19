from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import pytest

from tvparser import core


def _make_row(
    ts: int,
    o: float,
    h: float,
    low: float,
    c: float,
    v: int,
) -> Dict[str, Any]:
    return {
        "time": ts,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "Volume": v,
    }


def test_normalize_basic() -> None:
    rows = [
        _make_row(1000, 10.0, 11.0, 9.5, 10.5, 5),
        _make_row(1060, "12.0", "12.5", "11.5", "12.0", "7"),
    ]
    df = pd.DataFrame(rows)

    norm = core.normalize(df, drop_incomplete=True)

    assert set(norm.columns) >= {
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }

    assert norm["time"].dtype.kind in ("i", "u")
    assert list(norm["time"]) == [1000, 1060]

    for col in ("open", "high", "low", "close"):
        assert pd.api.types.is_float_dtype(norm[col])

    assert pd.api.types.is_integer_dtype(norm["volume"])


def test_normalize_drop_incomplete() -> None:
    rows = [
        _make_row(2000, 20.0, 21.0, 19.5, 20.5, 10),
        {
            "time": 2060,
            "open": None,
            "high": 22.0,
            "low": 20.0,
            "close": 21.0,
            "volume": 3,
        },
    ]
    df = pd.DataFrame(rows)

    kept = core.normalize(df, drop_incomplete=True)
    assert len(kept) == 1

    kept2 = core.normalize(df, drop_incomplete=False)
    assert len(kept2) == 2


def test_deduplicate_strategies() -> None:
    rows = [
        _make_row(3000, 30.0, 31.0, 29.5, 30.5, 100),
        _make_row(3000, 30.1, 31.1, 29.6, 30.6, 150),
        _make_row(3060, 31.0, 32.0, 30.0, 31.0, 50),
    ]
    df = pd.DataFrame(rows)
    df = core.normalize(df, drop_incomplete=True)

    last = core.deduplicate(df, strategy="last")
    assert len(last) == 2
    assert int(last.loc[last["time"] == 3000, "volume"].iloc[0]) == 150

    first = core.deduplicate(df, strategy="first")
    assert int(first.loc[first["time"] == 3000, "volume"].iloc[0]) == 100

    maxv = core.deduplicate(df, strategy="max_volume")
    assert int(maxv.loc[maxv["time"] == 3000, "volume"].iloc[0]) == 150

    with pytest.raises(ValueError):
        core.deduplicate(df, strategy="nope")


def test_merge_frames_with_dataframes_and_sorting() -> None:
    a = pd.DataFrame([_make_row(4000, 40.0, 41.0, 39.5, 40.5, 1)])
    b = pd.DataFrame(
        [
            _make_row(3940, 39.0, 40.0, 38.5, 39.5, 2),
            _make_row(4000, 40.2, 41.2, 39.6, 40.6, 3),
        ]
    )
    c = pd.DataFrame([_make_row(4060, 41.0, 42.0, 40.0, 41.0, 4)])

    merged = core.merge_frames(
        [a, b, c],
        dedupe_strategy="max_volume",
        drop_incomplete=True,
        sort_order="asc",
    )

    assert list(merged["time"]) == [3940, 4000, 4060]
    assert int(merged.loc[merged["time"] == 4000, "volume"].iloc[0]) == 3

    merged_desc = core.merge_frames(
        [a, b, c],
        dedupe_strategy="last",
        drop_incomplete=True,
        sort_order="desc",
    )
    assert list(merged_desc["time"]) == sorted(
        merged_desc["time"], reverse=True
    )


def test_summarize_counts_correctly() -> None:
    rows = [
        _make_row(5000, 50.0, 51.0, 49.5, 50.5, 10),
        _make_row(5060, 51.0, 52.0, 50.0, 51.0, 20),
        _make_row(5060, 51.1, 52.1, 50.1, 51.1, 5),
    ]
    df = pd.DataFrame(rows)
    norm = core.normalize(df, drop_incomplete=True)

    merged = core.merge_frames(
        [norm],
        dedupe_strategy="last",
        drop_incomplete=True,
        sort_order="asc",
    )
    summary = core.summarize(merged)
    assert summary["rows"] == int(len(merged))
    assert summary["start_time"] == int(merged["time"].min())
    assert summary["end_time"] == int(merged["time"].max())
    assert set(summary.keys()) >= {"rows", "start_time", "end_time"}
