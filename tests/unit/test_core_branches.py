from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from tvparser import core


def _row(ts: int) -> dict[str, Any]:
    return {
        "time": ts,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
    }


def test_normalize_none_raises() -> None:
    """normalize must reject None input (guard clause)."""
    with pytest.raises(ValueError):
        core.normalize(None)  # type: ignore[arg-type]


def test_normalize_empty_df_returns_expected_columns() -> None:
    """Empty DataFrame should return a frame with required columns."""
    empty = pd.DataFrame()
    out = core.normalize(empty, drop_incomplete=True)
    # should return a DataFrame with at least required cols
    for c in ("time", "open", "high", "low", "close", "volume"):
        assert c in out.columns


def test_missing_required_columns_raises() -> None:
    """If required columns are missing, raise MissingColumnsError."""
    df = pd.DataFrame({"time": [1, 2, 3]})
    with pytest.raises(core.MissingColumnsError):
        core.normalize(df)


def test_deduplicate_unknown_strategy_raises() -> None:
    """Pass an invalid dedupe strategy and expect ValueError."""
    df = pd.DataFrame([_row(1), _row(2)])
    with pytest.raises(ValueError):
        core.deduplicate(df, strategy="nope")


def test_deduplicate_max_volume_handles_all_nan_volume() -> None:
    """
    If volume is NaN for a time group, idxmax may dropna; ensure no crash.
    Returns either empty or reasonable result rather than raising.
    """
    rows = [
        {"time": 100, "open": 1, "high": 2, "low": 0, "close": 1, "volume": None},
        {"time": 100, "open": 1, "high": 2, "low": 0, "close": 1, "volume": None},
    ]
    df = pd.DataFrame(rows)
    # Should not raise; result may be empty or one row depending on impl.
    out = core.deduplicate(df, strategy="max_volume")
    assert isinstance(out, pd.DataFrame)


def test_merge_frames_calls_io_read_csv_for_paths(monkeypatch, tmp_path: Path) -> None:
    """
    When merge_frames gets path-like items, it should call io.read_csv.
    We monkeypatch tvparser.io.read_csv and verify it's used.
    """
    calls: list[str] = []

    def fake_read_csv(p: str) -> pd.DataFrame:
        calls.append(str(p))
        # return a minimal valid dataframe
        return pd.DataFrame([_row(1)])

    monkeypatch.setattr("tvparser.io.read_csv", fake_read_csv)
    # pass strings (representing paths)
    merged = core.merge_frames(["a.csv", "b.csv"], dedupe_strategy="last")
    assert calls, "io.read_csv should have been called"
    assert "a.csv" in calls[0] or "b.csv" in calls[0]
    # merged should contain canonical columns
    assert "time" in merged.columns


def test_merge_frames_with_no_frames_returns_empty_df() -> None:
    """If no readable frames present, return an empty canonical df."""
    out = core.merge_frames([], dedupe_strategy="last")
    assert isinstance(out, pd.DataFrame)
    assert out.empty or set(out.columns) >= set(
        ("time", "open", "high", "low", "close", "volume")
    )


def test_merge_frames_sort_desc() -> None:
    """Ensure the desc sort_order branch sorts descending by time."""
    a = pd.DataFrame([_row(10)])
    b = pd.DataFrame([_row(20)])
    merged = core.merge_frames([a, b], sort_order="desc")
    # times should be descending
    times = list(merged["time"])
    assert times == sorted(times, reverse=True)


def test_summarize_empty_and_nonempty() -> None:
    """Verify summarize returns expected keys for empty and normal df."""
    empty = pd.DataFrame()
    s = core.summarize(empty)
    assert s["rows"] == 0
    assert s["start_time"] is None
    assert s["end_time"] is None

    df = pd.DataFrame([_row(123), _row(456)])
    df = core.normalize(df, drop_incomplete=False)
    s2 = core.summarize(df)
    assert s2["rows"] == int(len(df))
    assert s2["start_time"] == int(df["time"].min())
    assert s2["end_time"] == int(df["time"].max())
