from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tvparser import timeutils

SECONDS_PER_DAY = 24 * 3600


def test_align_into_window_forward_backward_and_noop() -> None:
    """
    Test new helper align_into_window for:
    - forward shift into a cross-midnight window
    - backward shift into a cross-midnight window
    - no-op when alignment is impossible
    """
    date = "10/13/24"
    # cross-midnight window 17:00 -> 07:00 (end is next day)
    start_ts, end_ts = timeutils.window_start_end(date, "17:00", "07:00", tz="UTC")

    # base entry at 01:30 on same calendar date should be before start
    entry_base = timeutils.to_timestamp(date, "01:30", tz="UTC")
    assert entry_base < start_ts

    # forward shift by one day should bring it into window
    entry_forward = timeutils.align_into_window(entry_base, start_ts, end_ts)
    assert start_ts <= entry_forward <= end_ts
    assert entry_forward == entry_base + SECONDS_PER_DAY

    # create a base exit that is two days after intended exit,
    # it should be shifted backward into the window
    exit_base = timeutils.to_timestamp(date, "07:00", tz="UTC") + 2 * SECONDS_PER_DAY
    assert exit_base > end_ts
    exit_back = timeutils.align_into_window(exit_base, start_ts, end_ts)
    assert start_ts <= exit_back <= end_ts
    # should have been shifted backward at least one day
    assert exit_back <= exit_base - SECONDS_PER_DAY

    # a window that is short (09:00 -> 10:00) and an entry at 09:00
    # shifting forward will not fit into the window, so original should be returned
    s_short, e_short = timeutils.window_start_end(date, "09:00", "10:00", tz="UTC")
    base = timeutils.to_timestamp(date, "08:00", tz="UTC")
    assert base < s_short
    # trying to align should return the original since moving forward
    # places it outside the tiny window
    aligned = timeutils.align_into_window(base, s_short, e_short)
    assert aligned == base


def test_to_timestamp_fold_behavior_for_ambiguous_time() -> None:
    """
    When ZoneInfo is available, ambiguous times during DST fall-back
    should differ depending on fold=0 vs fold=1.
    """
    if timeutils.ZoneInfo is None:
        pytest.skip("zoneinfo not available; skipping fold test")

    # US 2024 DST ends on 2024-11-03; 01:30 occurs twice in many zones
    date = "11/03/24"
    t = "01:30"
    ts_fold0 = timeutils.to_timestamp(date, t, tz="America/Chicago", fold=0)
    ts_fold1 = timeutils.to_timestamp(date, t, tz="America/Chicago", fold=1)
    assert ts_fold0 != ts_fold1
    # both should be integer seconds and reasonable values
    assert isinstance(ts_fold0, int) and isinstance(ts_fold1, int)


def test_tzinfo_for_name_raises_on_invalid_name_when_zoneinfo_present() -> None:
    """
    If ZoneInfo exists, passing an invalid tz name to the helper should
    raise a ValueError. If ZoneInfo is missing, the function should
    return UTC tzinfo.
    """
    if timeutils.ZoneInfo is None:
        # behaviour: fallback to UTC when zoneinfo absent
        tzinfo = timeutils._tzinfo_for_name("NotARealTZ")
        assert tzinfo.utcoffset(None) == timezone.utc.utcoffset(None)
    else:
        with pytest.raises(ValueError):
            timeutils._tzinfo_for_name("NotARealTZ")


def test_window_start_end_and_to_ms_flag_and_full_day() -> None:
    """
    Verify window_start_end respects to_ms flag and that start==end
    yields a 24-hour window (end > start by one day).
    """
    s_ms, e_ms = timeutils.window_start_end(
        "10/13/24", "17:00", "07:00", tz="UTC", to_ms=True
    )
    assert e_ms > s_ms
    assert s_ms % 1000 == 0 and e_ms % 1000 == 0

    # start == end treated as next day -> 24h window
    s24, e24 = timeutils.window_start_end("10/13/24", "00:00", "00:00", tz="UTC")
    assert e24 - s24 == SECONDS_PER_DAY


def test_full_pipeline_record_has_ordering() -> None:
    """
    End-to-end: compute start/end, compute raw entry/exit and align,
    then ensure the final ordering holds: start <= entry <= exit <= end.
    """
    date = "10/13/24"
    start_s = "17:00"
    entry_s = "19:30"
    exit_s = "23:30"
    end_s = "07:00"

    start_ts, end_ts = timeutils.window_start_end(date, start_s, end_s, tz="UTC")
    entry_base = timeutils.to_timestamp(date, entry_s, tz="UTC")
    exit_base = timeutils.to_timestamp(date, exit_s, tz="UTC")

    entry_aligned = timeutils.align_into_window(entry_base, start_ts, end_ts)
    exit_aligned = timeutils.align_into_window(exit_base, start_ts, end_ts)

    assert start_ts <= entry_aligned <= exit_aligned <= end_ts


def test_to_timestamp_to_ms_true_returns_ms() -> None:
    """to_timestamp should return milliseconds when to_ms=True."""
    ms = timeutils.to_timestamp("10/25/24", "00:00", tz="UTC", to_ms=True)
    expected = int(datetime(2024, 10, 25, 0, 0, tzinfo=timezone.utc).timestamp())
    assert ms == expected * 1000
