from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from tvparser import timeutils


def test_to_timestamp_utc_seconds() -> None:
    """to_timestamp returns the correct epoch seconds in UTC."""
    ts = timeutils.to_timestamp("10/13/24", "17:00", tz="UTC")
    expected_dt = datetime(2024, 10, 13, 17, 0, tzinfo=timezone.utc)
    expected = int(expected_dt.timestamp())
    assert ts == expected


def test_to_timestamp_returns_milliseconds_when_requested() -> None:
    """to_timestamp returns milliseconds when to_ms=True."""
    ms = timeutils.to_timestamp("10/25/24", "00:00", tz="UTC", to_ms=True)
    expected_dt = datetime(2024, 10, 25, 0, 0, tzinfo=timezone.utc)
    expected = int(expected_dt.timestamp())
    assert ms == expected * 1000


def test_window_crosses_midnight_next_day() -> None:
    """
    If end_time is earlier than start_time, assume end is next calendar day.
    """
    start_ts, end_ts = timeutils.window_start_end(
        "10/13/24",
        "17:00",
        "07:00",
        tz="UTC",
    )
    expected_dt = datetime(2024, 10, 13, 17, 0, tzinfo=timezone.utc)
    start_exp = int(expected_dt.timestamp())
    expected_dt = datetime(2024, 10, 14, 7, 0, tzinfo=timezone.utc)
    end_exp = int(expected_dt.timestamp())
    assert start_ts == start_exp
    assert end_ts == end_exp


def test_timezone_aware_chicago() -> None:
    """
    Ensure timezone names are accepted (America/Chicago).
    Timestamps should reflect that zone.
    """
    tz = "America/Chicago"
    ts = timeutils.to_timestamp("10/13/24", "17:00", tz=tz)
    expected_dt = datetime(
        2024, 10, 13, 17, 0, tzinfo=ZoneInfo(tz)
    )
    expected = int(expected_dt.timestamp())
    assert ts == expected
