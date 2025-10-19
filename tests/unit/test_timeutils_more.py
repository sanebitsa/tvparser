from __future__ import annotations

from datetime import datetime, timezone

from tvparser import timeutils


def test_to_timestamp_with_ms() -> None:
    """to_timestamp returns milliseconds when to_ms=True."""
    ms = timeutils.to_timestamp("10/25/24", "00:00", tz="UTC", to_ms=True)
    expected = int(
        datetime(2024, 10, 25, 0, 0, tzinfo=timezone.utc).timestamp()
    )
    assert ms == expected * 1000


def test_zoneinfo_missing_fallback_to_utc(monkeypatch) -> None:
    """
    Simulate missing zoneinfo by monkeypatching ZoneInfo to None and
    ensure named tz doesn't crash and falls back to UTC.
    """
    monkeypatch.setattr(timeutils, "ZoneInfo", None)
    # calling with a non-UTC tz name should not raise
    ts = timeutils.to_timestamp("10/13/24", "17:00", tz="America/Chicago")
    expected = int(
        datetime(2024, 10, 13, 17, 0, tzinfo=timezone.utc).timestamp()
    )
    assert ts == expected


def test_window_start_end_to_next_day() -> None:
    """Ensure window that crosses midnight returns end > start."""
    start, end = timeutils.window_start_end(
        "10/13/24", "23:30", "01:30", tz="UTC"
    )
    assert end > start
