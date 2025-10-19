# tests/unit/test_timeutils_remaining.py
from __future__ import annotations

import builtins
import importlib
from datetime import datetime, timezone

from tvparser import timeutils


def test_parse_date_accepts_4_digit_year() -> None:
    """_mm/dd/yyyy_ form should produce a 4-digit year correctly."""
    y, m, d = timeutils._parse_date("10/13/2024")
    assert y == 2024
    assert m == 10
    assert d == 13


def test_parse_time_invalid_range_raises() -> None:
    """Hours >=24 or minutes >=60 should raise ValueError."""
    with __import__("pytest").raises(ValueError):
        timeutils._parse_time("24:00")
    with __import__("pytest").raises(ValueError):
        timeutils._parse_time("12:60")


def test_window_start_end_returns_milliseconds_when_requested() -> None:
    """window_start_end should honor to_ms=True and return ms values."""
    s_ms, e_ms = timeutils.window_start_end(
        "10/13/24", "17:00", "07:00", tz="UTC", to_ms=True
    )
    # check they are multiples of 1000 (ms) and end>start
    assert s_ms % 1000 == 0
    assert e_ms % 1000 == 0
    assert e_ms > s_ms


def test_import_timeutils_without_zoneinfo_triggers_except_branch():
    """
    Reload timeutils while forcing ImportError for 'zoneinfo' so the
    module's import-time except block runs (ZoneInfo assigned None).
    """
    # save originals
    orig_import = builtins.__import__
    tw_mod = importlib.import_module("tvparser.timeutils")

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        # force ImportError for stdlib 'zoneinfo' import attempts
        if name == "zoneinfo":
            raise ImportError("simulate missing zoneinfo")
        return orig_import(name, globals, locals, fromlist, level)

    # patch __import__ and reload the module to execute the except block
    builtins.__import__ = fake_import
    try:
        importlib.reload(tw_mod)
        # when zoneinfo import fails, module should set ZoneInfo = None
        assert tw_mod.ZoneInfo is None
    finally:
        # restore original import and reload to restore normal state
        builtins.__import__ = orig_import
        importlib.reload(tw_mod)


def test_to_timestamp_with_none_tz_uses_utc() -> None:
    """to_timestamp(tz=None) should behave like UTC."""
    ts = timeutils.to_timestamp("10/13/24", "17:00", tz=None)
    expected = int(datetime(2024, 10, 13, 17, 0, tzinfo=timezone.utc).timestamp())
    assert ts == expected
