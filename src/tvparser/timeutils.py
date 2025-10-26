from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from typing import Tuple, Union

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

import logging

LOGGER = logging.getLogger("tvparser.timeutils")


def _parse_date(date_str: str) -> Tuple[int, int, int]:
    """Parse 'MM/DD/YY' or 'MM/DD/YYYY' -> (year, month, day)."""
    parts = date_str.strip().split("/")
    if len(parts) != 3:
        raise ValueError("date must be MM/DD/YY or MM/DD/YYYY")
    m, d, y = parts
    month = int(m)
    day = int(d)
    yy = int(y)
    year = 2000 + yy if yy < 100 else yy
    return year, month, day


def _parse_time(time_str: str) -> Tuple[int, int]:
    """Parse 'HH:MM' or 'H:MM' into (hour, minute)."""
    parts = time_str.strip().split(":")
    if len(parts) != 2:
        raise ValueError("time must be HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError("invalid hour or minute")
    return hour, minute


def _tzinfo_for_name(tz: Union[str, None]) -> tzinfo:
    if tz is None or tz == "UTC":
        return timezone.utc
    if ZoneInfo is None:
        LOGGER.debug("zoneinfo not available, falling back to UTC")
        return timezone.utc
    try:
        return ZoneInfo(tz)
    except Exception as exc:
        raise ValueError(f"unknown timezone {tz!r}") from exc


def to_timestamp(
    date_str: str,
    time_str: str,
    tz: Union[str, None] = "UTC",
    *,
    to_ms: bool = False,
    fold: int | None = None,
) -> int:
    """
    Convert date + time to unix epoch seconds (or ms if to_ms=True).

    date_str: 'MM/DD/YY' or 'MM/DD/YYYY'
    time_str: 'HH:MM'
    tz: IANA name (e.g. 'America/Chicago') or 'UTC' or None -> UTC
    """
    year, month, day = _parse_date(date_str)
    hour, minute = _parse_time(time_str)

    tzinfo = _tzinfo_for_name(tz)
    dt = datetime(year, month, day, hour, minute, tzinfo=tzinfo)
    if fold is not None:
        dt = dt.replace(fold=fold)
    ts = int(dt.timestamp())
    return ts * 1000 if to_ms else ts


def window_start_end(
    date_str: str,
    start_time: str,
    end_time: str,
    tz: Union[str, None] = "UTC",
    *,
    to_ms: bool = False,
) -> Tuple[int, int]:
    """
    Return (start_ts, end_ts) in seconds (unless to_ms=True).

    If end_time <= start_time, end is assumed next calendar day.
    """
    start = to_timestamp(date_str, start_time, tz=tz, to_ms=False)
    end = to_timestamp(date_str, end_time, tz=tz, to_ms=False)

    if end <= start:
        # end on next day
        year, month, day = _parse_date(date_str)
        ehour, emin = _parse_time(end_time)
        tzinfo = _tzinfo_for_name(tz)
        end_dt = datetime(year, month, day, ehour, emin, tzinfo=tzinfo)
        end_dt = end_dt + timedelta(days=1)
        end = int(end_dt.timestamp())

    if to_ms:
        return (start * 1000, end * 1000)
    return (start, end)


SECONDS_PER_DAY = 86400


def align_into_window(ts: int, start_ts: int, end_ts: int) -> int:
    if start_ts <= ts <= end_ts:
        return ts
    if ts < start_ts:
        steps = (start_ts - ts + SECONDS_PER_DAY - 1) // SECONDS_PER_DAY
        cand = ts + steps * SECONDS_PER_DAY
        if cand <= end_ts:
            return cand
    if ts > end_ts:
        steps = (ts - end_ts + SECONDS_PER_DAY - 1) // SECONDS_PER_DAY
        cand = ts - steps * SECONDS_PER_DAY
        if cand >= start_ts:
            return cand
    return ts
