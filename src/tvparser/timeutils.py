from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple, Union

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - older Pythons
    ZoneInfo = None  # type: ignore


def _parse_date(date_str: str) -> Tuple[int, int, int]:
    """Parse date 'MM/DD/YY' or 'MM/DD/YYYY' -> (year, month, day)."""
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
    """Parse time like 'HH:MM' or 'H:MM' into (hour, minute)."""
    parts = time_str.strip().split(":")
    if len(parts) != 2:
        raise ValueError("time must be HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError("invalid hour or minute")
    return hour, minute


def to_timestamp(
    date_str: str,
    time_str: str,
    tz: Union[str, None] = "UTC",
    *,
    to_ms: bool = False,
) -> int:
    """
    Convert date + time strings to a unix timestamp.

    - date_str: 'MM/DD/YY' or 'MM/DD/YYYY'
    - time_str: 'HH:MM' (24h)
    - tz: timezone name (e.g. 'UTC' or 'America/Chicago') or None -> UTC
    - to_ms: return milliseconds if True (default: seconds)
    """
    year, month, day = _parse_date(date_str)
    hour, minute = _parse_time(time_str)

    if tz is None or tz == "UTC":
        tzinfo = timezone.utc
    else:
        if ZoneInfo is None:
            raise RuntimeError("zoneinfo not available for tz names")
        tzinfo = ZoneInfo(tz)

    dt = datetime(year, month, day, hour, minute, tzinfo=tzinfo)
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
    Return (start_ts, end_ts) for a window starting on date_str/start_time.

    If end_time is earlier than or equal to start_time we assume the end is
    on the next calendar day (e.g. start 17:00 end 07:00 -> next day).
    """
    # compute in seconds first
    start = to_timestamp(date_str, start_time, tz=tz, to_ms=False)
    end = to_timestamp(date_str, end_time, tz=tz, to_ms=False)

    if end <= start:
        # end is next day in same timezone
        year, month, day = _parse_date(date_str)
        ehour, emin = _parse_time(end_time)
        if tz is None or tz == "UTC":
            tzinfo = timezone.utc
        else:
            tzinfo = ZoneInfo(tz) if ZoneInfo is not None else timezone.utc

        end_dt = datetime(year, month, day, ehour, emin, tzinfo=tzinfo)
        end_dt = end_dt + timedelta(days=1)
        end = int(end_dt.timestamp())

    if to_ms:
        return (start * 1000, end * 1000)
    return (start, end)
