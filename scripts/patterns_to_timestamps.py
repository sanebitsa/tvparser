#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable, List, Dict

from tvparser import timeutils

BASE_DIR = Path("/home/dribble/Development/tvdata")
DEFAULT_TZ = "America/Chicago"


def _resolve_pattern_path(arg: str | None) -> Path:
    if arg is None:
        # default file name in base dir
        return BASE_DIR / "range_breakout.txt"
    p = Path(arg)
    if arg.find(os.sep) != -1 or p.suffix:
        return p
    return BASE_DIR / f"{arg}.txt"


def _iter_rows(lines: Iterable[str]) -> Iterable[List[str]]:
    """
    Yield tokenized rows (date, start, entry, exit, end).
    Skip a header line if it looks like one.
    """
    it = iter(lines)
    first = True
    for raw in it:
        line = raw.strip()
        if not line:
            continue
        if first:
            first = False
            low = line.lower()
            if "date" in low and "start" in low:
                # skip header-looking line
                continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            raise ValueError(f"malformed line (need 5 cols): {line!r}")
        yield parts[:5]


def lines_to_timestamp_records(
    lines: Iterable[str], tz: str = DEFAULT_TZ
) -> List[Dict[str, int]]:
    """
    Parse lines of the pattern file and return a list of records where
    each record contains integer-second timestamps for start, end,
    entry and exit.
    """
    records: List[Dict[str, int]] = []
    for date_str, start_s, entry_s, exit_s, end_s in _iter_rows(lines):
        # compute canonical start and end (handles cross-midnight)
        start_ts, end_ts = timeutils.window_start_end(
            date_str, start_s, end_s, tz=tz, to_ms=False
        )

        # compute raw entry/exit on the same calendar date
        entry_ts = timeutils.to_timestamp(date_str, entry_s, tz=tz)
        exit_ts = timeutils.to_timestamp(date_str, exit_s, tz=tz)

        # align into the window using the library helper
        entry_aligned = timeutils.align_into_window(entry_ts, start_ts, end_ts)
        exit_aligned = timeutils.align_into_window(exit_ts, start_ts, end_ts)

        # include values as ints (seconds)
        records.append(
            {
                "start": int(start_ts),
                "end": int(end_ts),
                "entry": int(entry_aligned),
                "exit": int(exit_aligned),
            }
        )
    return records


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="patterns_to_timestamps",
        description="Convert SLrun-like pattern txt -> JSON array of timestamps",
    )
    p.add_argument(
        "pattern",
        nargs="?",
        help=("pattern basename or path (default: range_breakout in base dir)"),
    )
    p.add_argument(
        "out",
        nargs="?",
        help="output .json path (default: same name with .timestamps.json)",
    )
    p.add_argument(
        "--tz",
        default=DEFAULT_TZ,
        help=f"Timezone name (default: {DEFAULT_TZ})",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="write indented JSON",
    )
    args = p.parse_args(argv)

    pattern_path = _resolve_pattern_path(args.pattern)
    if not pattern_path.exists():
        print("pattern file not found:", pattern_path, file=sys.stderr)
        return 2

    out_path = Path(args.out) if args.out else pattern_path.with_suffix(".json")

    lines = pattern_path.read_text(encoding="utf-8").splitlines()
    try:
        records = lines_to_timestamp_records(lines, tz=args.tz)
    except Exception as exc:
        print("failed to parse pattern file:", exc, file=sys.stderr)
        return 3

    if args.pretty:
        out_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    else:
        out_path.write_text(
            json.dumps(records, separators=(",", ":")), encoding="utf-8"
        )

    print(f"Wrote {len(records)} records to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
