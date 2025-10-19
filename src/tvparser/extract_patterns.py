# src/tvparser/extract_patterns.py
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from . import slicer, timeutils


@dataclass(frozen=True)
class Window:
    """Minimal window representing one pattern row."""

    date_str: str
    start: str
    end: str
    start_ts: int
    end_ts: int


def parse_pattern_lines(lines: Iterable[str], tz: str = "UTC") -> List[Window]:
    """
    Parse lines from a SLrunLong-like file.

    Expects header: date, start, entry, exit, end
    Each data line must contain at least five comma-separated fields.
    Returns a list of Window objects with computed timestamps.
    """
    out: List[Window] = []
    first = True
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if first:
            first = False
            if "date" in line.lower():
                # header present, skip it
                continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            raise ValueError(f"malformed pattern line: {line!r}")
        date_str, start, _entry, _exit, end = parts[:5]
        start_ts, end_ts = timeutils.window_start_end(
            date_str, start, end, tz=tz, to_ms=False
        )
        out.append(Window(date_str, start, end, start_ts, end_ts))
    return out


def _iso_date_from_mdy(date_str: str) -> str:
    """Convert 'MM/DD/YY' or 'MM/DD/YYYY' -> 'YYYY-MM-DD'."""
    parts = [p.strip() for p in date_str.split("/")]
    if len(parts) != 3:
        raise ValueError("date must be MM/DD/YY or MM/DD/YYYY")
    m, d, y = parts
    month = int(m)
    day = int(d)
    yy = int(y)
    year = 2000 + yy if yy < 100 else yy
    return f"{year:04d}-{month:02d}-{day:02d}"


def format_window_filename(
    csv_path: Path,
    w: Window,
    numbered: bool = False,
    index: Optional[int] = None,
    prefix: str = "slrun_long",
    pad: int = 3,
    filename_template: str = "{stem}_{date}_{start}__{end}.csv",
) -> str:
    """
    Produce a filename for a Window.

    - If numbered=True, index must be provided and filename becomes
      '{prefix}{index:0{pad}d}.csv'.
    - Otherwise filename_template is used (default: old style).
    """
    if numbered:
        if index is None:
            raise ValueError("index required when numbered=True")
        return f"{prefix}{index:0{pad}d}.csv"

    # default template uses stem/date/start/end (colons -> hyphen)
    date_iso = _iso_date_from_mdy(w.date_str)
    start_s = w.start.replace(":", "-")
    end_s = w.end.replace(":", "-")
    stem = csv_path.stem
    return filename_template.format(stem=stem, date=date_iso, start=start_s, end=end_s)


def extract_from_patterns(
    slrun_path: Path,
    csv_path: Path,
    out_dir: Optional[Path] = None,
    tz: str = "UTC",
    numbered: bool = False,
    prefix: str = "slrun_long",
    pad: int = 3,
    filename_template: str = "{stem}_{date}_{start}__{end}.csv",
    force: bool = False,
    continue_on_error: bool = False,
) -> List[Path]:
    """
    Extract windows defined in slrun_path from csv_path.

    - numbered: if True output files are sequentially named using prefix/pad
    - filename_template: used when numbered is False
    Returns list of output file paths written (or skipped if exists and not
    forced).
    """
    if out_dir is None:
        out_dir = csv_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    content = slrun_path.read_text(encoding="utf-8").splitlines()
    windows = parse_pattern_lines(content, tz=tz)
    written: List[Path] = []

    for i, w in enumerate(windows, start=1):
        fname = format_window_filename(
            csv_path,
            w,
            numbered=numbered,
            index=(i if numbered else None),
            prefix=prefix,
            pad=pad,
            filename_template=filename_template,
        )
        outp = out_dir / fname
        if outp.exists() and not force:
            # skip existing unless force True
            continue
        try:
            _ = slicer.slice_csv_window(
                csv_path, w.start_ts, w.end_ts, outp, ts_column="ts"
            )
            written.append(outp)
        except Exception:
            if continue_on_error:
                continue
            raise

    return written


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tvparser.extract_patterns")
    p.add_argument(
        "--slrun",
        required=True,
        help="Path to SLrun-like file (date,start,entry,exit,end)",
    )
    p.add_argument("--csv", required=True, help="Path to 1-min CSV to slice")
    p.add_argument("--out-dir", default=None, help="Directory to write per-window CSVs")
    p.add_argument(
        "--tz", default="UTC", help="Timezone name for parsing dates (e.g. UTC)"
    )
    p.add_argument(
        "--numbered",
        action="store_true",
        help="Write outputs as prefixNNN.csv (see --prefix/--pad)",
    )
    p.add_argument(
        "--prefix",
        default="slrun_long",
        help="Prefix used when --numbered (default slrun_long)",
    )
    p.add_argument(
        "--pad", type=int, default=3, help="Zero-pad width when --numbered (default 3)"
    )
    p.add_argument(
        "--filename-template",
        default="{stem}_{date}_{start}__{end}.csv",
        help="Template for filenames when not numbered",
    )
    p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue when a window extraction fails",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    p = _build_parser()
    args = p.parse_args(argv)

    slrun = Path(args.slrun)
    csvp = Path(args.csv)
    outd = Path(args.out_dir) if args.out_dir else None

    if not slrun.exists():
        print(f"Pattern file not found: {slrun}", file=sys.stderr)
        return 1
    if not csvp.exists():
        print(f"CSV file not found: {csvp}", file=sys.stderr)
        return 1

    try:
        written = extract_from_patterns(
            slrun,
            csvp,
            out_dir=outd,
            tz=args.tz,
            numbered=bool(args.numbered),
            prefix=str(args.prefix),
            pad=int(args.pad),
            filename_template=str(args.filename_template),
            force=bool(args.force),
            continue_on_error=bool(args.continue_on_error),
        )
    except Exception as exc:
        print(f"Failed to extract windows: {exc}", file=sys.stderr)
        return 2

    print(f"Wrote {len(written)} window files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
