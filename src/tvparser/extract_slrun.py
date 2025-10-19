from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from . import slicer, timeutils


@dataclass(frozen=True)
class SLWindow:
    """Minimal window representing an SLrun entry."""
    date_str: str
    start: str
    end: str
    start_ts: int
    end_ts: int


def parse_slrun_lines(lines: Iterable[str], tz: str = "UTC") -> List[SLWindow]:
    """
    Parse lines from an SLrunLong.txt-like file.

    Header expected: date, start, entry, exit, end
    We only keep date, start and end (plus computed timestamps).
    """
    out: List[SLWindow] = []
    first = True
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if first:
            first = False
            if "date" in line.lower():
                continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            raise ValueError(f"malformed slrun line: {line!r}")
        date_str, start, _entry, _exit, end = parts[:5]
        start_ts, end_ts = timeutils.window_start_end(
            date_str, start, end, tz=tz, to_ms=False
        )
        out.append(
            SLWindow(
                date_str=date_str,
                start=start,
                end=end,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )
    return out


def format_window_filename(csv_path: Path, w: SLWindow) -> str:
    """
    Format a safe filename for the window.

    Example: gc_1min_2024-10-13_17-00__07-00.csv
    """
    m, d, y = [int(x) for x in w.date_str.split("/")]
    year = 2000 + y if y < 100 else y
    date_iso = f"{year:04d}-{m:02d}-{d:02d}"
    start_s = w.start.replace(":", "-")
    end_s = w.end.replace(":", "-")
    stem = csv_path.stem
    return f"{stem}_{date_iso}_{start_s}__{end_s}.csv"


def extract_from_slrun(
    slrun_path: Path,
    csv_path: Path,
    out_dir: Optional[Path] = None,
    tz: str = "UTC",
    force: bool = False,
    continue_on_error: bool = False,
) -> List[Path]:
    """
    Extract windows defined in slrun_path from csv_path.

    Returns list of output file paths created (or overwritten if force).
    """
    if out_dir is None:
        out_dir = csv_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    content = slrun_path.read_text(encoding="utf-8").splitlines()
    windows = parse_slrun_lines(content, tz=tz)
    written: List[Path] = []

    for w in windows:
        fname = format_window_filename(csv_path, w)
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
    p = argparse.ArgumentParser(prog="tvparser.extract_slrun")
    p.add_argument("--slrun", required=True,
                   help="Path to SLrunLong.txt")
    p.add_argument("--csv", required=True,
                   help="Path to 1-min CSV to slice")
    p.add_argument("--out-dir", default=None,
                   help="Directory to write per-window CSVs")
    p.add_argument("--tz", default="UTC",
                   help="Timezone name for parsing dates (e.g. UTC)")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing outputs")
    p.add_argument("--continue-on-error", action="store_true",
                   help="Continue to next window if an error occurs")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    p = _build_parser()
    args = p.parse_args(argv)

    slrun = Path(args.slrun)
    csvp = Path(args.csv)
    outd = Path(args.out_dir) if args.out_dir else None

    if not slrun.exists():
        print(f"SLrun file not found: {slrun}", file=sys.stderr)
        return 1
    if not csvp.exists():
        print(f"CSV file not found: {csvp}", file=sys.stderr)
        return 1

    try:
        written = extract_from_slrun(
            slrun,
            csvp,
            out_dir=outd,
            tz=args.tz,
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
