#!/usr/bin/env python3
"""
Merge many TradingView CSVs into one normalized, deduped, sorted CSV.

Usage examples:
  # merge all CSVs in a directory
  python scripts/merge_csvs.py --dir /path/to/csvs \
    --output /path/to/merged_1min.csv

  # merge explicit files
  python scripts/merge_csvs.py a.csv b.csv c.csv \
    --output merged.csv --dedupe max_volume

  # preview first 5 timestamps without writing
  python scripts/merge_csvs.py --dir /path/to/csvs \
    --preview 5
"""

from __future__ import annotations

import argparse
import gzip
import logging
import sys
from pathlib import Path
from typing import List, Optional

from tvparser import core, io

LOGGER = logging.getLogger("tvparser.merge_csvs")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def _gather_inputs(dir_path: Optional[Path], files: List[str]) -> List[Path]:
    """Return list of csv Path objects from args."""
    results: List[Path] = []
    if dir_path:
        if not dir_path.exists():
            raise FileNotFoundError(f"directory not found: {dir_path}")
        results = sorted(dir_path.glob("*.csv"))
    else:
        results = [Path(p) for p in files if p]
    return results


def _maybe_open_out(path: Path, gz: bool):
    """Return a file-like object for writing CSV output."""
    if gz:
        return gzip.open(path, "wt", encoding="utf-8")
    return path.open("w", encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="merge_csvs")
    p.add_argument(
        "files",
        nargs="*",
        help="Explicit CSV files to merge (optional if --dir used).",
    )
    p.add_argument(
        "--dir",
        dest="dir_path",
        help="Directory containing CSVs to merge (glob *.csv).",
    )
    p.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output merged CSV path.",
    )
    p.add_argument(
        "--dedupe",
        default="last",
        choices=("last", "first", "max_volume"),
        help="Deduplication strategy (default: last).",
    )
    p.add_argument(
        "--drop-incomplete",
        dest="drop_incomplete",
        action="store_true",
        default=True,
        help="Drop rows with missing OHLCV (default: true).",
    )
    p.add_argument(
        "--no-drop-incomplete",
        dest="drop_incomplete",
        action="store_false",
        help="Do not drop incomplete rows.",
    )
    p.add_argument(
        "--sort",
        dest="sort_order",
        choices=("asc", "desc"),
        default="asc",
        help="Sort order for output by time (asc|desc).",
    )
    p.add_argument(
        "--preview",
        type=int,
        default=0,
        help="Print first N timestamps and exit (no write).",
    )
    p.add_argument(
        "--gz",
        action="store_true",
        help="Write output as gzipped CSV (.gz).",
    )
    args = p.parse_args(argv)

    # resolve input list
    dir_path = Path(args.dir_path) if args.dir_path else None
    try:
        inputs = _gather_inputs(dir_path, args.files)
    except Exception as exc:
        print("error:", exc, file=sys.stderr)
        return 2

    if not inputs:
        print("no input CSVs found; provide files or --dir", file=sys.stderr)
        return 2

    # read CSVs using tvparser.io
    frames = []
    for fp in inputs:
        if not fp.exists():
            LOGGER.warning("skipping missing file: %s", fp)
            continue
        try:
            df = io.read_csv(fp)
            frames.append(df)
            LOGGER.info("read %s -> %d rows", fp.name, len(df))
        except Exception as exc:
            LOGGER.error("failed to read %s: %s", fp, exc)
            return 3

    if not frames:
        print("no readable CSVs found", file=sys.stderr)
        return 2

    # merge using tvparser.core
    try:
        merged = core.merge_frames(
            frames,
            dedupe_strategy=args.dedupe,
            drop_incomplete=bool(args.drop_incomplete),
            sort_order=args.sort_order,
        )
    except Exception as exc:
        LOGGER.error("merge failed: %s", exc)
        return 4

    # preview mode: print first N timestamps and exit
    if args.preview:
        if "ts" in merged.columns:
            col = "ts"
        else:
            col = "time"
        try:
            vals = list(merged[col].astype(int).head(args.preview))
            for v in vals:
                print(v)
            return 0
        except Exception as exc:
            LOGGER.error("preview failed: %s", exc)
            return 6

    # write output
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    gz = bool(args.gz) or outp.suffix == ".gz"
    try:
        # io.write_csv is the canonical writer in tvparser; use it
        if hasattr(io, "write_csv"):
            # prefer the library writer when available
            if gz:
                # write to a temp file then gzip to satisfy write_csv
                tmp = outp.with_suffix(outp.suffix + ".tmp")
                io.write_csv(merged, tmp)
                # gzip the temp file content to final path
                with tmp.open("rb") as fh_in, gzip.open(outp, "wb") as fh_out:
                    fh_out.writelines(fh_in)
                tmp.unlink(missing_ok=True)
            else:
                io.write_csv(merged, outp)
        else:
            # fallback: basic pandas dump (shouldn't be needed)
            with _maybe_open_out(outp, gz) as fh:
                merged.to_csv(fh, index=False)
    except Exception as exc:
        LOGGER.error("failed to write output %s: %s", outp, exc)
        return 5

    # print summary
    try:
        summary = core.summarize(merged)
    except Exception:
        summary = {
            "rows": int(len(merged)),
            "start_time": int(merged["time"].min()),
            "end_time": int(merged["time"].max()),
        }

    LOGGER.info("wrote %s (%d rows)", outp, summary.get("rows", len(merged)))
    LOGGER.info(
        "start=%s end=%s",
        summary.get("start_time"),
        summary.get("end_time"),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
