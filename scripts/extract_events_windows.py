#!/usr/bin/env python3
"""
Extract 1-min candles from a source CSV using windows listed in an
events CSV (rows are epoch seconds). Writes one CSV per window in a
separate folder named by the window's start timestamp.

Example:
  PYTHONPATH=$(pwd)/src python scripts/extract_events_windows.py \
    --candles gc_1min_vwap.csv \
    --events second_chance_smb_events.csv \
    --out /home/dribble/Development/tvdata/SecondChance
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from tvparser import core, io

DEFAULT_DIR = Path("/home/dribble/Development/tvdata")
DEFAULT_CANDLES = Path(f"{DEFAULT_DIR}/gc_1min_vwap.csv")
DEFAULT_EVENTS = Path(f"{DEFAULT_DIR}/second_chance_smb_events.csv")
DEFAULT_OUT = Path(f"{DEFAULT_DIR}/SecondChance")


def _read_events(path: Path) -> List[Dict[str, int]]:
    """Read events CSV with header start,entry,exit,end (epoch seconds)."""
    events: List[Dict[str, int]] = []
    with path.open() as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            try:
                start = int(row["start"])
                entry = int(row.get("entry", 0) or 0)
                exit_ = int(row.get("exit", 0) or 0)
                end = int(row.get("end", 0) or 0)
            except Exception as exc:
                raise ValueError(f"invalid row in {path}: {row!r}") from exc
            events.append({"start": start, "entry": entry, "exit": exit_, "end": end})
    return events


def _ensure_time_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has canonical 'time' column as integer seconds.

    tvparser.core.normalize expects columns like 'time'/'open' etc.
    """
    # quick heuristic: accept 'ts' or 'time'
    if "time" not in df.columns and "ts" in df.columns:
        df = df.rename(columns={"ts": "time"})
    # normalize with tvparser to coerce dtypes and canonicalize names
    try:
        df = core.normalize(df, drop_incomplete=False)
    except Exception:
        # fallback: ensure time as int and keep other columns
        df["time"] = df["time"].astype(int)
    return df


def _write_window_rows(path: Path, df: pd.DataFrame) -> None:
    """Write DataFrame rows to CSV using tvparser.io.write_csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # use keyword args to avoid signature ambiguity
    io.write_csv(df, path)


def _extract_in_memory(
    candles_path: Path,
    events: List[Dict[str, int]],
    out_dir: Path,
) -> List[Path]:
    """Load full candle CSV and write one file per event."""
    df = io.read_csv(candles_path)
    df = _ensure_time_col(df)
    df["time"] = df["time"].astype(int)

    written: List[Path] = []
    for ev in events:
        s = ev["start"]
        e = ev["end"]
        sel = df[(df["time"] >= s) & (df["time"] <= e)]
        folder = out_dir
        fname = f"{s}.csv"
        outp = folder / fname
        _write_window_rows(outp, sel)
        written.append(outp)
    return written


def _extract_chunked(
    candles_path: Path,
    events: List[Dict[str, int]],
    out_dir: Path,
    chunksize: int,
) -> List[Path]:
    """
    Stream the candle CSV and append matching rows to files for each
    event. This keeps memory usage bounded.
    """
    # prepare per-event files and header flags
    targets: Dict[Tuple[int, int], Path] = {}
    headers_written: Dict[Tuple[int, int], bool] = {}

    for ev in events:
        key = (ev["start"], ev["end"])
        folder = out_dir / str(ev["start"])
        folder.mkdir(parents=True, exist_ok=True)
        outp = folder / f"{ev['start']}_{ev['end']}.csv"
        targets[key] = outp
        headers_written[key] = False

    # stream read
    usecols = None  # read all columns; could optimize to a subset
    for chunk in pd.read_csv(candles_path, chunksize=chunksize):
        # canonicalize time column in chunk
        if "time" not in chunk.columns and "ts" in chunk.columns:
            chunk = chunk.rename(columns={"ts": "time"})
        chunk["time"] = chunk["time"].astype(int)
        for key, outp in targets.items():
            s, e = key
            sel = chunk[(chunk["time"] >= s) & (chunk["time"] <= e)]
            if sel.empty:
                continue
            # append to file; write header only once
            sel.to_csv(
                outp,
                mode="a",
                header=not headers_written[key],
                index=False,
            )
            headers_written[key] = True

    # return list of files created (may be empty if no matches)
    return [p for p in targets.values() if p.exists()]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="extract_events_windows")
    p.add_argument(
        "--candles",
        default=str(DEFAULT_CANDLES),
        help="path to 1-min candle CSV (ts in seconds).",
    )
    p.add_argument(
        "--events",
        default=str(DEFAULT_EVENTS),
        help="CSV listing windows with start,entry,exit,end (seconds).",
    )
    p.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help="base output directory to hold per-window folders.",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=0,
        help="If >0, read candles in chunks of this many rows.",
    )
    args = p.parse_args(argv)

    candles = Path(args.candles)
    events_p = Path(args.events)
    out_dir = Path(args.out)

    if not candles.exists():
        print("candles file not found:", candles, file=sys.stderr)
        return 2
    if not events_p.exists():
        print("events file not found:", events_p, file=sys.stderr)
        return 2

    events = _read_events(events_p)
    if not events:
        print("no events found in", events_p, file=sys.stderr)
        return 0

    if args.chunksize and args.chunksize > 0:
        written = _extract_chunked(candles, events, out_dir, args.chunksize)
    else:
        written = _extract_in_memory(candles, events, out_dir)

    print(f"Wrote {len(written)} files to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
