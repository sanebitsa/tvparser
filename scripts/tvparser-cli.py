#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import json
import pandas as pd
import sys
import re
from pathlib import Path

from tvparser import extract_patterns, csv2json

# Base directory for default inputs/outputs
BASE_DIR = Path("/home/dribble/Development/tvdata")

# Defaults derived from BASE_DIR
DEFAULT_CSV = BASE_DIR / "merged_1min.csv"
DEFAULT_PATTERN = BASE_DIR / "range_breakout.txt"
DEFAULT_OUTDIR = BASE_DIR / "RangeBreakout"
DEFAULT_JSON_DIR = DEFAULT_OUTDIR

# Requested defaults
DEFAULT_TZ = "America/Chicago"
DEFAULT_NUMBERED = True
DEFAULT_PAD = 3


def _to_output_dir_name(raw: str) -> str:
    parts = re.split(r"[^0-9A-Za-z]+", raw)
    parts = [p.title() for p in parts if p]
    return "".join(parts) or raw


def _resolve_csv_path(arg: str | None) -> Path:
    if arg is None:
        return DEFAULT_CSV
    p = Path(arg)
    if arg.find(os.sep) != -1 or p.suffix == ".csv":
        return p
    return BASE_DIR / f"{arg}.csv"


def _resolve_pattern_path(arg: str | None) -> Path:
    if arg is None:
        return DEFAULT_PATTERN
    p = Path(arg)
    if arg.find(os.sep) != -1 or p.suffix == ".txt":
        return p
    return BASE_DIR / f"{arg}.txt"


def _resolve_outdir(arg: str | None, pattern_basename: str | None) -> Path:
    if arg:
        return Path(arg)
    if pattern_basename:
        name = _to_output_dir_name(pattern_basename)
        return BASE_DIR / name
    return DEFAULT_OUTDIR


def cmd_extract(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="tvparser extract")
    p.add_argument(
        "csv_name",
        nargs="?",
        help="CSV basename or path (default uses base dir)",
    )
    p.add_argument(
        "pattern_name",
        nargs="?",
        help="pattern basename or path (default uses base dir)",
    )
    p.add_argument(
        "out_dir",
        nargs="?",
        help="output dir (default derived from pattern in base dir)",
    )
    p.add_argument("--tz", default=DEFAULT_TZ, help=f"Timezone (default: {DEFAULT_TZ})")
    p.add_argument(
        "--numbered",
        dest="numbered",
        action="store_true",
        default=DEFAULT_NUMBERED,
        help="Write outputs as prefixNNN.csv (default: on)",
    )
    p.add_argument(
        "--no-numbered",
        dest="numbered",
        action="store_false",
        help="Disable numbered output and use descriptive filenames",
    )
    p.add_argument(
        "--prefix", default="slrun_long", help="Prefix when --numbered is set"
    )
    p.add_argument(
        "--pad",
        type=int,
        default=DEFAULT_PAD,
        help=f"Zero-pad width when --numbered (default {DEFAULT_PAD})",
    )
    p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue on individual extraction errors",
    )
    args = p.parse_args(argv)

    csvp = _resolve_csv_path(args.csv_name)
    patp = _resolve_pattern_path(args.pattern_name)

    pattern_basename = None
    if args.pattern_name:
        pattern_basename = Path(args.pattern_name).stem

    outd = _resolve_outdir(args.out_dir, pattern_basename)

    if not patp.exists():
        print("pattern file not found:", patp, file=sys.stderr)
        return 2
    if not csvp.exists():
        print("csv file not found:", csvp, file=sys.stderr)
        return 2

    outd.mkdir(parents=True, exist_ok=True)

    prefix_value = args.prefix
    if prefix_value == "slrun_long" and pattern_basename:
        prefix_value = pattern_basename

    try:
        written = extract_patterns.extract_from_patterns(
            patp,
            csvp,
            out_dir=outd,
            tz=args.tz,
            numbered=bool(args.numbered),
            prefix=str(prefix_value),
            pad=int(args.pad),
            filename_template="{stem}_{date}_{start}__{end}.csv",
            force=bool(args.force),
            continue_on_error=bool(args.continue_on_error),
        )
    except Exception as exc:
        print("extraction failed:", exc, file=sys.stderr)
        return 3

    print(f"Wrote {len(written)} files to {outd}")
    return 0


def cmd_json(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="tvparser json")
    p.add_argument(
        "path",
        nargs="?",
        default=str(DEFAULT_JSON_DIR),
        help=f"CSV file or directory (default: {DEFAULT_JSON_DIR})",
    )
    p.add_argument(
        "--out-dir",
        default=None,
        help="Optional output directory (defaults to same dir)",
    )
    p.add_argument(
        "--no-camel",
        dest="camel",
        action="store_false",
        help="Do not convert keys to camelCase",
    )
    p.add_argument(
        "--generate-dts",
        action="store_true",
        help="Generate .d.ts files alongside JSON",
    )
    args = p.parse_args(argv)

    arg = args.path
    pth = Path(arg)
    if arg and arg.find(os.sep) == -1 and not pth.suffix:
        pth = BASE_DIR / arg
        if (pth.with_suffix(".csv")).exists():
            pth = pth.with_suffix(".csv")
        elif pth.is_dir():
            pass

    out_base = Path(args.out_dir) if args.out_dir else None

    if pth.is_dir():
        targets = sorted(pth.glob("*.csv"))
        if not targets:
            print("no CSVs found in", pth, file=sys.stderr)
            return 0
    elif pth.is_file():
        targets = [pth]
    else:
        print("path not found:", pth, file=sys.stderr)
        return 2

    for csvf in targets:
        outdir = out_base if out_base else csvf.parent
        outdir.mkdir(parents=True, exist_ok=True)
        out = outdir / csvf.with_suffix(".json").name
        iface = f"{csvf.stem}_Row"
        try:
            csv2json.csv_to_json_array(
                input_path=csvf,
                output_path=out,
                camel_case=bool(args.camel),
                generate_dts=bool(args.generate_dts),
                interface_name=iface,
            )
            print("wrote", out)
        except Exception as exc:
            print(f"failed to convert {csvf}: {exc}", file=sys.stderr)
            return 4

    return 0


def cmd_extract_and_json(argv: list[str]) -> int:
    """
    Combined command: extract windows, then convert resulting CSVs to
    single-array JSON files.
    """
    p = argparse.ArgumentParser(prog="tvparser extract-json")
    p.add_argument(
        "csv_name",
        nargs="?",
        help="CSV basename or path (default uses base dir)",
    )
    p.add_argument(
        "pattern_name",
        nargs="?",
        help="pattern basename or path (default uses base dir)",
    )
    p.add_argument(
        "out_dir",
        nargs="?",
        help="output dir for extracted CSVs (derived from pattern by default)",
    )
    p.add_argument("--tz", default=DEFAULT_TZ, help=f"Timezone (default: {DEFAULT_TZ})")
    p.add_argument(
        "--numbered",
        dest="numbered",
        action="store_true",
        default=DEFAULT_NUMBERED,
        help="Write outputs as prefixNNN.csv (default: on)",
    )
    p.add_argument(
        "--no-numbered",
        dest="numbered",
        action="store_false",
        help="Disable numbered output",
    )
    p.add_argument(
        "--prefix", default="slrun_long", help="Prefix when --numbered is set"
    )
    p.add_argument(
        "--pad", type=int, default=DEFAULT_PAD, help="Zero-pad width when --numbered"
    )
    p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue on individual extraction errors",
    )
    # JSON conversion options
    p.add_argument(
        "--no-camel",
        dest="camel",
        action="store_false",
        help="Do not convert keys to camelCase in JSON",
    )
    p.add_argument(
        "--generate-dts",
        action="store_true",
        help="Generate .d.ts files alongside JSON",
    )
    p.add_argument(
        "--out-dir-json", default=None, help="Optional directory to write JSON outputs"
    )
    args = p.parse_args(argv)

    csvp = _resolve_csv_path(args.csv_name)
    patp = _resolve_pattern_path(args.pattern_name)

    pattern_basename = None
    if args.pattern_name:
        pattern_basename = Path(args.pattern_name).stem

    outd = _resolve_outdir(args.out_dir, pattern_basename)

    if not patp.exists():
        print("pattern file not found:", patp, file=sys.stderr)
        return 2
    if not csvp.exists():
        print("csv file not found:", csvp, file=sys.stderr)
        return 2

    outd.mkdir(parents=True, exist_ok=True)

    prefix_value = args.prefix
    if prefix_value == "slrun_long" and pattern_basename:
        prefix_value = pattern_basename

    try:
        written = extract_patterns.extract_from_patterns(
            patp,
            csvp,
            out_dir=outd,
            tz=args.tz,
            numbered=bool(args.numbered),
            prefix=str(prefix_value),
            pad=int(args.pad),
            filename_template="{stem}_{date}_{start}__{end}.csv",
            force=bool(args.force),
            continue_on_error=bool(args.continue_on_error),
        )
    except Exception as exc:
        print("extraction failed:", exc, file=sys.stderr)
        return 3

    print(f"Wrote {len(written)} files to {outd}")

    # Convert CSVs in outd to JSON arrays. Use out-dir-json if provided.
    json_out_base = Path(args.out_dir_json) if args.out_dir_json else None

    # Convert all CSV files in outd (this includes files that may have
    # existed before). If you want only newly written files, use 'written'.
    to_convert = written if written else sorted(outd.glob("*.csv"))

    for csvf in to_convert:
        outdir = json_out_base if json_out_base else csvf.parent
        outdir.mkdir(parents=True, exist_ok=True)
        out = outdir / csvf.with_suffix(".json").name
        iface = f"{csvf.stem}_Row"
        try:
            csv2json.csv_to_json_array(
                input_path=csvf,
                output_path=out,
                camel_case=bool(args.camel),
                generate_dts=bool(args.generate_dts),
                interface_name=iface,
            )
            print("wrote", out)
        except Exception as exc:
            print(f"failed to convert {csvf}: {exc}", file=sys.stderr)
            if not args.continue_on_error:
                return 5
            # else continue converting next file

    return 0


def _resolve_json_path(arg: str | None) -> Path:
    """Resolve json file or directory relative to BASE_DIR."""
    if arg is None:
        return BASE_DIR / "range_breakout.timestamps.json"
    p = Path(arg)
    if arg.find(os.sep) != -1 or p.suffix == ".json":
        return p
    candidate = BASE_DIR / arg
    if candidate.with_suffix(".json").exists():
        return candidate.with_suffix(".json")
    if candidate.is_dir():
        return candidate
    return p


def cmd_build_from_json(argv: list[str]) -> int:
    """
    Build CSV files from JSON timestamp arrays using tvparser.slicer.
    If the json argument is a simple basename (no path / no suffix),
    create BASE_DIR/PascalCase(basename) and write outputs there.
    """
    p = argparse.ArgumentParser(prog="tvparser build")
    p.add_argument(
        "json_path",
        nargs="?",
        help=(
            "JSON file (array) or directory of JSON files, or a basename "
            "in base dir (e.g. 'range_breakout')."
        ),
    )
    p.add_argument(
        "out_dir",
        nargs="?",
        help="Optional explicit output directory for built CSVs.",
    )
    p.add_argument(
        "--csv",
        dest="csv",
        default=str(DEFAULT_CSV),
        help=f"Merged CSV path (default: {DEFAULT_CSV})",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files",
    )
    args = p.parse_args(argv)

    # require slicer module and function (fail fast if missing)
    try:
        import tvparser.slicer as slicer_mod  # type: ignore
    except Exception as exc:
        print(
            "error: tvparser.slicer is required but not importable:",
            exc,
            file=sys.stderr,
        )
        return 2

    if not hasattr(slicer_mod, "slice_csv_window"):
        print(
            "error: tvparser.slicer.slice_csv_window is required " "but not found.",
            file=sys.stderr,
        )
        return 2

    raw_arg = args.json_path
    json_resolved = _resolve_json_path(raw_arg)

    # determine list of json files to process
    if json_resolved.is_dir():
        json_files = sorted(json_resolved.glob("*.json"))
    elif json_resolved.is_file():
        json_files = [json_resolved]
    else:
        print("json path not found:", json_resolved, file=sys.stderr)
        return 2

    if not json_files:
        print("no JSON files to process at", json_resolved, file=sys.stderr)
        return 0

    csv_path = _resolve_csv_path(args.csv)
    if not csv_path.exists():
        print("merged CSV not found:", csv_path, file=sys.stderr)
        return 2

    # Decide output base dir:
    # 1) explicit -- out_dir
    # 2) if user passed a bare basename (no sep, no suffix) -> PascalCase under BASE_DIR
    # 3) json_resolved is a file -> same parent
    # 4) json_resolved is a dir -> that dir
    # 5) otherwise fallback to BASE_DIR/SlrunBuilt (unlikely)
    if args.out_dir:
        out_base = Path(args.out_dir)
    elif raw_arg and raw_arg.find(os.sep) == -1 and not Path(raw_arg).suffix:
        # user provided a bare name such as "range_breakout"
        out_base = BASE_DIR / _to_output_dir_name(raw_arg)
    elif json_resolved.is_file():
        out_base = json_resolved.parent
    elif json_resolved.is_dir():
        out_base = json_resolved
    else:
        out_base = BASE_DIR / "SlrunBuilt"

    out_base.mkdir(parents=True, exist_ok=True)

    written = []
    for jf in json_files:
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"failed to load {jf}: {exc}", file=sys.stderr)
            continue

        if not isinstance(data, list):
            print(f"skipping {jf}: expected JSON array", file=sys.stderr)
            continue

        for rec in data:
            if not isinstance(rec, dict):
                print(f"skipping record (not object) in {jf}: {rec}", file=sys.stderr)
                continue
            try:
                start = int(rec["start"])
                end = int(rec["end"])
            except Exception as exc:
                print(f"bad record in {jf}: {rec} ({exc})", file=sys.stderr)
                continue

            out_name = f"{start}.csv"
            outp = out_base / out_name

            if outp.exists() and not args.force:
                # skip existing file
                continue

            try:
                # Use the library slicer directly — must exist per policy
                # expected signature:
                # slice_csv_window(csv_path, start, end, out_path, force=False)
                slicer_mod.slice_csv_window(csv_path, start, end, outp)
                written.append(outp)
            except Exception as exc:
                print(f"failed to create {outp}: {exc}", file=sys.stderr)
                # continue with other records
                continue

    print(f"Wrote {len(written)} CSV files to {out_base}")
    return 0


def main() -> int:
    top = argparse.ArgumentParser(prog="tvparser-cli")
    subs = top.add_subparsers(dest="cmd", required=True)
    subs.add_parser("extract", help="Extract windows from merged CSV")
    subs.add_parser("json", help="Convert CSV(s) to single-array JSON")
    subs.add_parser(
        "extract-json",
        help="Extract windows then convert produced CSVs to .json",
    )
    subs.add_parser("build", help="Build CSVs from JSON timestamp arrays")

    if len(sys.argv) < 2:
        top.print_help()
        return 1

    cmd = sys.argv[1]
    argv = sys.argv[2:]

    if cmd == "extract":
        return cmd_extract(argv)
    if cmd == "json":
        return cmd_json(argv)
    if cmd == "extract-json":
        return cmd_extract_and_json(argv)
    if cmd == "build":
        return cmd_build_from_json(argv)

    print("unknown command:", cmd, file=sys.stderr)
    top.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
