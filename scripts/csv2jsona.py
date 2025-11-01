#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from tvparser import csv2json


def convert_file(
    csv_path: Path,
    out_dir: Optional[Path],
    camel_case: bool,
    generate_dts: bool,
    iface_fmt: Optional[str],
    force: bool,
) -> int:
    """Convert one CSV to a single-array JSON file.

    Returns 0 on success, 1 on failure.
    """
    csv_path = csv_path.expanduser().resolve()
    if not csv_path.is_file():
        print("not a file:", csv_path)
        return 1

    dest_dir = out_dir if out_dir else csv_path.parent
    dest_dir.mkdir(parents=True, exist_ok=True)

    out_path = dest_dir / csv_path.with_suffix(".json").name
    if out_path.exists() and not force:
        print("skip (exists):", out_path)
        return 0

    iface = (
        iface_fmt.format(stem=csv_path.stem) if iface_fmt else f"{csv_path.stem}_Row"
    )

    try:
        csv2json.csv_to_json_array(
            input_path=csv_path,
            output_path=out_path,
            camel_case=bool(camel_case),
            generate_dts=bool(generate_dts),
            interface_name=iface,
        )
        print("wrote", out_path)
        return 0
    except Exception as exc:
        print(f"failed {csv_path}: {exc}")
        return 1


def convert_path(
    path: Path,
    out_dir: Optional[Path],
    camel_case: bool,
    generate_dts: bool,
    iface_fmt: Optional[str],
    force: bool,
) -> int:
    """Convert a path which may be a file or a directory.

    If directory: convert all CSVs inside it.
    """
    p = path.expanduser().resolve()
    if p.is_dir():
        csvs = sorted(p.glob("*.csv"))
        if not csvs:
            print("no CSVs in", p)
            return 0
        rc_sum = 0
        for csvf in csvs:
            rc = convert_file(csvf, out_dir, camel_case, generate_dts, iface_fmt, force)
            rc_sum |= rc
        return rc_sum
    else:
        return convert_file(p, out_dir, camel_case, generate_dts, iface_fmt, force)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="csvs-to-json-array")
    p.add_argument(
        "paths",
        nargs="*",
        default=["SecondChance"],
        help="file(s) or dir(s) to convert (default: SecondChance)",
    )
    p.add_argument(
        "--out-dir",
        default=None,
        help="write JSONs into this directory (default: same dir)",
    )
    p.add_argument(
        "--no-camel",
        dest="camel",
        action="store_false",
        help="do not convert keys to camelCase",
    )
    p.add_argument(
        "--generate-dts",
        action="store_true",
        help="emit .d.ts interface files alongside JSON",
    )
    p.add_argument(
        "--iface-fmt",
        default=None,
        help="format string for interface name (use '{stem}')",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing JSON files",
    )
    args = p.parse_args(argv)

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    rc_agg = 0
    for raw in args.paths:
        path = Path(raw)
        rc = convert_path(
            path,
            out_dir,
            camel_case=args.camel,
            generate_dts=args.generate_dts,
            iface_fmt=args.iface_fmt,
            force=bool(args.force),
        )
        rc_agg |= rc

    return rc_agg


if __name__ == "__main__":
    raise SystemExit(main())
