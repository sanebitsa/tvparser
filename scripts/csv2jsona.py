#!/usr/bin/env python3
# scripts/csvs_to_json_array.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from tvparser import csv2json


def convert_dir(
    directory: Path,
    camel_case: bool = True,
    generate_dts: bool = False,
    interface_name_fmt: Optional[str] = None,
) -> int:
    directory = directory.expanduser().resolve()
    if not directory.is_dir():
        print("not a directory:", directory)
        return 2

    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        print("no CSV files in", directory)
        return 0

    for csvf in csv_files:
        out = csvf.with_suffix(".json")
        iface = (
            interface_name_fmt.format(stem=csvf.stem)
            if interface_name_fmt
            else f"{csvf.stem}_Row"
        )
        try:
            csv2json.csv_to_json_array(
                input_path=csvf,
                output_path=out,
                camel_case=bool(camel_case),
                generate_dts=bool(generate_dts),
                interface_name=iface,
            )
            print("wrote", out)
        except Exception as exc:
            print(f"failed {csvf}: {exc}")
            return 3
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="csvs-to-json-array")
    p.add_argument(
        "dir",
        nargs="?",
        default="SecondChance",
        help="directory with CSV files (default: SecondChance)",
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
        help="format string for TypeScript interface name, " "use '{stem}' token",
    )
    args = p.parse_args(argv)
    return convert_dir(
        Path(args.dir),
        camel_case=args.camel,
        generate_dts=args.generate_dts,
        interface_name_fmt=args.iface_fmt,
    )


if __name__ == "__main__":
    raise SystemExit(main())
