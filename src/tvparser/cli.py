from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING, List, Optional

from . import core, io  # modules are importable and patchable in tests

if TYPE_CHECKING:
    from pathlib import Path

LOGGER = logging.getLogger("tvparser.cli")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tvparser")
    p.add_argument(
        "-i",
        "--input",
        action="append",
        metavar="PATH",
        help="Input CSV file(s). Can be repeated.",
    )
    p.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing input CSV files (reads *.csv).",
    )
    p.add_argument(
        "-o",
        "--output",
        metavar="PATH",
        help="Output file path (CSV). Required unless --dry-run is used.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write output; print a summary of the merged data.",
    )
    p.add_argument(
        "--dedupe",
        choices=("last", "first", "max_volume"),
        default="last",
        help="Deduplication strategy.",
    )
    p.add_argument(
        "--drop-incomplete",
        dest="drop_incomplete",
        action="store_true",
        help="Drop rows with missing fields during normalization.",
    )
    p.add_argument(
        "--no-drop-incomplete",
        dest="drop_incomplete",
        action="store_false",
        help="Keep rows with incomplete fields (do not drop).",
    )
    p.set_defaults(drop_incomplete=True)
    p.add_argument(
        "--sort-order",
        choices=("asc", "desc"),
        default="asc",
        help="Sort order for final output by timestamp.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v, -vv).",
    )
    return p


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def _gather_inputs(
    input_args: Optional[List[str]], input_dir: Optional[str]
) -> List["Path"]:
    from pathlib import Path

    paths: List[Path] = []
    if input_args:
        for p in input_args:
            paths.append(Path(p))
        return paths

    if input_dir:
        d = Path(input_dir)
        if not d.is_dir():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")
        paths = sorted(d.glob("*.csv"))
    return paths


def _expand_inputs(inputs: List["Path"]) -> List["Path"]:
    from pathlib import Path

    expanded: List[Path] = []
    for p in inputs:
        if isinstance(p, Path) and p.is_dir():
            expanded.extend(sorted(p.glob("*.csv")))
            continue
        expanded.append(Path(p))
    return expanded


def _merge_inputs(
    inputs: List["Path"], dedupe: str, drop_incomplete: bool, sort_order: str
):
    return core.merge_frames(
        inputs,
        dedupe_strategy=dedupe,
        drop_incomplete=drop_incomplete,
        sort_order=sort_order,
    )


def _print_summary(merged) -> None:
    try:
        summary = core.summarize(merged)
    except Exception:
        summary = {"rows": getattr(merged, "shape", (0,))[0]}
    print("Summary:")
    if isinstance(summary, dict):
        for k, v in summary.items():
            print(f"  {k}: {v}")
    else:
        print(summary)


def _write_output(merged, out_path: str) -> None:
    # create parent dir if missing
    from pathlib import Path as _P

    op = _P(out_path)
    if op.parent and not op.parent.exists():
        op.parent.mkdir(parents=True, exist_ok=True)
    io.write_csv(merged, out_path)


def main(argv: Optional[List[str]] = None) -> int:
    """
    Entrypoint. Return codes:
      0 success
      1 user/config error
      2 runtime/internal error
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)
    LOGGER.debug("parsed args: %s", args)

    try:
        inputs = _gather_inputs(args.input, args.input_dir)
    except FileNotFoundError as exc:
        print(f"Input error: {exc}", file=sys.stderr)
        return 1

    if not inputs:
        print("No input files specified.", file=sys.stderr)
        return 1

    expanded = _expand_inputs(inputs)
    if not expanded:
        print("No CSV files discovered from inputs.", file=sys.stderr)
        return 1

    LOGGER.info("Discovered %d input file(s)", len(expanded))

    try:
        merged = _merge_inputs(
            expanded,
            dedupe=args.dedupe,
            drop_incomplete=bool(args.drop_incomplete),
            sort_order=args.sort_order,
        )
    except Exception as exc:
        LOGGER.exception("Failed to merge inputs")
        print(f"Failed to merge inputs: {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        _print_summary(merged)
        return 0

    if not args.output:
        print("No --output specified; provide --output or use --dry-run.",
              file=sys.stderr)
        return 1

    try:
        _write_output(merged, args.output)
    except Exception as exc:
        LOGGER.exception("Failed to write output")
        print(f"Failed to write output: {exc}", file=sys.stderr)
        return 2

    print(f"Wrote output to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
