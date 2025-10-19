# TVParser

`tvparser` — TradingView CSV tools for Python.
A small, well-tested toolkit for parsing, normalizing, merging, and
extracting 1-minute TradingView CSV exports, and for converting CSV
outputs to JSON/NDJSON + a simple TypeScript `.d.ts` interface.

This README documents the package modules, public functions, and
command-line usage examples you can copy-paste. It assumes the package
layout:

```
src/tvparser/
├─ cli.py
├─ core.py
├─ io.py
├─ csv2json.py
├─ slicer.py
├─ timeutils.py
└─ extract_slrun.py
```

---

# Table of contents

- [Quick install](#quick-install)
- [Quickstart examples](#quickstart-examples)
- [Modules & API reference](#modules--api-reference)

  - `cli` — merge CSVs (command-line)
  - `core` — normalize / dedupe / merge / summarize (DataFrame API)
  - `io` — read/write helpers (CSV / parquet / discovery)
  - `csv2json` — CSV → NDJSON / JSON array + `.d.ts` generator
  - `slicer` — slice CSV by timestamp window
  - `timeutils` — parse SLrun dates/times → unix timestamps (seconds)
  - `extract_slrun` — parse SLrunLong and extract windows (end-to-end)

- [Examples (command line & Python)](#examples-command-line--python)
- [Testing & linting](#testing--linting)
- [Troubleshooting / Notes](#troubleshooting--notes)
- [Contributing](#contributing)
- [License](#license)

---

# Quick install

Create a virtualenv and install project in editable/dev mode:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
```

`.[dev]` should include dev deps such as `pytest`, `pytest-cov`, `ruff`,
and optionally `pyarrow` / `fastparquet` if you plan to write/read
parquet in tests.

If you don't want to install, you can run using `PYTHONPATH=src`:

```bash
cd /path/to/project
export PYTHONPATH=$(pwd)/src
python -m tvparser.cli --help
```

---

# Quickstart examples

Merge a directory of 1-minute CSVs into a single CSV using the CLI:

```bash
# run from project root (use venv or PYTHONPATH)
python -m tvparser.cli \
  --input-dir /home/dribble/Development/tvdata/1min \
  --output /home/dribble/Development/tvdata/1min/merged_1min.csv
```

Create an NDJSON file (camelCase keys) and a `.d.ts` interface from the
merged CSV:

```bash
python - <<'PY'
from pathlib import Path
from tvparser import csv2json

in_path = Path("/home/dribble/Development/tvdata/1min/merged_1min.csv")
out_path = in_path.with_name("merged_1min.jsonl")
csv2json.csv_to_ndjson(
    input_path=in_path,
    output_path=out_path,
    camel_case=True,
    generate_dts=True,
    interface_name="MergedRow",
)
print("wrote", out_path)
PY
```

Extract windows defined in `SLrunLong.txt` from a 1-minute CSV:

```bash
python -m tvparser.extract_slrun \
  --slrun /path/to/SLrunLong.txt \
  --csv /path/to/gc_1min.csv \
  --out-dir /path/to/out \
  --tz UTC
```

---

# Modules & API reference

Below is a compact reference for each module and its primary functions,
with CLI examples and small Python snippets.

> Note: all timestamps used by these modules are **seconds** (Unix epoch
> seconds) unless an explicit `to_ms=True` argument is used.

---

## `tvparser.cli` — merge CSV files (command-line)

**Purpose**

Thin CLI that discovers input CSVs, delegates merging to `core.merge_frames`,
and writes the merged output via `io.write_csv`. Test-friendly and
monitored by `ruff`/`pytest`.

**Usage (CLI)**

```bash
python -m tvparser.cli --help
```

Arguments (high-level):

- `-i, --input PATH` — appendable argument; can repeat.
- `--input-dir DIR` — read `*.csv` files from this directory.
- `-o, --output PATH` — output file path (CSV). Required unless `--dry-run`.
- `--dry-run` — don't write, print summary.
- `--dedupe {last,first,max_volume}` — dedupe strategy (default `last`).
- `--drop-incomplete` / `--no-drop-incomplete` — drop rows missing fields.
- `--sort-order {asc,desc}` — sort final output by timestamp.
- `-v` / `-vv` — verbose logging levels.

**Example**

Merge directory into `merged_1min.csv`:

```bash
python -m tvparser.cli \
  --input-dir /data/1min \
  --output /data/1min/merged_1min.csv \
  --dedupe max_volume -v
```

**Python snippet (programmatic)**

```py
from tvparser import cli
# call from tests: provide argv list:
rc = cli.main(["--input", "a.csv", "--input", "b.csv", "--output", "out.csv"])
```

---

## `tvparser.core` — normalize / dedupe / merge / summarize

**Purpose**

Main processing functions manipulating pandas DataFrames:

- `normalize(df, drop_incomplete=True)` — canonicalize column names,
  coalesce duplicate-name columns (like `Volume` vs `volume`), convert
  numeric columns, coerce `time` to seconds, optionally drop incomplete rows.
- `deduplicate(df, strategy="last")` — dedupe rows that share the same
  `time` stamp. Strategies: `last` (default), `first`, `max_volume`.
- `merge_frames(inputs, dedupe_strategy="last", drop_incomplete=True, sort_order="asc")`
  — accept an iterable of DataFrames or file paths, normalize each,
  concatenate, deduplicate and sort.
- `summarize(df)` — return a small dict with `rows`, `start_time`, `end_time`, etc.

**Python example**

```py
from tvparser import core, io

# read two CSVs
a = io.read_csv("gc_part1.csv")
b = io.read_csv("gc_part2.csv")

merged = core.merge_frames([a, b], dedupe_strategy="max_volume",
                           drop_incomplete=True, sort_order="asc")
print(core.summarize(merged))
```

Return types: `pandas.DataFrame` for normalize/merge/deduplicate.

---

## `tvparser.io` — file I/O helpers

**Purpose**

Convenience wrappers around `pandas` I/O plus small helpers:

- `read_csv(path: str|Path) -> pd.DataFrame` — read csv and return DataFrame.
- `write_csv(df, path: str|Path)` — write CSV; ensures parent dir exists.
- `write_parquet(df, path: str|Path)` — write parquet (requires `pyarrow` or `fastparquet`).
- `discover_input_files(path_or_pattern)` — returns list of CSV paths matching directory or glob.

**Examples**

```py
from tvparser import io
df = io.read_csv("gc_1min.csv")
io.write_csv(df, "/tmp/out.csv")
io.write_parquet(df, "/tmp/out.parquet")  # needs pyarrow or fastparquet
```

Command-line usage (via scripts or python -c) mirrors these functions.

---

## `tvparser.csv2json` — convert CSV -> NDJSON / JSON array + `.d.ts`

**Purpose**

Streaming conversion of CSV to NDJSON (`csv_to_ndjson`) or a single JSON
array (`csv_to_json_array`). Optionally converts keys to `camelCase` and
generates a simple TypeScript `.d.ts` interface inferred from the sample.

**API**

```py
csv_to_ndjson(
    input_path, output_path, *,
    camel_case=False, time_col="time",
    chunksize=100_000, generate_dts=False,
    interface_name="Row"
) -> Path

csv_to_json_array(
    input_path, output_path, *,
    camel_case=False, time_col="time",
    generate_dts=False, interface_name="Row"
) -> Path
```

**Notes**

- `csv_to_ndjson` streams through chunks (memory friendly).
- `csv_to_json_array` loads entire CSV (not recommended for very large files).
- Both coerce `time` column to integer seconds and optionally output a `.d.ts` interface.

**Examples**

NDJSON with camelCase and `.d.ts`:

```bash
python - <<'PY'
from pathlib import Path
from tvparser import csv2json
inp = Path("merged_1min.csv")
out = inp.with_name("merged_1min.jsonl")
csv2json.csv_to_ndjson(inp, out, camel_case=True, generate_dts=True,
                       interface_name="MergedRow")
PY
```

JSON array:

```py
from tvparser import csv2json
csv2json.csv_to_json_array("merged_1min.csv", "merged_1min.json",
                           camel_case=True, generate_dts=True)
```

---

## `tvparser.slicer` — slice a CSV by timestamp window

**Purpose**

Extract rows from a 1-minute CSV whose timestamp column lies in a
[start_ts, end_ts] inclusive window.

**API**

```py
slice_csv_window(
    csv_path: str|Path,
    start_ts: int,
    end_ts: int,
    out_path: str|Path,
    ts_column: str = "ts"
) -> int
```

Returns number of rows written. Ensures parent dir exists. Raises
`FileNotFoundError` if input doesn't exist, or `ValueError` if the
timestamp column is missing.

**Example**

```py
from tvparser import slicer
n = slicer.slice_csv_window("gc_1min.csv",
                            start_ts=1728252000,
                            end_ts=1728252240,
                            out_path="window.csv")
print("written", n)
```

---

## `tvparser.timeutils` — parse SLrun dates/times → timestamps

**Purpose**

Helpers to convert SLrun-style rows (date like `10/13/24` and times like
`17:00`) to Unix timestamps in seconds (TradingView format). Handles
cross-midnight windows (e.g., `start=17:00` end `07:00` next day).

**API**

```py
to_timestamp(date_str, time_str, tz="UTC", *, to_ms=False) -> int
window_start_end(date_str, start_time, end_time, tz="UTC", *, to_ms=False) -> (start_ts, end_ts)
```

**Notes**

- `tz` can be `"UTC"` or IANA zone name like `"America/Chicago"` (requires stdlib `zoneinfo`).
- `to_ms=True` returns milliseconds (legacy support), but defaults to seconds.

**Examples**

```py
from tvparser import timeutils
start_ts = timeutils.to_timestamp("10/13/24", "17:00", tz="UTC")
start_ts, end_ts = timeutils.window_start_end("10/13/24", "17:00", "07:00",
                                             tz="UTC")
```

---

## `tvparser.extract_slrun` — parse SLrunLong and extract windows

**Purpose**

End-to-end helper and CLI that:

- parses an `SLrunLong.txt` file (CSV-like with header `date, start, entry, exit, end`),
- uses `timeutils.window_start_end()` to compute timestamps,
- calls `slicer.slice_csv_window()` for each window,
- writes per-window CSV files (safely), optionally skipping existing files,
  optionally continuing on error.

**Public API**

```py
# parse textual lines -> List[SLWindow]
parse_slrun_lines(lines: Iterable[str], tz: str = "UTC") -> List[SLWindow]

# format a safe filename for a window
format_window_filename(csv_path: Path, w: SLWindow) -> str

# extract windows defined in slrun_path from csv_path
extract_from_slrun(
    slrun_path: Path,
    csv_path: Path,
    out_dir: Optional[Path] = None,
    tz: str = "UTC",
    force: bool = False,
    continue_on_error: bool = False,
) -> List[Path]
```

**CLI usage**

```bash
python -m tvparser.extract_slrun --help
```

Example full run:

```bash
python -m tvparser.extract_slrun \
  --slrun tests/data/SLrunLong.txt \
  --csv /data/gc_1min.csv \
  --out-dir /data/out \
  --tz UTC \
  --force
```

---

# Examples — end-to-end flows

### 1) Merge CSVs and convert to NDJSON (one-liner)

```bash
python -m tvparser.cli \
  --input-dir /data/1min \
  --output /tmp/merged.csv

python - <<'PY'
from pathlib import Path
from tvparser import csv2json
csv2json.csv_to_ndjson("/tmp/merged.csv", "/tmp/merged.jsonl",
                       camel_case=True, generate_dts=True,
                       interface_name="MergedRow")
PY
```

### 2) Extract windows from `SLrunLong.txt` and produce JSON per window

```bash
python -m tvparser.extract_slrun \
  --slrun /path/to/SLrunLong.txt \
  --csv /data/gc_1min.csv \
  --out-dir /data/out \
  --tz UTC

# then convert each window CSV to NDJSON (optional)
python - <<'PY'
from pathlib import Path
from tvparser import csv2json
out_dir = Path("/data/out")
for csvf in out_dir.glob("*.csv"):
    csv2json.csv_to_ndjson(csvf, csvf.with_suffix(".jsonl"),
                           camel_case=True, generate_dts=False)
PY
```

---

# Testing & linting

Run tests and coverage locally:

```bash
# run all tests
python -m pytest -q

# run tests + coverage
python -m pytest -q --cov=src --cov-report=term-missing

# run a single integration test
python -m pytest -q tests/integration/test_extract_slrun.py -q

# lint with ruff
ruff check src tests --fix
```

Notes:

- Parquet tests require a parquet engine (`pyarrow` or `fastparquet`) to
  be installed, or tests skip/monkeypatch accordingly.
- Many tests rely on `pandas`.

---

# Troubleshooting / notes

- **Timestamps**: TradingView timestamps are in **seconds**. Functions in
  `timeutils` and `slicer` use seconds by default.
- **Parquet**: `io.write_parquet()` requires `pyarrow` or `fastparquet`.
- **Timezone**: `timeutils` uses `zoneinfo` (stdlib). If you use older
  Python versions without `zoneinfo`, named timezones will error.
- **Atomic writes**: `csv2json` and IO helpers use temp-file+`os.replace`
  atomic writes where appropriate.
- **Safety**: Do not set `--output` to the same path as any input CSV — it
  will overwrite that file. Use `--force` with care.

---

# Contributing

1. Fork, create a branch, implement tests first.
2. Run `ruff check` and `pytest`.
3. Open PR against `main` with description and tests.

Suggested dev tooling:

```bash
python -m pip install -e ".[dev]"
ruff check src tests
python -m pytest -q
```

---

# FAQ

**Q: Can I use this on very large CSV files?**
A: Use NDJSON mode with `csv2json.csv_to_ndjson()` and tune `chunksize` to
control memory use. `csv_to_json_array()` loads the whole CSV into memory —
avoid this on very large files.

**Q: How do I validate the JSON output?**
A: Use `jq` for syntax checks, `jsonschema` or `ajv` for schema validation.
You can generate a `.d.ts` for TypeScript consumers and then produce JSON
Schema from types if you want schema-based validation for CI.
