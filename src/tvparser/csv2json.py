from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("tvparser.csv2json")


def _to_camel(s: str) -> str:
    """Convert snake or space separated string to camelCase."""
    s = re.sub(r"[^\w\s]", "", s).strip()
    if not s:
        return s
    parts = re.split(r"[_\s]+", s)
    head = parts[0].lower()
    tail = "".join(p.title() for p in parts[1:])
    return head + tail


def _infer_ts_type(series: pd.Series) -> bool:
    """
    Return True if series looks like milliseconds timestamps.

    Heuristic: median > 1e12 indicates ms since epoch.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().all():
        return False
    median_val = float(numeric.median(skipna=True))
    return median_val > 1e12


def _coerce_time_column(df: pd.DataFrame, col: str = "time") -> pd.DataFrame:
    """
    Ensure `col` is integer seconds. If values look like ms they get divided.
    """
    if col not in df.columns:
        return df
    if _infer_ts_type(df[col]):
        df[col] = (pd.to_numeric(df[col], errors="coerce") // 1000).astype(
            "Int64"
        )
    else:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if not df[col].isna().all():
        df[col] = df[col].astype("int64")
    return df


def _pandas_dtype_to_ts(dtype: object) -> str:
    """Map pandas dtype kind to a TypeScript type string."""
    kind = getattr(dtype, "kind", "")
    if kind in ("i", "u", "f", "n"):
        return "number"
    if kind == "b":
        return "boolean"
    return "string"


def _build_interface(df: pd.DataFrame, name: str = "Row") -> str:
    """
    Build a simple TypeScript interface from DataFrame column types.

    Returns the interface text (not written to disk).
    """
    lines = [f"export interface {name} {{"]
    for col in df.columns:
        ts_type = _pandas_dtype_to_ts(df[col].dtype)
        nullable = df[col].isna().any()
        typ = f"{ts_type} | null" if nullable else ts_type
        prop = col if re.match(r"^[A-Za-z_]\w*$", col) else f"['{col}']"
        lines.append(f"  {prop}: {typ};")
    lines.append("}")
    return "\n".join(lines)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes to a temp file in same dir and move into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def csv_to_ndjson(
    input_path: str | Path,
    output_path: str | Path,
    *,
    camel_case: bool = False,
    time_col: str = "time",
    chunksize: int = 100_000,
    generate_dts: bool = False,
    interface_name: str = "Row",
) -> Path:
    """
    Convert CSV -> NDJSON (one JSON object per line).

    - streaming via pandas.read_csv(chunksize=...)
    - camel_case: convert column names to camelCase
    - generate_dts: create a sibling .d.ts file with inferred interface
    """
    inp = Path(input_path)
    out = Path(output_path)
    out_tmp = out.with_suffix(out.suffix + ".tmp")

    sample_frames: list[pd.DataFrame] = []

    # open temp file and stream chunked JSON lines
    with open(out_tmp, "wb") as fh:
        for chunk in pd.read_csv(inp, chunksize=chunksize):
            if camel_case:
                rename_map = {c: _to_camel(c) for c in chunk.columns}
                chunk = chunk.rename(columns=rename_map)

            chunk = _coerce_time_column(chunk, col=time_col)

            if generate_dts and len(sample_frames) < 3:
                sample_frames.append(chunk.copy(deep=False))

            records = chunk.to_dict(orient="records")
            for rec in records:
                fh.write((json.dumps(rec, default=int) + "\n").encode("utf-8"))

    # atomically move into final path
    os.replace(str(out_tmp), str(out))

    if generate_dts and sample_frames:
        sample_df = pd.concat(sample_frames, ignore_index=True)
        if camel_case:
            sample_df = sample_df.rename(
                columns={c: _to_camel(c) for c in sample_df.columns}
            )
        dts = _build_interface(sample_df, name=interface_name)
        dts_path = out.with_suffix(out.suffix + ".d.ts")
        _atomic_write_bytes(dts_path, dts.encode("utf-8"))
        LOGGER.info("Wrote TypeScript definition to %s", dts_path)

    LOGGER.info("Wrote NDJSON to %s", out)
    return out


def csv_to_json_array(
    input_path: str | Path,
    output_path: str | Path,
    *,
    camel_case: bool = False,
    time_col: str = "time",
    generate_dts: bool = False,
    interface_name: str = "Row",
) -> Path:
    """
    Convert CSV -> single JSON array (not streaming).

    Not suitable for very large files.
    """
    inp = Path(input_path)
    out = Path(output_path)

    df = pd.read_csv(inp)
    if camel_case:
        df = df.rename(columns={c: _to_camel(c) for c in df.columns})
    df = _coerce_time_column(df, col=time_col)

    data = df.to_dict(orient="records")
    payload = json.dumps(data, ensure_ascii=False)

    _atomic_write_bytes(out, payload.encode("utf-8"))

    if generate_dts:
        dts = _build_interface(df, name=interface_name)
        dts_path = out.with_suffix(out.suffix + ".d.ts")
        _atomic_write_bytes(dts_path, dts.encode("utf-8"))
        LOGGER.info("Wrote TypeScript definition to %s", dts_path)

    LOGGER.info("Wrote array JSON to %s", out)
    return out
