from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tests.support.json_helpers import canonical_records_from_csv
from tvparser import csv2json

if TYPE_CHECKING:
    from pathlib import Path

BASE_CSV = """time,open,high,low,close,Volume
1736722800,2716.8,2717.9,2715.1,2716.1,292
1736722860,2716.1,2716.4,2714,2715.1,165
1736722920,2715.1,2715.8,2714.5,2714.5,87
1736722980,2714.8,2716.6,2714.2,2716.4,69
1736723040,2716.3,2717.5,2715.3,2717,109
1736723100,2716.9,2717.8,2716.6,2717,66
1736723160,2717,2717.3,2715.7,2716,61
1736723220,2716.2,2716.7,2716.1,2716.1,45
1736723280,2716.3,2717.2,2716.2,2717,28
1736723340,2716.9,2717.5,2716.9,2717.3,27
"""


def _write_sample(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(BASE_CSV)


def test_csv_to_ndjson_streaming_and_dts(tmp_path: Path) -> None:
    """
    csv_to_ndjson should stream input in chunks, write NDJSON lines,
    and generate a .d.ts when requested. The JSON objects should match
    the canonical records derived from the CSV.
    """
    inp = tmp_path / "in.csv"
    _write_sample(inp)

    out = tmp_path / "out.jsonl"
    dts = out.with_suffix(out.suffix + ".d.ts")

    # expected canonical records (camelCase)
    expected = canonical_records_from_csv(inp, camel_case=True)

    # run converter with small chunksize to exercise streaming loop
    csv2json.csv_to_ndjson(
        input_path=inp,
        output_path=out,
        camel_case=True,
        generate_dts=True,
        interface_name="Row",
        chunksize=2,
    )

    assert out.exists()
    assert dts.exists()

    lines = out.read_text(encoding="utf-8").splitlines()
    actual = [json.loads(_line) for _line in lines]

    assert len(actual) == len(expected)
    # semantic equality (order preserved)
    for exp, act in zip(expected, actual, strict=False):
        assert exp == act

    # simple check that d.ts exposes interface and keys
    dts_txt = dts.read_text(encoding="utf-8")
    assert "export interface Row" in dts_txt
    assert "time:" in dts_txt and "volume:" in dts_txt


def test_csv_to_json_array_and_dts_matches_expected(tmp_path: Path) -> None:
    """
    csv_to_json_array writes a single JSON array that should match
    canonical records derived with camelCase enabled.
    """
    inp = tmp_path / "in2.csv"
    _write_sample(inp)

    out = tmp_path / "out.json"
    dts = out.with_suffix(out.suffix + ".d.ts")

    expected = canonical_records_from_csv(inp, camel_case=True)

    csv2json.csv_to_json_array(
        input_path=inp,
        output_path=out,
        camel_case=True,
        generate_dts=True,
        interface_name="Row",
    )

    assert out.exists()
    assert dts.exists()

    data = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == len(expected)
    for exp, act in zip(expected, data, strict=False):
        assert exp == act

    dts_txt = dts.read_text(encoding="utf-8")
    assert "export interface Row" in dts_txt
    assert "open:" in dts_txt and "close:" in dts_txt


def test_camel_case_toggle_preserves_keys_when_disabled(tmp_path: Path) -> None:
    """
    When camel_case=False, keys in output should match the original CSV
    column names (e.g. 'Volume' with capital V).
    """
    inp = tmp_path / "nocamel.csv"
    _write_sample(inp)

    out = tmp_path / "nocamel.jsonl"
    csv2json.csv_to_ndjson(
        input_path=inp,
        output_path=out,
        camel_case=False,
        generate_dts=False,
        chunksize=3,
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 10
    first = json.loads(lines[0])
    # original CSV had 'Volume' with capital V
    assert "Volume" in first
    assert "volume" not in first
