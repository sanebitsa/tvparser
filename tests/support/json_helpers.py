from __future__ import annotations

import json
from pathlib import Path

from tests.support.json_helpers import canonical_records_from_csv
from tvparser import csv2json

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


def _ensure_test_csv(path: Path) -> None:
    """Write canonical CSV used by these integration tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(BASE_CSV)


def test_csv_to_ndjson_and_dts_matches_expected(tmp_path: Path) -> None:
    """
    Convert the test CSV to NDJSON + .d.ts (camelCase) and assert output
    semantically matches expected records derived from the CSV.
    """
    data_dir = Path("tests") / "data"
    in_path = data_dir / "gc_011224_020524_1m.csv"
    _ensure_test_csv(in_path)

    expected = canonical_records_from_csv(in_path, camel_case=True)

    out = tmp_path / "merged.jsonl"
    dts_path = out.with_suffix(out.suffix + ".d.ts")

    csv2json.csv_to_ndjson(
        input_path=in_path,
        output_path=out,
        camel_case=True,
        generate_dts=True,
        interface_name="MergedRow",
    )

    assert out.exists()
    assert dts_path.exists()

    # read NDJSON and parse objects
    lines = out.read_text(encoding="utf-8").splitlines()
    actual = [json.loads(line) for line in lines]

    assert len(actual) == len(expected)
    # Compare records one-by-one for semantic equality
    for exp, act in zip(expected, actual, strict=False):
        assert exp == act

    dts_txt = dts_path.read_text(encoding="utf-8")
    assert "export interface MergedRow" in dts_txt
    assert "time:" in dts_txt and "volume:" in dts_txt


def test_csv_to_json_array_and_dts_matches_expected(tmp_path: Path) -> None:
    """
    Convert the test CSV to a single JSON array + .d.ts and assert the
    array content matches the expected canonical records.
    """
    data_dir = Path("tests") / "data"
    in_path = data_dir / "gc_011224_020524_1m.csv"
    _ensure_test_csv(in_path)

    expected = canonical_records_from_csv(in_path, camel_case=True)

    out = tmp_path / "merged.json"
    dts_path = out.with_suffix(out.suffix + ".d.ts")

    csv2json.csv_to_json_array(
        input_path=in_path,
        output_path=out,
        camel_case=True,
        generate_dts=True,
        interface_name="MergedRow",
    )

    assert out.exists()
    assert dts_path.exists()

    data = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == len(expected)

    # row-by-row semantic comparison
    for exp, act in zip(expected, data, strict=False):
        assert exp == act

    dts_txt = dts_path.read_text(encoding="utf-8")
    assert "export interface MergedRow" in dts_txt
    assert "open:" in dts_txt and "close:" in dts_txt
