from __future__ import annotations

from pathlib import Path

from tvparser import extract_slrun

SAMPLE_CSV = """ts,o
1728252000,10
1728252060,20
1728252120,30
1728252180,40
1728252240,50
"""


def test_parse_slrun_without_header() -> None:
    """If header missing, first line should be treated as data."""
    lines = ["10/13/24, 17:00, 19:30, 23:30, 07:00"]
    windows = extract_slrun.parse_slrun_lines(lines, tz="UTC")
    assert len(windows) == 1
    w = windows[0]
    assert w.date_str.startswith("10/13/24")
    assert hasattr(w, "start_ts") and hasattr(w, "end_ts")


def test_format_window_filename_handles_4digit_year(tmp_path: Path) -> None:
    """format_window_filename accepts MM/DD/YYYY as date_str as well."""
    w = extract_slrun.SLWindow(
        date_str="10/13/2024",
        start="17:00",
        end="07:00",
        start_ts=1,
        end_ts=2,
    )
    fname = extract_slrun.format_window_filename(Path("gc_1min.csv"), w)
    assert "2024-10-13" in fname
    assert "17-00" in fname and "07-00" in fname


def test_extract_from_slrun_skip_and_force(tmp_path: Path) -> None:
    """If output exists, skip unless force=True; force overwrites."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csvp = data_dir / "gc_1min.csv"
    slrun = data_dir / "SLrunLong.txt"
    csvp.write_text(SAMPLE_CSV)
    slrun.write_text(
        "date, start, entry, exit, end\n" "10/06/24, 17:00, 17:30, 18:00, 18:30\n"
    )

    # compute expected filename (same logic as extract_from_slrun)
    slrun_lines = slrun.read_text().splitlines()
    windows = extract_slrun.parse_slrun_lines(slrun_lines, tz="UTC")
    assert len(windows) == 1
    w = windows[0]
    fname = extract_slrun.format_window_filename(csvp, w)
    outp = data_dir / fname

    # create an existing file with sentinel content
    outp.write_text("SENTINEL")

    # without force, extract_from_slrun should skip (no overwrite)
    written = extract_slrun.extract_from_slrun(
        slrun, csvp, out_dir=data_dir, force=False
    )
    assert outp.exists()
    assert outp.read_text() == "SENTINEL"
    # returned written should not include the existing file
    assert outp not in written

    # with force=True it should overwrite (and include in returned list)
    written2 = extract_slrun.extract_from_slrun(
        slrun,
        csvp,
        out_dir=data_dir,
        force=True,
    )
    assert outp in written2
    assert outp.exists()
    # content should now be CSV with header (not the old sentinel)
    missing_slrun = tmp_path / "nope.txt"
    csvp = tmp_path / "nope.csv"
    rc1 = extract_slrun.main(["--slrun", str(missing_slrun), "--csv", str(csvp)])
    # create slrun but not csv
    slrun = tmp_path / "SLrunLong.txt"
    slrun.write_text("date, start, entry, exit, end\n")
    rc2 = extract_slrun.main(["--slrun", str(slrun), "--csv", str(csvp)])
    assert rc2 == 1
    assert rc1 == 1

    # create slrun but not csv
    slrun = tmp_path / "SLrunLong.txt"
    csvp = data_dir / "gc_1min.csv"
    slrun = data_dir / "SLrunLong.txt"
    csvp.write_text(SAMPLE_CSV)
    slrun.write_text(
        "date, start, entry, exit, end\n" "10/06/24, 17:00, 17:30, 18:00, 18:30\n"
    )


def test_main_returns_2_on_internal_error(tmp_path: Path, monkeypatch) -> None:
    """If slicer raises an error during extraction, main should return 2."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csvp = data_dir / "gc_1min.csv"
    slrun = data_dir / "SLrunLong.txt"
    csvp.write_text(SAMPLE_CSV)
    slrun.write_text(
        "date, start, entry, exit, end\n10/06/24, 17:00, 17:30, 18:00, 18:30\n"
    )

    def boom(*a, **k):
        raise RuntimeError("boom")

    # monkeypatch slicer.slice_csv_window to raise so main catches and returns 2
    monkeypatch.setattr(extract_slrun, "slicer", extract_slrun.slicer)
    monkeypatch.setattr(extract_slrun.slicer, "slice_csv_window", boom)

    rc = extract_slrun.main(
        [
            "--slrun",
            str(slrun),
            "--csv",
            str(csvp),
            "--out-dir",
            str(data_dir),
        ]
    )
    assert rc == 2
