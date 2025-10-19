from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pathlib import Path


from tvparser import cli


def run_main(argv: Optional[list[str]] = None) -> int:
    """Call cli.main and return numeric exit code."""
    try:
        rc = cli.main(argv)
    except SystemExit as exc:
        rc = int(exc.code or 0)
    return int(rc)


def test_cli_missing_output_returns_nonzero(tmp_path: Path) -> None:
    """If --output is missing and not dry-run, CLI returns 1."""
    f = tmp_path / "a.csv"
    f.write_text("time,open,high,low,close,Volume\n1,1,2,3,4,5\n")

    rc = run_main(["--input", str(f)])
    assert rc == 1


def test_cli_input_dir_not_found_returns_1() -> None:
    """Non-existent --input-dir should return error code 1."""
    rc = run_main(["--input-dir", "nope-i-dont-exist"])
    assert rc == 1


def test_cli_write_failure_returns_nonzero(
    tmp_path: Path, monkeypatch
) -> None:
    """
    If io.write_csv raises, CLI should return non-zero (2).
    This patches io.write_csv to raise and verifies the code path.
    """
    d = tmp_path / "in"
    d.mkdir()
    p = d / "x.csv"
    p.write_text("time,open,high,low,close,Volume\n1,1,2,3,4,5\n")

    def bad_write(df, path, *a, **k):
        raise RuntimeError("disk is full")

    monkeypatch.setattr(cli, "io", cli.io)
    monkeypatch.setattr(
        cli.io, "write_csv", bad_write
    )

    rc = run_main([
        "--input-dir", str(d),
        "--output", str(tmp_path / "out.csv")
    ])
    assert rc == 2


def test_cli_verbose_and_dry_run_ok(tmp_path: Path) -> None:
    """
    Run CLI with verbose flags and --dry-run to exercise logging setup
    and dry-run code path.
    """
    p = tmp_path / "f.csv"
    p.write_text("time,open,high,low,close,Volume\n1,1,2,3,4,5\n")

    rc1 = run_main(["-v", "--input", str(p), "--dry-run"])
    rc2 = run_main(["-vv", "--input", str(p), "--dry-run"])
    assert rc1 == 0
    assert rc2 == 0
