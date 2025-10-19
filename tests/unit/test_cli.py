from __future__ import annotations

from typing import Any, Callable, List, Optional

import pytest


def run_cli_main(main_func: Callable[[Optional[List[str]]], Any],
                 argv: Optional[List[str]] = None) -> int:
    """
    Call CLI main and return int exit code.

    Handles functions that either return int or call sys.exit(...).
    """
    try:
        result = main_func(argv)
    except SystemExit as exc:
        code = exc.code if exc.code is not None else 0
        return int(code)
    return int(result or 0)


def make_sample_csv(path):
    path.write_text(
        "time,open,high,low,close,Volume\n"
        "1736722800,2716.8,2717.9,2715.1,2716.1,292\n"
        "1736722860,2716.1,2716.4,2714,2715.1,165\n"
    )


def test_cli_calls_core_and_writes_output(tmp_path, monkeypatch):
    """
    CLI should call core.merge_frames and io.write_csv when given inputs
    and an output path.
    """
    import tvparser.cli as cli

    f1 = tmp_path / "a.csv"
    f2 = tmp_path / "b.csv"
    make_sample_csv(f1)
    make_sample_csv(f2)

    SENTINEL = object()
    state = {"merge_called": False, "write_args": None}

    def fake_merge(frames, *a, **k):
        state["merge_called"] = True
        return SENTINEL

    def fake_write(df, path, *a, **k):
        state["write_args"] = (df, path)

    # patch core.merge_frames and core.summarize
    monkeypatch.setattr(cli, "core", cli.core)
    monkeypatch.setattr(cli.core, "merge_frames", fake_merge)
    monkeypatch.setattr(cli.core, "summarize",
                        lambda df: {"rows": 2, "start_time": 1_736_722_800,
                                    "end_time": 1_736_722_860})

    # patch io.write_csv
    monkeypatch.setattr(cli, "io", cli.io)
    monkeypatch.setattr(cli.io, "write_csv", fake_write)

    out = tmp_path / "out.csv"
    argv = ["--input", str(f1), "--input", str(f2), "--output", str(out)]
    rc = run_cli_main(cli.main, argv)
    assert rc == 0

    assert state["merge_called"] is True
    assert state["write_args"] is not None
    df_arg, path_arg = state["write_args"]
    assert df_arg is SENTINEL
    assert str(path_arg) == str(out)


def test_cli_dry_run_prints_summary_and_does_not_write(
    tmp_path, monkeypatch, capsys
) -> None:
    """--dry-run should print a summary and not call write_csv."""
    import tvparser.cli as cli

    f = tmp_path / "a.csv"
    make_sample_csv(f)

    SENTINEL = object()

    monkeypatch.setattr(cli, "core", cli.core)
    monkeypatch.setattr(cli.core, "merge_frames",
                        lambda frames, *a, **k: SENTINEL)
    monkeypatch.setattr(cli.core, "summarize",
                        lambda df: {"rows": 2, "start_time": 1, "end_time": 2})

    def fail_if_called(*a, **k):
        pytest.fail("io.write_csv must not be called on --dry-run")

    monkeypatch.setattr(cli, "io", cli.io)
    monkeypatch.setattr(cli.io, "write_csv", fail_if_called)

    argv = ["--input", str(f), "--dry-run"]
    rc = run_cli_main(cli.main, argv)
    assert rc == 0

    captured = capsys.readouterr()
    out = captured.out
    assert any(k in out for k in ("rows", "start_time", "end_time"))


def test_cli_returns_nonzero_on_core_error(tmp_path, monkeypatch) -> None:
    """If core.merge_frames raises, CLI should return non-zero."""
    import tvparser.cli as cli

    f = tmp_path / "a.csv"
    make_sample_csv(f)

    def explosive(frames, *a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "core", cli.core)
    monkeypatch.setattr(cli.core, "merge_frames", explosive)

    argv = ["--input", str(f), "--output", str(tmp_path / "out.csv")]
    rc = run_cli_main(cli.main, argv)
    assert rc != 0
