from __future__ import annotations

import importlib

from typer.testing import CliRunner

import oznak.cli


runner = CliRunner()


def test_legacy_cli_module_delegates_to_package_cli() -> None:
    module = importlib.import_module("src.cli.main")

    assert module.app is oznak.cli.app
    assert module.benchmark_chunked is oznak.cli.benchmark_chunked
    assert module.load is oznak.cli.load
    assert module.load_chunked is oznak.cli.load_chunked


def test_legacy_cli_app_exposes_package_commands() -> None:
    module = importlib.import_module("src.cli.main")

    result = runner.invoke(module.app, ["--help"])

    assert result.exit_code == 0
    assert "benchmark-chunked" in result.stdout
    assert "load-chunked" in result.stdout
    assert "tui" in result.stdout
