"""Tests for MalloyCliClient."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from dagster_malloy.cli_client import MalloyCliClient, MalloyCliError


@patch("subprocess.run")
def test_cli_compile_success(mock_run):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout='{"sql": "SELECT 1 FROM users", "dialect": "duckdb"}',
        stderr="",
    )

    client = MalloyCliClient(cli_path="malloy-cli")
    sql, dialect = client.compile(file_path="model.malloy", query_name="q1")

    assert sql == "SELECT 1 FROM users"
    assert dialect == "duckdb"
    mock_run.assert_called_once()
    args = mock_run.call_args[0][0]
    assert "compile" in args
    assert "--name" in args
    assert "q1" in args


@patch("subprocess.run")
def test_cli_compile_failure(mock_run):
    mock_run.return_value = MagicMock(
        returncode=1,
        stdout="",
        stderr="Syntax error at line 5",
    )

    client = MalloyCliClient(cli_path="malloy-cli")
    with pytest.raises(MalloyCliError, match="malloy-cli compile failed"):
        client.compile(file_path="model.malloy", query_name="q1")


@patch("subprocess.run")
def test_cli_run_success(mock_run):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout='[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]',
        stderr="",
    )

    client = MalloyCliClient(cli_path="malloy-cli")
    df = client.run(file_path="model.malloy", query_name="q1")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]
