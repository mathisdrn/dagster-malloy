"""Tests for MalloyCliClient."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dagster_malloy.cli_client import MalloyCliClient, MalloyCliError, _format_cli_error


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
        stderr='{"error": "Reference to undefined object \'comments\'"}',
    )

    client = MalloyCliClient(cli_path="malloy-cli")
    with pytest.raises(MalloyCliError, match="Malloy Compiler Error"):
        client.compile(file_path="model.malloy", query_name="q1")


def test_format_cli_error_json():
    raw_json = '{"error": "Reference to undefined object \'comments\'"}'
    formatted = _format_cli_error(raw_json)
    assert "Malloy Compiler Error:" in formatted
    assert "Reference to undefined object 'comments'" in formatted


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


@patch("subprocess.run")
def test_cli_cwd_resolution_project_dir(mock_run, tmp_path):
    mock_run.return_value = MagicMock(returncode=0, stdout="[]")
    
    project_dir = tmp_path / "my_project"
    project_dir.mkdir()
    
    client = MalloyCliClient(cli_path="malloy-cli", project_dir=project_dir)
    client.run(file_path=tmp_path / "model.malloy", query_name="q")
    
    kwargs = mock_run.call_args[1]
    assert kwargs.get("cwd") == str(project_dir.resolve())


@patch("subprocess.run")
def test_cli_cwd_resolution_config_path(mock_run, tmp_path):
    mock_run.return_value = MagicMock(returncode=0, stdout="[]")
    
    config_dir = tmp_path / "config_dir"
    config_dir.mkdir()
    config_file = config_dir / "malloy-config.json"
    config_file.touch()
    
    client = MalloyCliClient(cli_path="malloy-cli", config_path=config_file)
    client.run(file_path=tmp_path / "model.malloy", query_name="q")
    
    kwargs = mock_run.call_args[1]
    assert kwargs.get("cwd") == str(config_dir.resolve())


@patch("subprocess.run")
def test_cli_cwd_resolution_auto_discover(mock_run, tmp_path):
    mock_run.return_value = MagicMock(returncode=0, stdout="[]")
    
    project_root = tmp_path / "nested_project"
    project_root.mkdir()
    (project_root / "malloy-config.json").touch()
    
    sub_dir = project_root / "src" / "queries"
    sub_dir.mkdir(parents=True)
    model_file = sub_dir / "model.malloy"
    model_file.touch()
    
    client = MalloyCliClient(cli_path="malloy-cli")
    client.run(file_path=model_file, query_name="q")
    
    kwargs = mock_run.call_args[1]
    assert kwargs.get("cwd") == str(project_root.resolve())

