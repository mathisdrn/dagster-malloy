"""Tests for MalloyResource."""

from unittest.mock import MagicMock, patch

import polars as pl

from dagster_malloy.resource import MalloyResource


def test_resource_initialization():
    resource = MalloyResource(
        execution_mode="cli",
        cli_path="npx malloy-cli",
        config_path="config.json",
        project_dir=".",
    )
    assert resource.execution_mode == "cli"
    assert resource.cli_path == "npx malloy-cli"

    import os
    cli_client = resource.get_cli_client()
    assert cli_client.cli_path == "npx malloy-cli"
    assert cli_client.config_path == os.path.abspath("config.json")
    assert cli_client.project_dir == os.path.abspath(".")


@patch("dagster_malloy.resource.MalloyCliClient")
def test_resource_compile_query_cli(mock_cli_cls):
    mock_instance = MagicMock()
    mock_instance.compile.return_value = ("SELECT 1", "duckdb")
    mock_cli_cls.return_value = mock_instance

    resource = MalloyResource(execution_mode="cli")
    sql, dialect = resource.compile_query(file_path="model.malloy", query_name="q1")

    assert sql == "SELECT 1"
    assert dialect == "duckdb"
    mock_instance.compile.assert_called_once_with(file_path="model.malloy", query_name="q1")


@patch("dagster_malloy.resource.MalloyCliClient")
def test_resource_execute_query_cli(mock_cli_cls):
    mock_instance = MagicMock()
    mock_instance.run.return_value = pl.DataFrame([{"a": 1}])
    mock_cli_cls.return_value = mock_instance

    resource = MalloyResource(execution_mode="cli")
    df = resource.execute_query(file_path="model.malloy", query_name="q1")

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 1
    mock_instance.run.assert_called_once_with(file_path="model.malloy", query_name="q1", raw_query=None)
