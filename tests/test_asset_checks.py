"""Tests for build_malloy_asset_checks."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from dagster import AssetKey, materialize_to_memory

from dagster_malloy.asset_checks import build_malloy_asset_checks
from dagster_malloy.resource import MalloyResource


@pytest.fixture
def temp_check_file(tmp_path: Path) -> Path:
    check_file = tmp_path / "checks.malloy"
    check_file.write_text(
        """
source: users is duckdb.table('users.parquet')

# @check
query: check_no_null_users is users -> {
  where: id is null
  aggregate: null_count is count()
}
""",
        encoding="utf-8",
    )
    return check_file


@patch("dagster_malloy.resource.MalloyCliClient")
def test_malloy_asset_check_execution_passing(mock_cli_cls, temp_check_file: Path):
    mock_cli = MagicMock()
    # 0 failing rows
    mock_cli.run.return_value = pd.DataFrame([{"null_count": 0}])
    mock_cli_cls.return_value = mock_cli

    target_key = AssetKey(["users", "user_table"])
    check_defs = build_malloy_asset_checks(
        file_path=temp_check_file,
        target_asset_key=target_key,
    )

    assert len(check_defs) == 1
    check_fn = check_defs[0]
    check_key = list(check_fn.check_keys)[0]
    assert check_key.name == "check_no_null_users"
