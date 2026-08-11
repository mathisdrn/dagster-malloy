"""Tests for inline asset check execution in load_malloy_assets."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
from dagster import (
    AssetKey,
    materialize_to_memory,
)

from dagster_malloy.asset_decorator import load_malloy_assets
from dagster_malloy.resource import MalloyResource


def test_inline_asset_checks_registration(tmp_path: Path):
    file_path = tmp_path / "checks.malloy"
    file_path.write_text(
        """
source: orders is duckdb.table('orders.parquet')

query: order_summary is orders -> { aggregate: total is count() }

query: check_valid_ids is orders -> {
  where: id is null
  aggregate: invalid_count is count()
}
""",
        encoding="utf-8",
    )

    assets_def = load_malloy_assets(tmp_path, include_checks=True)

    assert assets_def is not None
    check_specs = list(assets_def.check_specs)
    assert len(check_specs) == 1
    assert check_specs[0].name == "check_valid_ids"


@patch("dagster_malloy.resource.MalloyCliClient")
def test_inline_asset_checks_execution(mock_cli_cls, tmp_path: Path):
    file_path = tmp_path / "checks.malloy"
    file_path.write_text(
        """
source: orders is duckdb.table('orders.parquet')

query: order_summary is orders -> { aggregate: total is count() }

query: check_valid_ids is orders -> {
  where: id is null
  aggregate: invalid_count is count()
}
""",
        encoding="utf-8",
    )

    mock_cli = MagicMock()
    mock_cli.compile.return_value = ("SELECT 1", "duckdb")

    def mock_run(file_path, query_name, raw_query=None):
        if query_name == "order_summary":
            return pl.DataFrame([{"total": 100}])
        elif query_name == "check_valid_ids":
            return pl.DataFrame([{"invalid_count": 0}])
        return pl.DataFrame()

    mock_cli.run.side_effect = mock_run
    mock_cli_cls.return_value = mock_cli

    assets_def = load_malloy_assets(tmp_path, include_checks=True)
    resource = MalloyResource(execution_mode="cli")

    result = materialize_to_memory(
        [assets_def],
        resources={"malloy": resource},
    )

    assert result.success
    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    assert check_evals[0].check_name == "check_valid_ids"
    assert check_evals[0].passed is True
