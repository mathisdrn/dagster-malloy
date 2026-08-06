"""End-to-end tests for load_malloy_assets and multi_asset execution."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    materialize_to_memory,
)

from dagster_malloy.asset_decorator import load_malloy_assets
from dagster_malloy.resource import MalloyResource


@pytest.fixture
def temp_malloy_dir(tmp_path: Path) -> Path:
    malloy_file = tmp_path / "sales.malloy"
    malloy_file.write_text(
        """
source: orders is duckdb.table('orders.parquet') extend {
  dimension: amount is price * qty
}

query: revenue_by_region is orders -> {
  group_by: region
  aggregate: total_revenue is sum(amount)
}

# @dashboard
query: region_dashboard is orders -> {
  group_by: region
  aggregate: total_revenue is sum(amount)
}
""",
        encoding="utf-8",
    )
    return tmp_path


def test_load_malloy_assets_discovery(temp_malloy_dir: Path):
    assets_def = load_malloy_assets(temp_malloy_dir)

    assert assets_def is not None
    assert len(assets_def.keys) == 3  # revenue_by_region, region_dashboard, region_dashboard_dashboard
    expected_key = AssetKey(["sales", "revenue_by_region"])
    assert expected_key in assets_def.keys


@patch("dagster_malloy.resource.MalloyCliClient")
def test_materialize_malloy_assets(mock_cli_cls, temp_malloy_dir: Path):
    mock_cli = MagicMock()
    mock_cli.compile.return_value = (
        "SELECT region, sum(price * qty) AS total_revenue FROM orders GROUP BY region",
        "duckdb",
    )
    mock_cli.run.return_value = pd.DataFrame(
        [
            {"region": "US", "total_revenue": 1000},
            {"region": "EU", "total_revenue": 800},
        ]
    )
    mock_cli_cls.return_value = mock_cli

    assets_def = load_malloy_assets(temp_malloy_dir)
    resource = MalloyResource(execution_mode="cli")

    result = materialize_to_memory(
        [assets_def],
        resources={"malloy": resource},
    )

    assert result.success
    mat_events = result.get_asset_materialization_events()
    assert len(mat_events) == 3

    mat_event = mat_events[0]
    metadata = mat_event.materialization.metadata

    assert metadata["dagster/row_count"].value == 2
    assert metadata["dialect"].value == "duckdb"
    assert "SELECT region" in metadata["compiled_sql"].value
