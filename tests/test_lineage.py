"""Tests focusing on Dagster asset lineage and dependency resolution for Malloy models."""

from pathlib import Path
from dagster import AssetKey
from dagster_malloy import load_malloy_assets
from dagster_malloy.parser import MalloyParser


def test_lineage_multi_stage_pipeline(tmp_path: Path):
    file_path = tmp_path / "pipeline.malloy"
    file_path.write_text(
        """
source: orders is duckdb.table('data/orders.parquet') extend {
  primary_key: id
  dimension: category is 'general'
}

# Multi-stage aggregation pipeline query
# @dashboard
# @bar_chart
query: top_5_categories is orders -> {
  group_by: category
  aggregate: gross_revenue is sum(price * quantity)
} -> {
  top: 5
  order_by: gross_revenue desc
}
""",
        encoding="utf-8",
    )

    # 1. Test AST Parsing
    parsed = MalloyParser.parse_file(file_path)
    assert "orders" in parsed.sources
    assert "top_5_categories" in parsed.queries

    # 2. Test Asset Lineage Generation
    assets_def = load_malloy_assets(file_path, include_sources=True, create_dashboards=True)
    key_to_deps = {spec.key: spec.deps for spec in assets_def.specs}

    query_key = AssetKey(["pipeline", "top_5_categories"])
    source_key = AssetKey(["pipeline", "orders"])
    dashboard_key = AssetKey(["pipeline", "top_5_categories_dashboard"])

    assert query_key in key_to_deps
    assert source_key in key_to_deps
    assert dashboard_key in key_to_deps

    # Verify query asset depends on source asset
    query_deps = [dep.asset_key for dep in key_to_deps[query_key]]
    assert source_key in query_deps

    # Verify dashboard asset depends on query asset
    dash_deps = [dep.asset_key for dep in key_to_deps[dashboard_key]]
    assert query_key in dash_deps


def test_lineage_complex_joins_and_sources(tmp_path: Path):
    file_path = tmp_path / "joins.malloy"
    file_path.write_text(
        """
source: users is duckdb.table('data/users.parquet') extend {
  primary_key: id
}

source: products is duckdb.table('data/products.parquet') extend {
  primary_key: id
}

source: orders is duckdb.table('data/orders.parquet') extend {
  primary_key: id
  join_one: users on customer_id = users.id
  join_many: products on product_id = products.id

  view: customer_summary is {
    group_by: users.name
    aggregate: total_spent is sum(price)
  }
}

query: order_summary is orders -> customer_summary
""",
        encoding="utf-8",
    )

    # 1. Test AST Parsing
    parsed = MalloyParser.parse_file(file_path)
    assert parsed.sources["orders"].joined_sources == {"users", "products"}

    # 2. Test Lineage Resolution for Joined Sources
    assets_def = load_malloy_assets(file_path, include_sources=True)
    key_to_deps = {spec.key: spec.deps for spec in assets_def.specs}

    order_query_key = AssetKey(["joins", "order_summary"])
    orders_source_key = AssetKey(["joins", "orders"])
    users_source_key = AssetKey(["joins", "users"])
    products_source_key = AssetKey(["joins", "products"])

    query_deps = [dep.asset_key for dep in key_to_deps[order_query_key]]

    # The query asset should depend on primary source (orders) AND joined sources (users, products)
    assert orders_source_key in query_deps
    assert users_source_key in query_deps
    assert products_source_key in query_deps


def test_lineage_table_dependencies_without_source_assets(tmp_path: Path):
    file_path = tmp_path / "raw_tables.malloy"
    file_path.write_text(
        """
source: raw_events is duckdb.table('data/raw_events.parquet') extend {
  primary_key: id
}

query: event_count is raw_events -> {
  aggregate: total is count()
}
""",
        encoding="utf-8",
    )

    # Test lineage when include_sources=False (direct raw table dependency keys)
    assets_def = load_malloy_assets(file_path, include_sources=False)
    key_to_deps = {spec.key: spec.deps for spec in assets_def.specs}

    query_key = AssetKey(["raw_tables", "event_count"])
    assert query_key in key_to_deps

    query_deps = [dep.asset_key for dep in key_to_deps[query_key]]
    table_dep_key = AssetKey(["data", "raw_events"])
    assert table_dep_key in query_deps


def test_lineage_nested_view_dependencies(tmp_path: Path):
    file_path = tmp_path / "nested_views.malloy"
    file_path.write_text(
        """
source: sales is duckdb.table('data/sales.parquet') extend {
  primary_key: id
}

query: base_summary is sales -> {
  group_by: category
  aggregate: revenue is sum(price)
}

query: top_summary is sales -> {
  nest: base_summary
}
""",
        encoding="utf-8",
    )

    assets_def = load_malloy_assets(file_path, include_sources=False)
    key_to_deps = {spec.key: spec.deps for spec in assets_def.specs}

    top_query_key = AssetKey(["nested_views", "top_summary"])
    base_query_key = AssetKey(["nested_views", "base_summary"])

    top_deps = [dep.asset_key for dep in key_to_deps[top_query_key]]
    assert base_query_key in top_deps
