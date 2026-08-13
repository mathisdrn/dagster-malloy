"""Tests for batch AST parsing, in-memory manifests, DDL identifier quoting, and transitive join resolution."""

from pathlib import Path
from unittest.mock import patch
import pytest
from dagster import AssetKey

from dagster_malloy.asset_checks import build_malloy_asset_checks
from dagster_malloy.asset_decorator import load_malloy_assets
from dagster_malloy.cli_client import MalloyCliClient
from dagster_malloy.parser import MalloyParsedModel, MalloyQueryInfo, MalloySourceInfo
from dagster_malloy.project import MalloyProject
from dagster_malloy.resource import _quote_identifier, generate_ddl
from dagster_malloy.translator import MalloyTranslator, MalloyTranslatorData, _get_all_joined_sources, _table_to_asset_key


def test_quote_identifier():
    """Test dialect-specific identifier quoting in DDL generation."""
    # Postgres / DuckDB / Snowflake (double quotes)
    assert _quote_identifier("my_table", "postgres") == '"my_table"'
    assert _quote_identifier("my-table", "duckdb") == '"my-table"'
    assert _quote_identifier("schema.table", "snowflake") == '"schema"."table"'
    assert _quote_identifier('"already_quoted"', "duckdb") == '"already_quoted"'

    # BigQuery / MySQL (backticks)
    assert _quote_identifier("my_dataset.my_table", "bigquery") == "`my_dataset`.`my_table`"
    assert _quote_identifier("my_table", "mysql") == "`my_table`"
    assert _quote_identifier("`already_quoted`", "bigquery") == "`already_quoted`"


def test_generate_ddl_with_quoted_identifiers():
    """Test generate_ddl uses quoted identifiers."""
    sql = "SELECT 1 as x"
    ddl_postgres = generate_ddl(sql, "postgres", "my-table")
    assert 'DROP TABLE IF EXISTS "my-table";' in ddl_postgres
    assert 'CREATE TABLE "my-table" AS' in ddl_postgres

    ddl_bq = generate_ddl(sql, "bigquery", "analytics.my-table")
    assert "CREATE OR REPLACE TABLE `analytics`.`my-table` AS" in ddl_bq


def test_transitive_joined_sources_resolution():
    """Test recursive transitive join lookup in translator."""
    sources_map = {
        "orders": MalloySourceInfo(name="orders", joined_sources={"users"}),
        "users": MalloySourceInfo(name="users", joined_sources={"locations"}),
        "locations": MalloySourceInfo(name="locations"),
    }
    all_joins = _get_all_joined_sources("orders", sources_map)
    assert all_joins == {"users", "locations"}

    # Test translator get_deps with transitive joins
    translator = MalloyTranslator()
    parsed_model = MalloyParsedModel(file_path=Path("test.malloy"), sources=sources_map)
    q_info = MalloyQueryInfo(name="order_report", source_name="orders")
    t_data = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed_model,
        file_path=Path("test.malloy"),
        include_sources=True,
    )
    deps = list(translator.get_deps(t_data))
    assert AssetKey(["test", "orders"]) in deps
    assert AssetKey(["test", "users"]) in deps
    assert AssetKey(["test", "locations"]) in deps


def test_table_to_asset_key_consistency():
    """Test _table_to_asset_key consistency for raw table dependencies."""
    translator = MalloyTranslator()
    parsed_model = MalloyParsedModel(
        file_path=Path("test.malloy"),
        table_dependencies={"data/orders.parquet", "schema.customers"},
    )
    q_info = MalloyQueryInfo(name="raw_query")
    t_data = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed_model,
        file_path=Path("test.malloy"),
        table_dependencies=parsed_model.table_dependencies,
        include_sources=False,
    )
    deps = list(translator.get_deps(t_data))
    assert AssetKey(["data", "orders"]) in deps
    assert AssetKey(["schema", "customers"]) in deps


def test_in_memory_manifest_dict(tmp_path):
    """Test loading assets and checks via in-memory manifest_dict when Node is unavailable."""
    malloy_file = tmp_path / "sales.malloy"
    malloy_file.write_text("source: orders is duckdb.table('orders.parquet') { query: total_sales is { aggregate: total is count() } }")

    manifest_dict = {
        "models": {
            str(malloy_file.resolve()): {
                "sources": {
                    "orders": {
                        "name": "orders",
                        "connection": "duckdb",
                        "table_or_sql": "orders.parquet",
                    }
                },
                "queries": {
                    "total_sales": {
                        "name": "total_sales",
                        "source_name": "orders",
                        "line_number": 1,
                        "raw_code": "query: total_sales is { aggregate: total is count() }",
                    },
                    "check_valid_orders": {
                        "name": "check_valid_orders",
                        "source_name": "orders",
                        "is_check": True,
                        "line_number": 1,
                    },
                },
                "imports": [],
                "table_dependencies": ["orders.parquet"],
            }
        }
    }

    # Verify MalloyProject uses manifest_dict
    proj = MalloyProject(path=malloy_file, manifest_dict=manifest_dict)
    assert not proj.is_stale
    assert proj.load_manifest() == manifest_dict

    # Verify load_malloy_assets works with manifest_dict even if node is missing
    with patch("shutil.which", return_value=None):
        assets_def = load_malloy_assets(path=malloy_file, manifest_dict=manifest_dict)
        assert assets_def is not None
        assert len(assets_def.keys) > 0

        checks = build_malloy_asset_checks(
            file_path=malloy_file,
            target_asset_key=AssetKey(["sales", "total_sales"]),
            manifest_dict=manifest_dict,
        )
        assert len(checks) == 1


def test_cli_client_parse_ast_batch(tmp_path):
    """Test parse_ast_batch method of MalloyCliClient if node is available."""
    f1 = tmp_path / "m1.malloy"
    f2 = tmp_path / "m2.malloy"
    f1.write_text("source: s1 is duckdb.table('t1')")
    f2.write_text("source: s2 is duckdb.table('t2')")

    client = MalloyCliClient()
    try:
        results = client.parse_ast_batch([f1, f2])
        assert isinstance(results, dict)
        assert str(f1.resolve()) in results or str(f1) in results
    except Exception as e:
        assert "Node.js" in str(e) or "package" in str(e) or "@malloydata" in str(e)
