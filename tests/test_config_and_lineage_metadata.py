"""Tests for connection dialect resolution, asset kind badging, and lineage metadata enrichment (Issue #1)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetKey, materialize_to_memory

from dagster_malloy import (
    MalloyParsedModel,
    MalloyParser,
    MalloyQueryInfo,
    MalloyResource,
    MalloySourceInfo,
    MalloyTranslator,
    MalloyTranslatorData,
    load_malloy_assets,
)
from dagster_malloy.translator import _load_malloy_config, _resolve_dialect


def test_find_and_load_malloy_config(tmp_path: Path):
    project_dir = tmp_path / "malloy_project"
    project_dir.mkdir()
    config_file = project_dir / "malloy-config.json"
    config_data = {
        "connections": {
            "orca": {
                "is": "duckdb",
                "workingDirectory": "./data",
                "database": "orca_db",
                "schema": "analytics",
                "setupSQL": "INSTALL 'ducklake'; LOAD 'ducklake';",
            },
            "prod_bq": {
                "is": "bigquery",
                "projectId": "my-gcp-project",
                "dataset": "dw",
            },
        }
    }
    config_file.write_text(json.dumps(config_data), encoding="utf-8")

    sub_dir = project_dir / "models" / "nested"
    sub_dir.mkdir(parents=True)
    model_file = sub_dir / "sales.malloy"
    model_file.touch()

    # Auto-discovery by walking up parent directories and loading config
    loaded = _load_malloy_config(start_path=model_file)
    assert "connections" in loaded
    assert "orca" in loaded["connections"]

    # Resolve dialect
    assert _resolve_dialect("orca", config=loaded) == "duckdb"
    assert _resolve_dialect("prod_bq", config=loaded) == "bigquery"


def test_load_malloy_config_list_format():
    config_data = {
        "connections": [
            {
                "name": "orca",
                "is": "duckdb",
                "workingDirectory": "./data",
            },
            {
                "name": "snowflake_dw",
                "is": "snowflake",
                "account": "xy12345",
            },
        ]
    }
    assert _resolve_dialect("orca", config=config_data) == "duckdb"
    assert _resolve_dialect("snowflake_dw", config=config_data) == "snowflake"


def test_connection_dialect_badging_and_metadata_resolution(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    config_file = project_dir / "malloy-config.json"
    config_file.write_text(
        json.dumps(
            {
                "connections": {
                    "orca": {
                        "is": "duckdb",
                        "database": "main",
                        "schema": "raw",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    model_file = project_dir / "stories.malloy"
    model_file.write_text(
        """
source: stories is orca.table('stories') extend {
  primary_key: id
  dimension: author is user_id
}

query: top_stories is stories -> {
  group_by: author
  aggregate: story_count is count()
}
""",
        encoding="utf-8",
    )

    # 1. Parse AST
    parsed = MalloyParser.parse_file(model_file)
    assert "stories" in parsed.sources
    assert parsed.sources["stories"].connection == "orca"
    assert parsed.sources["stories"].table_or_sql == "stories"

    # 2. Load Assets
    assets_def = load_malloy_assets(
        path=model_file,
        include_sources=True,
        config_path=config_file,
    )

    specs_by_key = {spec.key: spec for spec in assets_def.specs}
    source_key = AssetKey(["stories", "stories"])
    query_key = AssetKey(["stories", "top_stories"])

    assert source_key in specs_by_key
    assert query_key in specs_by_key

    source_spec = specs_by_key[source_key]
    query_spec = specs_by_key[query_key]

    # Issue 1: Connection identifier "orca" must NOT be in kinds.
    # Dialect "duckdb" MUST be in kinds.
    assert "orca" not in source_spec.kinds
    assert "duckdb" in source_spec.kinds
    assert "semantic_model" in source_spec.kinds
    assert "malloy" in source_spec.kinds
    assert source_spec.kinds == {"semantic_model", "malloy", "duckdb"}

    assert "orca" not in query_spec.kinds
    assert "duckdb" in query_spec.kinds
    assert "⚙️\N{NO-BREAK SPACE}Query" in query_spec.kinds
    assert "malloy" in query_spec.kinds
    assert query_spec.kinds == {"⚙️\N{NO-BREAK SPACE}Query", "malloy", "duckdb"}

    # Issue 2: Metadata enrichment
    assert source_spec.metadata["malloy/connection"] == "orca"
    assert source_spec.metadata["malloy/dialect"] == "duckdb"
    assert source_spec.metadata["dagster/table_name"] == "stories"
    assert source_spec.metadata["dagster/storage_kind"] == "duckdb"
    assert source_spec.metadata["malloy/database"] == "main"
    assert source_spec.metadata["malloy/schema"] == "raw"

    assert query_spec.metadata["malloy/connection"] == "orca"
    assert query_spec.metadata["malloy/dialect"] == "duckdb"
    assert query_spec.metadata["dagster/table_name"] == "stories"
    assert query_spec.metadata["dagster/storage_kind"] == "duckdb"


def test_unresolved_custom_connection_does_not_leak_into_kinds():
    file_path = Path("custom.malloy")
    parsed = MalloyParsedModel(
        file_path=file_path,
        sources={
            "my_source": MalloySourceInfo(
                name="my_source",
                connection="my_custom_unresolved_conn",
                table_or_sql="raw_data",
            )
        },
        queries={
            "my_query": MalloyQueryInfo(
                name="my_query",
                source_name="my_source",
            )
        },
    )

    translator = MalloyTranslator()

    # Source spec
    source_spec = translator.get_source_asset_spec("my_source", file_path, parsed)
    assert source_spec.kinds == {"semantic_model", "malloy"}
    assert "my_custom_unresolved_conn" not in source_spec.kinds
    assert source_spec.metadata["malloy/connection"] == "my_custom_unresolved_conn"
    assert source_spec.metadata["dagster/table_name"] == "raw_data"

    # Query spec
    q_info = parsed.queries["my_query"]
    trans_data = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed,
        file_path=file_path,
    )
    query_spec = translator.get_asset_spec(trans_data)
    assert query_spec.kinds == {"⚙️\N{NO-BREAK SPACE}Query", "malloy"}
    assert "my_custom_unresolved_conn" not in query_spec.kinds
    assert query_spec.metadata["malloy/connection"] == "my_custom_unresolved_conn"
    assert query_spec.metadata["dagster/table_name"] == "raw_data"


def test_fallback_to_db_resource_key():
    file_path = Path("warehouse.malloy")
    parsed = MalloyParsedModel(
        file_path=file_path,
        sources={
            "events": MalloySourceInfo(
                name="events",
                connection="my_unknown_conn",
                table_or_sql="events_tbl",
            )
        },
        queries={
            "event_summary": MalloyQueryInfo(
                name="event_summary",
                source_name="events",
            )
        },
    )

    translator = MalloyTranslator()
    source_spec = translator.get_source_asset_spec(
        "events", file_path, parsed, db_resource_key="duckdb"
    )
    assert "duckdb" in source_spec.kinds
    assert "my_unknown_conn" not in source_spec.kinds
    assert source_spec.metadata["malloy/dialect"] == "duckdb"
    assert source_spec.metadata["dagster/storage_kind"] == "duckdb"

    trans_data = MalloyTranslatorData(
        query_info=parsed.queries["event_summary"],
        parsed_model=parsed,
        file_path=file_path,
        db_resource_key="duckdb",
    )
    query_spec = translator.get_asset_spec(trans_data)
    assert "duckdb" in query_spec.kinds
    assert query_spec.metadata["malloy/dialect"] == "duckdb"


@patch("dagster_malloy.resource.MalloyCliClient")
def test_materialization_metadata_enrichment(mock_cli_cls, tmp_path: Path):
    config_file = tmp_path / "malloy-config.json"
    config_file.write_text(
        json.dumps({"connections": {"orca": {"is": "duckdb", "schema": "analytics"}}}),
        encoding="utf-8",
    )

    malloy_file = tmp_path / "sales.malloy"
    malloy_file.write_text(
        """
source: items is orca.table('data/items.parquet') extend {
  primary_key: id
}

query: item_summary is items -> {
  aggregate: total is count()
}
""",
        encoding="utf-8",
    )

    mock_cli = MagicMock()
    mock_cli.compile.return_value = ("SELECT count(*) FROM items", "duckdb")
    mock_cli.run.return_value = pl.DataFrame([{"total": 100}])
    mock_cli_cls.return_value = mock_cli

    assets_def = load_malloy_assets(
        path=tmp_path,
        include_sources=True,
        config_path=config_file,
    )
    resource = MalloyResource(execution_mode="cli")

    result = materialize_to_memory([assets_def], resources={"malloy": resource})
    assert result.success

    events = result.get_asset_materialization_events()
    assert len(events) == 2

    # Find source and query events
    source_event = next(
        e for e in events if e.asset_key == AssetKey(["sales", "items"])
    )
    query_event = next(
        e for e in events if e.asset_key == AssetKey(["sales", "item_summary"])
    )

    s_meta = source_event.materialization.metadata
    assert s_meta["malloy/connection"].value == "orca"
    assert s_meta["malloy/dialect"].value == "duckdb"
    assert s_meta["dagster/table_name"].value == "data/items.parquet"
    assert s_meta["dagster/storage_kind"].value == "duckdb"
    assert s_meta["malloy/schema"].value == "analytics"

    q_meta = query_event.materialization.metadata
    assert q_meta["malloy/connection"].value == "orca"
    assert q_meta["malloy/dialect"].value == "duckdb"
    assert q_meta["dagster/storage_kind"].value == "duckdb"
    assert q_meta["dagster/row_count"].value == 1
