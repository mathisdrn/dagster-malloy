"""Tests for manifest build/load, dialect resilience, CLI commands, and degraded mode."""

import json
import time
from pathlib import Path

import pytest
from dagster import AssetKey, AssetsDefinition

from dagster_malloy import (
    MalloyCliClient,
    MalloyEnvironmentError,
    MalloyParser,
    build_malloy_asset_checks,
    load_malloy_assets,
)
from dagster_malloy.cli import build_manifest_command, main


def test_build_and_load_manifest(tmp_path: Path):
    model_content = """
    source: users is duckdb.table('users.parquet') extend {
      dimension: full_name is concat(first_name, ' ', last_name)
    }

    # @dashboard
    # Total Users Count
    query: user_count is users -> {
      aggregate: total_users is count()
    }
    """
    model_file = tmp_path / "analytics" / "users.malloy"
    model_file.parent.mkdir(parents=True, exist_ok=True)
    model_file.write_text(model_content, encoding="utf-8")

    manifest_path = tmp_path / "analytics" / "malloy_manifest.json"

    # Build manifest using MalloyParser
    out_path = MalloyParser.build_manifest(
        target_path=model_file.parent,
        output_path=manifest_path,
    )
    assert out_path.exists()

    with open(out_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "version" in data
    assert "models" in data
    assert "users.malloy" in data["models"]

    # Load assets using manifest
    assets = load_malloy_assets(
        path=model_file.parent,
        manifest_path=manifest_path,
    )
    assert isinstance(assets, AssetsDefinition)
    keys = [spec.key.path[-1] for spec in assets.specs]
    assert "users" in keys
    assert "user_count" in keys
    assert "user_count_dashboard" in keys


def test_manifest_autodiscovery(tmp_path: Path):
    model_content = """
    source: orders is duckdb.table('orders.parquet')
    query: order_stats is orders -> { aggregate: total is count() }
    """
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "orders.malloy"
    model_file.write_text(model_content, encoding="utf-8")

    manifest_file = MalloyParser.build_manifest(target_path=model_dir)
    assert manifest_file == model_dir / "malloy_manifest.json"

    # Auto-discovery with use_manifest_if_exists=True
    assets = load_malloy_assets(path=model_dir, use_manifest_if_exists=True)
    assert isinstance(assets, AssetsDefinition)


def test_cli_build_manifest_command(tmp_path: Path, monkeypatch):
    model_content = "source: items is duckdb.table('items.parquet')"
    model_dir = tmp_path / "cli_models"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "items.malloy").write_text(model_content, encoding="utf-8")

    output_manifest = model_dir / "custom_manifest.json"

    class DummyArgs:
        path = str(model_dir)
        output = str(output_manifest)

    res = build_manifest_command(DummyArgs())
    assert res == 0
    assert output_manifest.exists()


def test_resilient_custom_connection_dialect(tmp_path: Path):
    # Model using a custom connection name 'orca'
    model_content = """
    source: custom_data is orca.table('custom_table') extend {
      dimension: val_str is cast(val as string)
    }

    query: get_custom is custom_data -> { aggregate: c is count() }
    """
    model_file = tmp_path / "custom_conn.malloy"
    model_file.write_text(model_content, encoding="utf-8")

    # Parsing should not fail with 'Unknown Dialect orca'
    parsed = MalloyParser.parse_file(model_file)
    assert "custom_data" in parsed.sources
    assert parsed.sources["custom_data"].connection == "orca"


def test_degraded_mode_without_node(tmp_path: Path, monkeypatch):
    model_file = tmp_path / "no_node.malloy"
    model_file.write_text("source: s is duckdb.table('t')", encoding="utf-8")

    # Simulate node missing from PATH
    monkeypatch.setattr("shutil.which", lambda cmd: None if cmd == "node" else "/usr/bin/" + cmd)

    # Calling parse_ast should raise MalloyEnvironmentError
    client = MalloyCliClient()
    with pytest.raises(MalloyEnvironmentError) as exc_info:
        client.parse_ast(model_file)

    assert "Node.js was not found in PATH" in str(exc_info.value)
    assert "dagster-malloy build-manifest" in str(exc_info.value)


def test_auto_recompile_if_manifest_missing(tmp_path: Path):
    model_dir = tmp_path / "auto_create"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "sales.malloy"
    model_file.write_text(
        """
        source: sales is duckdb.table('sales.parquet')
        query: total_sales is sales -> { aggregate: amt is sum(amount) }
        """,
        encoding="utf-8",
    )

    manifest_file = model_dir / "malloy_manifest.json"
    assert not manifest_file.exists()

    # Calling load_malloy_assets with auto_recompile_if_stale=True should auto-build manifest
    assets = load_malloy_assets(path=model_dir, auto_recompile_if_stale=True)
    assert manifest_file.exists()
    keys = [spec.key.path[-1] for spec in assets.specs]
    assert "sales" in keys
    assert "total_sales" in keys


def test_auto_recompile_if_stale(tmp_path: Path):
    model_dir = tmp_path / "stale_test"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "products.malloy"
    model_file.write_text(
        """
        source: products is duckdb.table('products.parquet')
        query: product_count is products -> { aggregate: c is count() }
        """,
        encoding="utf-8",
    )

    # Build initial manifest
    manifest_file = MalloyParser.build_manifest(target_path=model_dir)
    assert manifest_file.exists()

    # Sleep slightly to ensure filesystem mtime difference
    time.sleep(0.1)

    # Edit file to add a new query
    model_file.write_text(
        """
        source: products is duckdb.table('products.parquet')
        query: product_count is products -> { aggregate: c is count() }
        query: product_categories is products -> { group_by: category }
        """,
        encoding="utf-8",
    )

    # Load assets with auto_recompile_if_stale=True
    assets = load_malloy_assets(path=model_dir, auto_recompile_if_stale=True)
    keys = [spec.key.path[-1] for spec in assets.specs]
    assert "product_count" in keys
    assert "product_categories" in keys


def test_auto_recompile_if_stale_disabled(tmp_path: Path):
    model_dir = tmp_path / "stale_disabled_test"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "events.malloy"
    model_file.write_text(
        """
        source: events is duckdb.table('events.parquet')
        query: event_count is events -> { aggregate: c is count() }
        """,
        encoding="utf-8",
    )

    manifest_file = MalloyParser.build_manifest(target_path=model_dir)

    time.sleep(0.1)

    # Edit file to add a new query
    model_file.write_text(
        """
        source: events is duckdb.table('events.parquet')
        query: event_count is events -> { aggregate: c is count() }
        query: new_event_query is events -> { aggregate: c is count() }
        """,
        encoding="utf-8",
    )

    # Load assets with auto_recompile_if_stale=False
    assets = load_malloy_assets(path=model_dir, auto_recompile_if_stale=False)
    keys = [spec.key.path[-1] for spec in assets.specs]
    assert "event_count" in keys
    assert "new_event_query" not in keys


def test_auto_recompile_zero_node_fast_path(tmp_path: Path, monkeypatch):
    model_dir = tmp_path / "zero_node"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "metrics.malloy"
    model_file.write_text(
        """
        source: metrics is duckdb.table('metrics.parquet')
        query: metric_sum is metrics -> { aggregate: s is sum(val) }
        """,
        encoding="utf-8",
    )

    # Pre-build manifest
    MalloyParser.build_manifest(target_path=model_dir)

    # Disable Node in PATH
    monkeypatch.setattr("shutil.which", lambda cmd: None if cmd == "node" else "/usr/bin/" + cmd)

    # Even with auto_recompile_if_stale=True, missing node skips re-compile and uses manifest directly
    assets = load_malloy_assets(path=model_dir, auto_recompile_if_stale=True)
    keys = [spec.key.path[-1] for spec in assets.specs]
    assert "metric_sum" in keys


def test_asset_checks_auto_recompile(tmp_path: Path):
    check_file = tmp_path / "checks_auto.malloy"
    check_file.write_text(
        """
        source: users is duckdb.table('users.parquet')

        # @check
        query: check_valid_ids is users -> {
          where: id is null
          aggregate: invalid_count is count()
        }
        """,
        encoding="utf-8",
    )

    manifest_file = tmp_path / "malloy_manifest.json"
    assert not manifest_file.exists()

    checks = build_malloy_asset_checks(
        file_path=check_file,
        target_asset_key=AssetKey("users"),
        auto_recompile_if_stale=True,
    )
    assert len(checks) == 1
    assert manifest_file.exists()

