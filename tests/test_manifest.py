"""Tests for manifest build/load, dialect resilience, CLI commands, and degraded mode."""

import json
from pathlib import Path

import pytest
from dagster import AssetsDefinition

from dagster_malloy import (
    MalloyCliClient,
    MalloyEnvironmentError,
    MalloyParser,
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
