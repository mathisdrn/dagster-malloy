"""Tests for MalloyProject class."""

from pathlib import Path
from unittest.mock import patch

from dagster_malloy.project import MalloyProject
from dagster_malloy.asset_decorator import load_malloy_assets


def test_malloy_project_init(tmp_path: Path):
    malloy_file = tmp_path / "orders.malloy"
    malloy_file.write_text("source: orders is duckdb.table('orders.parquet')\nquery: q is orders -> { select: * }\n")

    project = MalloyProject(path=malloy_file)

    assert project.path_obj == malloy_file.resolve()
    assert project.root_dir == tmp_path.resolve()
    assert project.manifest_file == tmp_path / "malloy_manifest.json"
    assert project.is_stale is True

    files = project.get_malloy_files()
    assert len(files) == 1
    assert files[0] == malloy_file.resolve()


def test_malloy_project_passed_to_load_assets(tmp_path: Path):
    malloy_file = tmp_path / "sales.malloy"
    malloy_file.write_text(
        """
source: orders is duckdb.table('orders.parquet')
query: total_sales is orders -> { aggregate: total is count() }
""",
        encoding="utf-8",
    )

    project = MalloyProject(path=tmp_path)
    assets_def = load_malloy_assets(project=project)

    assert assets_def is not None
    assert len(assets_def.keys) == 2  # orders, total_sales
