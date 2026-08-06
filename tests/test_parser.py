"""Unit tests for MalloyParser."""

from pathlib import Path

import pytest

from dagster_malloy.parser import MalloyParser


@pytest.fixture
def sample_malloy_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "model.malloy"
    file_path.write_text(
        """
import "base.malloy"

source: users is duckdb.table('data/users.parquet') extend {
  primary_key: id
  dimension: user_name is name
}

# Description for user_summary query
# @tag_analytics
query: user_summary is users -> {
  group_by: user_name
  aggregate: user_count is count()
}

# @check
query: check_valid_users is users -> {
  where: id is null
  aggregate: invalid_count is count()
}
""",
        encoding="utf-8",
    )
    return file_path


@pytest.fixture
def multi_model_malloy_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "complex_sales.malloy"
    file_path.write_text(
        """
import "common_dimensions.malloy"

source: users is duckdb.table('data/users.parquet') extend {
  primary_key: id
}

source: products is duckdb.table('data/products.parquet') extend {
  primary_key: id
}

source: orders is duckdb.table('data/orders.parquet') extend {
  primary_key: id
  join_one: users on customer_id = users.id
  join_one: products on product_id = products.id
}

source: inventory is duckdb.table('data/inventory.parquet') extend {
  primary_key: id
}

# User demographics query
query: user_analytics is users -> by_state

# Product catalog query
query: product_analytics is products -> by_category

# Order revenue query
query: order_analytics is orders -> revenue_summary

# Executive Dashboard summary
# @dashboard
# @bar_chart
query: executive_dashboard is orders -> {
  group_by: products.category
  aggregate: total_revenue is sum(price * quantity)
}

# Data quality check query
# @check
query: check_valid_orders is orders -> {
  where: id is null
  aggregate: invalid_count is count()
}
""",
        encoding="utf-8",
    )
    return file_path


@pytest.fixture
def sample_notebook_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "notebook.malloynb"
    file_path.write_text(
        """{
  "cells": [
    {
      "cell_type": "code",
      "source": [
        "source: orders is duckdb.table('orders.parquet') extend {\\n",
        "  dimension: order_id is id\\n",
        "}\\n",
        "query: order_summary is orders -> {\\n",
        "  aggregate: total_orders is count()\\n",
        "}\\n"
      ]
    }
  ]
}""",
        encoding="utf-8",
    )
    return file_path


def test_parse_malloy_file(sample_malloy_file: Path):
    parsed = MalloyParser.parse_file(sample_malloy_file)

    assert "users" in parsed.sources
    assert parsed.sources["users"].connection == "duckdb"
    assert parsed.sources["users"].table_or_sql == "data/users.parquet"

    assert "base.malloy" in parsed.imports
    assert "data/users.parquet" in parsed.table_dependencies

    assert "user_summary" in parsed.queries
    q_info = parsed.queries["user_summary"]
    assert q_info.name == "user_summary"
    assert q_info.source_name == "users"
    assert q_info.description == "Description for user_summary query"
    assert "tag_analytics" in q_info.tags
    assert "group_by: user_name" in q_info.raw_code
    assert "aggregate: user_count is count()" in q_info.raw_code

    assert "check_valid_users" in parsed.queries
    check_info = parsed.queries["check_valid_users"]
    assert check_info.is_check is True
    assert "where: id is null" in check_info.raw_code


def test_parse_multiple_sources_and_queries(multi_model_malloy_file: Path):
    parsed = MalloyParser.parse_file(multi_model_malloy_file)

    # 1. Verify sources parsing
    assert len(parsed.sources) == 4
    assert set(parsed.sources.keys()) == {"users", "products", "orders", "inventory"}
    assert parsed.sources["users"].table_or_sql == "data/users.parquet"
    assert parsed.sources["products"].table_or_sql == "data/products.parquet"
    assert parsed.sources["orders"].table_or_sql == "data/orders.parquet"
    assert parsed.sources["inventory"].table_or_sql == "data/inventory.parquet"

    # 2. Verify table dependencies extraction
    assert parsed.table_dependencies == {
        "data/users.parquet",
        "data/products.parquet",
        "data/orders.parquet",
        "data/inventory.parquet",
    }

    # 3. Verify queries parsing
    assert len(parsed.queries) == 5
    assert set(parsed.queries.keys()) == {
        "user_analytics",
        "product_analytics",
        "order_analytics",
        "executive_dashboard",
        "check_valid_orders",
    }

    # 4. Verify dashboard & check annotations
    assert parsed.queries["executive_dashboard"].is_dashboard is True
    assert parsed.queries["check_valid_orders"].is_check is True


def test_parse_notebook(sample_notebook_file: Path):
    parsed = MalloyParser.parse_file(sample_notebook_file)

    assert "orders" in parsed.sources
    assert "order_summary" in parsed.queries
    q_info = parsed.queries["order_summary"]
    assert q_info.is_notebook_cell is True
    assert q_info.cell_index == 0
