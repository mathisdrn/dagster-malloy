import duckdb
from pathlib import Path
import pytest
from dagster import AssetKey, Definitions, materialize

from dagster_malloy import MalloyResource, load_malloy_assets
from dagster_malloy.resource import generate_ddl


def test_generate_ddl():
    sql = "SELECT 1 as x"
    
    # DuckDB / Snowflake / BigQuery table mode
    ddl_table = generate_ddl(sql, dialect="duckdb", table_name="my_table", mode="table")
    assert ddl_table == "CREATE OR REPLACE TABLE my_table AS\nSELECT 1 as x"
    
    # View mode
    ddl_view = generate_ddl(sql, dialect="snowflake", table_name="my_view", mode="view")
    assert ddl_view == "CREATE OR REPLACE VIEW my_view AS\nSELECT 1 as x"
    
    # Postgres table mode
    ddl_pg = generate_ddl(sql, dialect="postgres", table_name="my_pg_table", mode="table")
    assert ddl_pg == "DROP TABLE IF EXISTS my_pg_table;\nCREATE TABLE my_pg_table AS\nSELECT 1 as x"


def test_compile_ctas(tmp_path: Path):
    model_file = tmp_path / "test.malloy"
    model_file.write_text("""
    source: items is duckdb.sql("SELECT 1 as id, 'widget' as name")
    query: item_summary is items -> { select: id, name }
    """)

    resource = MalloyResource(project_dir=str(tmp_path))
    ddl, sql, dialect = resource.compile_ctas(
        file_path=model_file, query_name="item_summary", target_table="item_summary", mode="table"
    )

    assert "CREATE OR REPLACE TABLE item_summary AS" in ddl
    assert "widget" in sql or "item_summary" in ddl


def test_load_malloy_assets_warehouse_mode(tmp_path: Path):
    db_file = tmp_path / "warehouse.duckdb"

    model_file = tmp_path / "sales.malloy"
    model_file.write_text("""
    source: orders is duckdb.sql("SELECT 1 as id, 'Alice' as customer, 100.0 as amount") extend {
      primary_key: id
      measure: total_amount is sum(amount)
    }
    query: customer_summary is orders -> {
      group_by: customer
      aggregate: total_amount
    }
    """)

    class DuckDBTestResource:
        def get_connection(self):
            return duckdb.connect(str(db_file))

    assets = load_malloy_assets(
        path=model_file,
        execution_mode="warehouse",
        materialization_mode="table",
        db_resource_key="duckdb",
    )

    res = materialize(
        [assets],
        resources={
            "malloy": MalloyResource(project_dir=str(tmp_path)),
            "duckdb": DuckDBTestResource(),
        },
    )

    assert res.success
    
    # Verify table was actually created in DuckDB warehouse
    verify_conn = duckdb.connect(str(db_file))
    result = verify_conn.execute("SELECT * FROM customer_summary").fetchall()
    verify_conn.close()

    assert len(result) == 1
    assert result[0][0] == "Alice"
    assert result[0][1] == 100.0
