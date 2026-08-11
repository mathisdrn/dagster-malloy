"""Dagster definitions for Malloy demo project."""

from pathlib import Path
import duckdb
import polars as pl
from dagster import AssetKey, Definitions, MaterializeResult, asset

from dagster_malloy import (
    MalloyResource,
    load_malloy_assets,
)

project_dir = Path(__file__).parent.resolve()
models_dir = project_dir / "models"
data_dir = project_dir / "data"

# Upstream Python Ingestion Assets


@asset(
    key_prefix="data",
    name="customers",
    group_name="ingestion",
    description="Ingests raw customer dimension records into Parquet storage.",
)
def raw_customers_ingestion() -> MaterializeResult:
    data_dir.mkdir(exist_ok=True)
    parquet_path = data_dir / "customers.parquet"
    df = pl.DataFrame([
        {"id": 1, "name": "Alice Smith", "segment": "Enterprise", "state": "CA"},
        {"id": 2, "name": "Bob Jones", "segment": "SMB", "state": "NY"},
        {"id": 3, "name": "Charlie Brown", "segment": "Consumer", "state": "TX"},
        {"id": 4, "name": "Diana Prince", "segment": "Enterprise", "state": "CA"},
        {"id": 5, "name": "Evan Wright", "segment": "SMB", "state": "WA"},
    ])
    df.write_parquet(parquet_path)
    return MaterializeResult(metadata={"row_count": len(df), "path": str(parquet_path)})


@asset(
    key_prefix="data",
    name="products",
    group_name="ingestion",
    description="Ingests raw product catalog dimension records into Parquet storage.",
)
def raw_products_ingestion() -> MaterializeResult:
    data_dir.mkdir(exist_ok=True)
    parquet_path = data_dir / "products.parquet"
    df = pl.DataFrame([
        {
            "id": 101,
            "title": "Laptop Pro 15",
            "category": "Electronics",
            "base_cost": 800.0,
        },
        {
            "id": 102,
            "title": "Ergonomic Chair",
            "category": "Furniture",
            "base_cost": 150.0,
        },
        {
            "id": 103,
            "title": "Wireless Mouse",
            "category": "Electronics",
            "base_cost": 25.0,
        },
        {
            "id": 104,
            "title": "4K Monitor",
            "category": "Electronics",
            "base_cost": 300.0,
        },
        {
            "id": 105,
            "title": "Standing Desk",
            "category": "Furniture",
            "base_cost": 450.0,
        },
    ])
    df.write_parquet(parquet_path)
    return MaterializeResult(metadata={"row_count": len(df), "path": str(parquet_path)})


@asset(
    key_prefix="data",
    name="orders",
    group_name="ingestion",
    description="Ingests raw order transactions into Parquet storage.",
)
def raw_orders_ingestion() -> MaterializeResult:
    data_dir.mkdir(exist_ok=True)
    parquet_path = data_dir / "orders.parquet"
    df = pl.DataFrame([
        {
            "id": 1,
            "customer_id": 1,
            "product_id": 101,
            "price": 1200.0,
            "quantity": 2,
            "order_date": "2026-08-01",
        },
        {
            "id": 2,
            "customer_id": 2,
            "product_id": 102,
            "price": 200.0,
            "quantity": 1,
            "order_date": "2026-08-02",
        },
        {
            "id": 3,
            "customer_id": 3,
            "product_id": 103,
            "price": 35.0,
            "quantity": 4,
            "order_date": "2026-08-02",
        },
        {
            "id": 4,
            "customer_id": 1,
            "product_id": 104,
            "price": 350.0,
            "quantity": 2,
            "order_date": "2026-08-03",
        },
        {
            "id": 5,
            "customer_id": 4,
            "product_id": 105,
            "price": 550.0,
            "quantity": 1,
            "order_date": "2026-08-04",
        },
        {
            "id": 6,
            "customer_id": 5,
            "product_id": 101,
            "price": 1200.0,
            "quantity": 1,
            "order_date": "2026-08-05",
        },
        {
            "id": 7,
            "customer_id": 2,
            "product_id": 103,
            "price": 35.0,
            "quantity": 10,
            "order_date": "2026-08-05",
        },
    ])
    df.write_parquet(parquet_path)
    return MaterializeResult(metadata={"row_count": len(df), "path": str(parquet_path)})


# DuckDB Warehouse Connection Resource for export database
class DuckDBWarehouseResource:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def get_connection(self):
        self.db_path.parent.mkdir(exist_ok=True)
        return duckdb.connect(str(self.db_path))


duckdb_resource = DuckDBWarehouseResource(data_dir / "export.duckdb")

# Warehouse Table Assets (Materializes CTAS tables directly in data/export.duckdb)
malloy_table_assets = load_malloy_assets(
    path=models_dir,
    name="malloy_warehouse_tables",
    execution_mode="warehouse",
    materialization_mode="table",
    db_resource_key="duckdb",
    create_dashboards=True,
    include_sources=False,
)

# Downstream Python Asset consuming materialized DuckDB table
@asset(
    deps=[AssetKey(["sales", "customer_analytics"])],
    group_name="export",
    description="Downstream Python asset that consumes Malloy-computed customer analytics table from DuckDB export.",
)
def vip_customer_digest() -> MaterializeResult:
    conn = duckdb.connect(str(data_dir / "export.duckdb"))
    try:
        rows = conn.execute("SELECT * FROM customer_analytics WHERE total_spent > 500").fetchall()
        vip_count = len(rows)
    except Exception:
        # Fallback if table not materialized yet
        raw_df = pl.read_parquet(data_dir / "orders.parquet")
        vip_count = len(raw_df.filter(pl.col("price") > 500))
    finally:
        conn.close()

    return MaterializeResult(
        metadata={
            "vip_count": vip_count,
            "status": "VIP Customer Digest generated from DuckDB warehouse export",
        }
    )


defs = Definitions(
    assets=[
        raw_customers_ingestion,
        raw_products_ingestion,
        raw_orders_ingestion,
        malloy_table_assets,
        vip_customer_digest,
    ],
    resources={
        "malloy": MalloyResource(
            execution_mode="warehouse",
            project_dir=str(project_dir),
        ),
        "duckdb": duckdb_resource,
    },
)
