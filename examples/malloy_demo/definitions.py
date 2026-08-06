"""Dagster definitions for Malloy demo project."""

from pathlib import Path
from dagster import AssetKey, Definitions, MaterializeResult, asset
import duckdb
import pandas as pd
from dagster_malloy import (
    MalloyResource,
    build_malloy_asset_checks,
    load_malloy_assets,
)

project_dir = Path(__file__).parent
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
    df = pd.DataFrame([
        {"id": 1, "name": "Alice Smith", "segment": "Enterprise", "state": "CA"},
        {"id": 2, "name": "Bob Jones", "segment": "SMB", "state": "NY"},
        {"id": 3, "name": "Charlie Brown", "segment": "Consumer", "state": "TX"},
        {"id": 4, "name": "Diana Prince", "segment": "Enterprise", "state": "CA"},
        {"id": 5, "name": "Evan Wright", "segment": "SMB", "state": "WA"},
    ])
    df.to_parquet(parquet_path)
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
    df = pd.DataFrame([
        {"id": 101, "title": "Laptop Pro 15", "category": "Electronics", "base_cost": 800.0},
        {"id": 102, "title": "Ergonomic Chair", "category": "Furniture", "base_cost": 150.0},
        {"id": 103, "title": "Wireless Mouse", "category": "Electronics", "base_cost": 25.0},
        {"id": 104, "title": "4K Monitor", "category": "Electronics", "base_cost": 300.0},
        {"id": 105, "title": "Standing Desk", "category": "Furniture", "base_cost": 450.0},
    ])
    df.to_parquet(parquet_path)
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
    df = pd.DataFrame([
        {"id": 1, "customer_id": 1, "product_id": 101, "price": 1200.0, "quantity": 2, "order_date": "2026-08-01"},
        {"id": 2, "customer_id": 2, "product_id": 102, "price": 200.0, "quantity": 1, "order_date": "2026-08-02"},
        {"id": 3, "customer_id": 3, "product_id": 103, "price": 35.0, "quantity": 4, "order_date": "2026-08-02"},
        {"id": 4, "customer_id": 1, "product_id": 104, "price": 350.0, "quantity": 2, "order_date": "2026-08-03"},
        {"id": 5, "customer_id": 4, "product_id": 105, "price": 550.0, "quantity": 1, "order_date": "2026-08-04"},
        {"id": 6, "customer_id": 5, "product_id": 101, "price": 1200.0, "quantity": 1, "order_date": "2026-08-05"},
        {"id": 7, "customer_id": 2, "product_id": 103, "price": 35.0, "quantity": 10, "order_date": "2026-08-05"},
    ])
    df.to_parquet(parquet_path)
    return MaterializeResult(metadata={"row_count": len(df), "path": str(parquet_path)})

# Automatically load Malloy assets & dashboard nodes
malloy_assets = load_malloy_assets(path=models_dir, create_dashboards=True)

# Downstream Python Asset consuming data produced by Malloy asset

@asset(
    deps=[AssetKey(["sales", "customer_analytics"])],
    group_name="export",
    description="Downstream Python asset that consumes Malloy-computed customer analytics data.",
)
def vip_customer_digest() -> MaterializeResult:
    df = pd.read_parquet(data_dir / "orders.parquet")
    vip_count = len(df[df["price"] > 500])
    return MaterializeResult(
        metadata={
            "vip_count": vip_count,
            "status": "VIP Customer Digest generated",
        }
    )

# Build asset checks from Malloy test queries
cust_analytics_key = AssetKey(["sales", "customer_analytics"])
malloy_checks = build_malloy_asset_checks(
    file_path=models_dir / "sales.malloy",
    target_asset_key=cust_analytics_key,
)

defs = Definitions(
    assets=[
        raw_customers_ingestion,
        raw_products_ingestion,
        raw_orders_ingestion,
        malloy_assets,
        vip_customer_digest,
    ],
    asset_checks=malloy_checks,
    resources={
        "malloy": MalloyResource(
            execution_mode="auto",
            home_dir=str(project_dir),
        ),
    },
)
