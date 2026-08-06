"""CLI entrypoint for dagster-malloy providing demo launcher commands."""

import argparse
from pathlib import Path
import sys
import subprocess
from typing import Optional


DEMO_MALLOY_MODEL = """# Malloy E-Commerce Analytics Model

source: customers is duckdb.table('data/customers.parquet') extend {
  primary_key: id
  dimension: full_name is name
}

source: products is duckdb.table('data/products.parquet') extend {
  primary_key: id
  dimension: item_title is title
}

source: orders is duckdb.table('data/orders.parquet') extend {
  primary_key: id
  join_one: customers on customer_id = customers.id
  join_one: products on product_id = products.id

  dimension: gross_revenue is price * quantity
  dimension: estimated_profit is gross_revenue - (products.base_cost * quantity)

  view: customer_performance is {
    group_by: 
      customers.name
      customers.segment
      customers.state
    aggregate: 
      total_orders is count()
      total_spent is sum(gross_revenue)
      total_profit is sum(estimated_profit)
  }

  view: category_breakdown is {
    group_by: 
      products.category
    aggregate: 
      revenue is sum(gross_revenue)
      order_count is count()
  }
}

# Customer Analytics Data Asset
query: customer_analytics is orders -> customer_performance

# Product Category Performance Data Asset
query: category_analytics is orders -> category_breakdown

# Executive Sales Dashboard Asset
# @dashboard
# @bar_chart
query: executive_sales_dashboard is orders -> {
  group_by: products.category, customers.segment
  aggregate: gross_revenue is sum(gross_revenue)
}

# Data Quality Check: Verify Customer IDs are non-null
# @check
query: check_valid_customer_ids is orders -> {
  where: customer_id is null
  aggregate: invalid_count is count()
}

# Data Quality Check: Verify Product IDs are non-null
# @check
query: check_valid_product_ids is orders -> {
  where: product_id is null
  aggregate: invalid_count is count()
}
"""

DEMO_DEFINITIONS = """\"\"\"Dagster definitions for Malloy demo project.\"\"\"

from pathlib import Path
from dagster import AssetKey, Definitions, MaterializeResult, asset
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
    import pandas as pd
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
    import pandas as pd
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
    import pandas as pd
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
    import pandas as pd
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
"""


def generate_demo_project(target_dir: Path):
    """Generates a self-contained Malloy demo project in target_dir."""
    target_dir.mkdir(parents=True, exist_ok=True)
    models_dir = target_dir / "models"
    data_dir = target_dir / "data"
    models_dir.mkdir(exist_ok=True)
    data_dir.mkdir(exist_ok=True)

    # Write sales.malloy
    (models_dir / "sales.malloy").write_text(DEMO_MALLOY_MODEL, encoding="utf-8")

    # Write definitions.py
    (target_dir / "definitions.py").write_text(DEMO_DEFINITIONS, encoding="utf-8")

    # Generate parquet data files using duckdb or pandas
    try:
        import duckdb
        duckdb.sql("""
        SELECT 1 as id, 'Alice Smith' as name, 'Enterprise' as segment, 'CA' as state UNION ALL
        SELECT 2, 'Bob Jones', 'SMB', 'NY' UNION ALL
        SELECT 3, 'Charlie Brown', 'Consumer', 'TX' UNION ALL
        SELECT 4, 'Diana Prince', 'Enterprise', 'CA' UNION ALL
        SELECT 5, 'Evan Wright', 'SMB', 'WA'
        """).write_parquet(str(data_dir / "customers.parquet"))

        duckdb.sql("""
        SELECT 101 as id, 'Laptop Pro 15' as title, 'Electronics' as category, 800.0 as base_cost UNION ALL
        SELECT 102, 'Ergonomic Chair' as title, 'Furniture' as category, 150.0 as base_cost UNION ALL
        SELECT 103, 'Wireless Mouse' as title, 'Electronics' as category, 25.0 as base_cost UNION ALL
        SELECT 104, '4K Monitor' as title, 'Electronics' as category, 300.0 as base_cost UNION ALL
        SELECT 105, 'Standing Desk' as title, 'Furniture' as category, 450.0 as base_cost
        """).write_parquet(str(data_dir / "products.parquet"))

        duckdb.sql("""
        SELECT 1 as id, 1 as customer_id, 101 as product_id, 1200.0 as price, 2 as quantity, '2026-08-01' as order_date UNION ALL
        SELECT 2, 2, 102, 200.0, 1, '2026-08-02' UNION ALL
        SELECT 3, 3, 103, 35.0, 4, '2026-08-02' UNION ALL
        SELECT 4, 1, 104, 350.0, 2, '2026-08-03' UNION ALL
        SELECT 5, 4, 105, 550.0, 1, '2026-08-04' UNION ALL
        SELECT 6, 5, 101, 1200.0, 1, '2026-08-05' UNION ALL
        SELECT 7, 2, 103, 35.0, 10, '2026-08-05'
        """).write_parquet(str(data_dir / "orders.parquet"))
    except Exception:
        pass


def run_demo():
    """CLI launcher for running the dagster-malloy demo server directly via uvx / python -m."""
    parser = argparse.ArgumentParser(description="Launch local Dagster webserver with Malloy demo assets.")
    parser.add_argument("--port", "-p", type=int, default=3000, help="Port for Dagster webserver (default: 3000)")
    parser.add_argument("--host", "-H", type=str, default="127.0.0.1", help="Host address (default: 127.0.0.1)")
    parser.add_argument("--dir", "-d", type=str, default="malloy_demo", help="Target demo directory name")

    args = parser.parse_args()

    demo_dir = Path(args.dir).resolve()
    if not (demo_dir / "definitions.py").exists():
        print(f"Generating Malloy demo project in {demo_dir}...")
        generate_demo_project(demo_dir)

    defs_file = demo_dir / "definitions.py"

    print("=" * 60)
    print(f" Malloy Demo Project generated at: {demo_dir}")
    print(" You can open and edit the following files:")
    print(f"   • Model:       {demo_dir / 'models' / 'sales.malloy'}")
    print(f"   • Definitions: {demo_dir / 'definitions.py'}")
    print("=" * 60)

    print(f"\nStarting Dagster webserver serving Malloy assets at http://{args.host}:{args.port}...\n")
    try:
        subprocess.run(
            [sys.executable, "-m", "dagster", "dev", "-f", str(defs_file), "-p", str(args.port), "-h", args.host],
            cwd=str(demo_dir),
            check=True,
        )
    except KeyboardInterrupt:
        print("\nDemo webserver stopped.")


def main():
    """Main CLI router."""
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        sys.argv.pop(1)
        run_demo()
    else:
        run_demo()


if __name__ == "__main__":
    main()
