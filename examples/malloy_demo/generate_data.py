"""Generate sample DuckDB Parquet datasets for the Malloy demo project."""

from pathlib import Path
import duckdb

def main():
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Customers dataset
    duckdb.sql("""
    SELECT 1 as id, 'Alice Smith' as name, 'Enterprise' as segment, 'CA' as state UNION ALL
    SELECT 2, 'Bob Jones', 'SMB', 'NY' UNION ALL
    SELECT 3, 'Charlie Brown', 'Consumer', 'TX' UNION ALL
    SELECT 4, 'Diana Prince', 'Enterprise', 'CA' UNION ALL
    SELECT 5, 'Evan Wright', 'SMB', 'WA'
    """).write_parquet(str(data_dir / "customers.parquet"))

    # Products dataset
    duckdb.sql("""
    SELECT 101 as id, 'Laptop Pro 15' as title, 'Electronics' as category, 800.0 as base_cost UNION ALL
    SELECT 102, 'Ergonomic Chair' as title, 'Furniture' as category, 150.0 as base_cost UNION ALL
    SELECT 103, 'Wireless Mouse' as title, 'Electronics' as category, 25.0 as base_cost UNION ALL
    SELECT 104, '4K Monitor' as title, 'Electronics' as category, 300.0 as base_cost UNION ALL
    SELECT 105, 'Standing Desk' as title, 'Furniture' as category, 450.0 as base_cost
    """).write_parquet(str(data_dir / "products.parquet"))

    # Orders dataset
    duckdb.sql("""
    SELECT 1 as id, 1 as customer_id, 101 as product_id, 1200.0 as price, 2 as quantity, '2026-08-01' as order_date UNION ALL
    SELECT 2, 2, 102, 200.0, 1, '2026-08-02' UNION ALL
    SELECT 3, 3, 103, 35.0, 4, '2026-08-02' UNION ALL
    SELECT 4, 1, 104, 350.0, 2, '2026-08-03' UNION ALL
    SELECT 5, 4, 105, 550.0, 1, '2026-08-04' UNION ALL
    SELECT 6, 5, 101, 1200.0, 1, '2026-08-05' UNION ALL
    SELECT 7, 2, 103, 35.0, 10, '2026-08-05'
    """).write_parquet(str(data_dir / "orders.parquet"))

    print(f"Generated sample datasets in {data_dir}")

if __name__ == "__main__":
    main()
