"""CLI entrypoint for dagster-malloy providing demo launcher commands."""

import argparse
import shutil
import subprocess
from pathlib import Path

# Bundled demo project files live alongside this package
DEMO_SRC = Path(__file__).parent


def generate_demo_project(target_dir: Path):
    """Copies the bundled Malloy demo project into target_dir."""
    target_dir.mkdir(parents=True, exist_ok=True)

    # Copy models/
    src_models = DEMO_SRC / "models"
    dst_models = target_dir / "models"
    if not dst_models.exists():
        shutil.copytree(src_models, dst_models)

    # Copy definitions.py
    dst_defs = target_dir / "definitions.py"
    if not dst_defs.exists():
        shutil.copy2(DEMO_SRC / "definitions.py", dst_defs)

    # Copy generate_data.py
    dst_gen = target_dir / "generate_data.py"
    if not dst_gen.exists():
        shutil.copy2(DEMO_SRC / "generate_data.py", dst_gen)

    # Generate parquet data files
    data_dir = target_dir / "data"
    data_dir.mkdir(exist_ok=True)
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
    except Exception:  # noqa: BLE001 - best-effort demo data generation; failure is non-fatal
        pass


def run_demo():
    """CLI launcher for running the dagster-malloy demo server directly via uvx / python -m."""
    parser = argparse.ArgumentParser(
        description="Launch local Dagster webserver with Malloy demo assets."
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=3000,
        help="Port for Dagster webserver (default: 3000)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--dir",
        "-d",
        type=str,
        default="malloy_demo",
        help="Target demo directory name (default: malloy_demo)",
    )

    args = parser.parse_args()

    demo_dir = Path(args.dir).resolve()
    if not (demo_dir / "definitions.py").exists():
        print(f"Generating Malloy demo project in {demo_dir}...")
        generate_demo_project(demo_dir)

    defs_file = demo_dir / "definitions.py"

    print("=" * 60)
    print(f" Malloy Demo Project ready at: {demo_dir}")
    print(" You can open and edit the following files:")
    print(f"   • Model:       {demo_dir / 'models' / 'sales.malloy'}")
    print(f"   • Definitions: {defs_file}")
    print("=" * 60)

    print(
        f"\nStarting Dagster webserver serving Malloy assets at http://{args.host}:{args.port}...\n"
    )

    dg_bin = shutil.which("dg") or "dg"

    try:
        subprocess.run(
            [
                dg_bin,
                "dev",
                "--python-file",
                str(defs_file),
                "--port",
                str(args.port),
                "--host",
                args.host,
            ],
            cwd=str(demo_dir),
            check=True,
        )
    except KeyboardInterrupt:
        print("\nDemo webserver stopped.")


if __name__ == "__main__":
    run_demo()
