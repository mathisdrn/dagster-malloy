# dagster-malloy

`dagster-malloy` is an **unofficial** community integration library providing [Dagster](https://dagster.io) support for [Malloy](https://github.com/malloydata/malloy) models (`.malloy`) and notebooks (`.malloynb`).

## Features

- **Malloy as Dagster assets:** Expose Malloy queries, dashboards and notebooks as Dagster assets including rich metadata (compiled SQL, Malloy source code, column schema, row preview, code references, and execution duration).
- **Complete data lineage:** Automatically resolve Malloy source dependencies — including joined sources — to build a complete asset graph visible in the Dagster UI.
- **Materialization:** Queries are compiled and executed via `malloy-cli` or the `malloy` Python SDK. Results are surfaced as [Apache Arrow](https://arrow.apache.org), enabling zero-copy handoff to downstream assets.
- **Data quality checks:** Write validation queries directly in Malloy and have them run automatically as Dagster [asset checks](https://docs.dagster.io/concepts/assets/asset-checks). Failed checks block downstream materializations, appear in the Dagster UI timeline, and are tracked in the asset health history — without any extra orchestration code.

    ```malloy
    # Verify Customer IDs are non-null
    query: check_valid_customer_ids is orders -> {
      where: customer_id is null
      aggregate: invalid_count is count()
    }
    ```

![Dagster Asset Lineage Graph](examples/malloy_demo/asset_lineage.svg)

## Quickstart

Try `dagster-malloy` using:

```bash
uvx dagster-malloy-demo
```

This generates a sample project (`./malloy_demo`) and launches the Dagster UI at [http://127.0.0.1:3000](http://127.0.0.1:3000).

## Installation

```bash
uv add dagster-malloy
```

To enable DataFrame outputs via pandas:

```bash
uv add "dagster-malloy[pandas]"
```

To enable the in-process Python SDK backend:

```bash
uv add "dagster-malloy[python-backend]"
```

## Usage

### 1. Loading Malloy assets

Use `load_malloy_assets` to discover and construct Dagster assets from `.malloy` files or `.malloynb` notebooks in a directory:

```python
from pathlib import Path
from dagster import Definitions
from dagster_malloy import load_malloy_assets, MalloyResource

malloy_assets = load_malloy_assets(path=Path(__file__).parent / "models")

defs = Definitions(
    assets=[malloy_assets],
    resources={
        "malloy": MalloyResource(
            execution_mode="auto",  # 'cli' (default), 'python', or 'auto'
        ),
    },
)
```

### 2. Execution engines

`dagster-malloy` supports two execution backends via `MalloyResource`:

1. **`cli`** (Default / Recommended): Executes compilation and query execution using `malloy-cli` / `npx malloy-cli`.
2. **`python`**: Executes queries in-process using the `malloy` Python SDK (`malloy.Runtime`).
3. **`auto`**: Selects `cli` if `malloy-cli` or `npx` is on `$PATH`, otherwise falls back to `python`.

```python
resource = MalloyResource(
    execution_mode="cli",
    cli_path="npx malloy-cli",  # Custom CLI executable path
    config_path="path/to/malloy-config.json",
)
```

### 3. Custom translator (`MalloyTranslator`)

Subclass `MalloyTranslator` to customize asset keys, tags, group names, metadata, or upstream dependencies:

```python
from dagster import AssetKey
from dagster_malloy import MalloyTranslator, MalloyTranslatorData, load_malloy_assets


class CustomMalloyTranslator(MalloyTranslator):
    def get_asset_key(self, data: MalloyTranslatorData) -> AssetKey:
        return AssetKey(["analytics", data.query_info.name])

    def get_group_name(self, data: MalloyTranslatorData) -> str:
        return "malloy_models"


malloy_assets = load_malloy_assets(
    path="./models",
    translator=CustomMalloyTranslator(),
)
```

### 4. Data quality checks

Use `build_malloy_asset_checks` to discover check queries in a `.malloy` file and register them as Dagster `AssetCheckResult` checks attached to a target asset:

```python
from dagster import AssetKey
from dagster_malloy import build_malloy_asset_checks

checks = build_malloy_asset_checks(
    file_path="./models/sales.malloy",
    target_asset_key=AssetKey(["sales", "customer_analytics"]),
)
```

A query is recognised as a check if it's name starts with `check_`, `test_`, `assert_` (eg. `query: check_valid_ids is ...`) or if it's annotated with `# @check`, `# @test` or `# @assert` before the query definition.

A check **passes** when the query returns zero rows, or when the first row contains `invalid_count = 0` or `fail_count = 0`.

## Example Project

A self-contained runnable example project is available in [`examples/malloy_demo`](examples/malloy_demo) with instructions to run locally.

To clone and run the example locally:

```bash
git clone https://github.com/mathisdrn/dagster-malloy.git
cd dagster-malloy/examples/malloy_demo
uv run generate_data.py
uv run dg dev -f definitions.py
```

Open [http://127.0.0.1:3000](http://127.0.0.1:3000) to view the asset catalog and lineage graph.


## Contributing

Contributions, issues, and pull requests are welcome! Feel free to open an issue or submit a pull request on GitHub.