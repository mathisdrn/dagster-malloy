# dagster-malloy

`dagster-malloy` is an **unofficial** community integration library providing [Dagster](https://dagster.io) support for [Malloy](https://github.com/malloydata/malloy) models (`.malloy`) and notebooks (`.malloynb`).

> **Note**: This is an unofficial community package and is not affiliated with, endorsed by, or maintained by Dagster Labs or Google/Malloy.

![Asset Lineage Graph](examples/malloy_demo/Global_asset_lineage.svg)

It enables loading Malloy models as Dagster Software-Defined Assets, executing queries via `malloy-cli` or the `malloy` Python SDK, tracking lineage and metadata, and running data quality asset checks.

---

## Demo Quickstart

Try the interactive Malloy demo webserver in a single command using `uvx`:

```bash
uvx dagster-malloy-demo
```

This generates a sample project (`./malloy_demo`) and launches the Dagster UI at [http://127.0.0.1:3000](http://127.0.0.1:3000).

---

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

---

## Usage

### 1. Loading Malloy Assets

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

### 2. Execution Engines

`dagster-malloy` supports two execution backends via `MalloyResource`:

1. **`cli`** (Default / Recommended): Executes compilation and query execution using `malloy-cli` / `npx malloy-cli`.
2. **`python`**: Executes queries in-process using the `malloy` Python SDK (`malloy.Runtime`).

```python
resource = MalloyResource(
    execution_mode="cli",
    cli_path="npx malloy-cli",  # Custom CLI executable path
    config_path="path/to/malloy-config.json",
)
```

### 3. Custom Translator (`MalloyTranslator`)

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

### 4. Data Quality Asset Checks

Use `build_malloy_asset_checks` to convert Malloy validation queries (`check_*` or `# @check`) into Dagster `AssetCheckResult` checks:

```python
from dagster import AssetKey
from dagster_malloy import build_malloy_asset_checks

checks = build_malloy_asset_checks(
    file_path="./models/sales.malloy",
    target_asset_key=AssetKey(["sales", "customer_analytics"]),
)
```

---

## Example Project

A self-contained runnable example project is available in [`examples/malloy_demo`](examples/malloy_demo).

To clone and run the example locally:

```bash
git clone https://github.com/mathisdrn/dagster-malloy.git
cd dagster-malloy/examples/malloy_demo
python generate_data.py
dagster dev -f definitions.py -p 3000
```

Open [http://127.0.0.1:3000](http://127.0.0.1:3000) to view the asset catalog and lineage graph.

---

## Testing

Run unit tests using `pytest`:

```bash
pytest tests/ -v
```

---

## Contributing

Contributions, issues, and pull requests are welcome! Feel free to open an issue or submit a pull request on GitHub.

---

## License

Apache License 2.0.
