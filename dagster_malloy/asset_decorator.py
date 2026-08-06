"""Multi-asset factory decorators for registering Malloy models in Dagster."""

from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableSchema,
    multi_asset,
)

try:
    import pandas as pd
except ImportError:
    pd = None

from dagster_malloy.parser import MalloyParser
from dagster_malloy.resource import MalloyResource
from dagster_malloy.translator import MalloyTranslator, MalloyTranslatorData


def _dataset_to_table_schema(data: Any) -> TableSchema:
    """Converts a pandas DataFrame or list of dicts into a Dagster TableSchema."""
    columns = []
    if pd is not None and isinstance(data, pd.DataFrame):
        for col_name, dtype in data.dtypes.items():
            columns.append(
                TableColumn(
                    name=str(col_name),
                    type=str(dtype),
                )
            )
    elif isinstance(data, list) and len(data) > 0:
        first_row = data[0]
        if isinstance(first_row, dict):
            for col_name, val in first_row.items():
                col_type = type(val).__name__ if val is not None else "string"
                columns.append(
                    TableColumn(
                        name=str(col_name),
                        type=col_type,
                    )
                )
    return TableSchema(columns=columns)


def _dataset_to_markdown_preview(data: Any, max_rows: int = 10) -> str:
    """Renders a pandas DataFrame or list of dicts as a Markdown preview table."""
    if pd is not None and isinstance(data, pd.DataFrame):
        preview_df = data.head(max_rows)
        return preview_df.to_markdown(index=False)
    elif isinstance(data, list) and len(data) > 0:
        preview_rows = data[:max_rows]
        first_row = preview_rows[0]
        if isinstance(first_row, dict):
            headers = [str(h) for h in first_row.keys()]
            header_line = "| " + " | ".join(headers) + " |"
            sep_line = "| " + " | ".join(["---"] * len(headers)) + " |"
            body_lines = []
            for r in preview_rows:
                if isinstance(r, dict):
                    row_str = "| " + " | ".join(str(r.get(h, "")) for h in headers) + " |"
                    body_lines.append(row_str)
            return "\n".join([header_line, sep_line] + body_lines)
    return "*No preview available.*"


def _get_row_count(data: Any) -> int:
    """Safely extracts row count from DataFrame or list."""
    if hasattr(data, "__len__"):
        try:
            return len(data)
        except Exception:
            return 0
    return 0


def load_malloy_assets(
    path: Union[str, Path],
    translator: Optional[MalloyTranslator] = None,
    name: Optional[str] = None,
    create_dashboards: bool = True,
) -> AssetsDefinition:
    """Loads Malloy queries and models from a file or directory as Dagster Software-Defined Assets."""
    path_obj = Path(path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"Path does not exist: {path_obj}")

    if path_obj.is_file():
        malloy_files = [path_obj]
    else:
        malloy_files = list(path_obj.glob("**/*.malloy")) + list(path_obj.glob("**/*.malloynb"))

    if not malloy_files:
        raise ValueError(f"No .malloy or .malloynb files found in path: {path_obj}")

    translator = translator or MalloyTranslator()
    parser = MalloyParser()

    specs: List[AssetSpec] = []
    asset_query_map: Dict[AssetKey, Dict[str, Any]] = {}

    for file in malloy_files:
        parsed_model = parser.parse_file(file)

        for q_name, q_info in parsed_model.queries.items():
            trans_data = MalloyTranslatorData(
                query_info=q_info,
                parsed_model=parsed_model,
                file_path=file,
                dialect=q_info.source_name and parsed_model.sources.get(q_info.source_name, {}).connection if parsed_model.sources.get(q_info.source_name) else None,
                table_dependencies=parsed_model.table_dependencies,
            )
            query_spec = translator.get_asset_spec(trans_data)
            specs.append(query_spec)

            asset_query_map[query_spec.key] = {
                "type": "query",
                "file_path": file,
                "query_name": q_name,
                "query_info": q_info,
            }

            # Create downstream dashboard asset node if requested
            if create_dashboards and (q_info.is_dashboard or "dashboard" in q_info.tags):
                dash_key = AssetKey([query_spec.key.path[0], f"{q_name}_dashboard"])
                dash_spec = AssetSpec(
                    key=dash_key,
                    deps=[query_spec.key],
                    description=f"Interactive Dashboard asset for Malloy query '{q_name}'",
                    group_name=query_spec.group_name or "malloy",
                    kinds={"malloy", "dashboard"},
                    tags={"dagster-malloy/dashboard": "true"},
                )
                specs.append(dash_spec)
                asset_query_map[dash_key] = {
                    "type": "dashboard",
                    "file_path": file,
                    "query_name": q_name,
                    "query_info": q_info,
                    "parent_asset_key": query_spec.key,
                }

    multi_asset_name = name or f"malloy_assets_{path_obj.stem.replace('.', '_')}"

    @multi_asset(
        specs=specs,
        name=multi_asset_name,
        required_resource_keys={"malloy"},
    )
    def _malloy_multi_asset(context: AssetExecutionContext):
        malloy_res: MalloyResource = getattr(context.resources, "malloy", None)
        if malloy_res is None:
            malloy_res = MalloyResource()

        selected_keys = sorted(
            context.selected_asset_keys,
            key=lambda k: 1 if asset_query_map.get(k, {}).get("type") == "dashboard" else 0,
        )

        for target_key in selected_keys:
            if target_key not in asset_query_map:
                continue

            info = asset_query_map[target_key]
            asset_type = info.get("type", "query")
            file_path = info["file_path"]
            query_name = info["query_name"]

            start_time = time.time()

            if asset_type == "query":
                sql, dialect = malloy_res.compile_query(file_path=file_path, query_name=query_name)
                res_data = malloy_res.execute_query(file_path=file_path, query_name=query_name)
                duration = time.time() - start_time
                row_count = _get_row_count(res_data)

                q_info = info.get("query_info")

                metadata = {
                    "file_path": str(file_path),
                    "query_name": query_name,
                    "dialect": dialect,
                    "compiled_sql": MetadataValue.text(sql) if sql else "N/A",
                    "dagster/row_count": row_count,
                    "execution_duration_seconds": round(duration, 4),
                }

                if q_info and q_info.raw_code:
                    metadata["malloy_source_code"] = MetadataValue.md(
                        f"```malloy\n{q_info.raw_code}\n```"
                    )

                if res_data is not None and row_count > 0:
                    metadata["dagster/column_schema"] = MetadataValue.table_schema(_dataset_to_table_schema(res_data))
                    metadata["preview"] = MetadataValue.md(_dataset_to_markdown_preview(res_data))

                yield MaterializeResult(asset_key=target_key, metadata=metadata)

            else:
                # Dashboard Asset Materialization
                res_data = malloy_res.execute_query(file_path=file_path, query_name=query_name)
                duration = time.time() - start_time
                row_count = _get_row_count(res_data)

                rendered_viz = f"### 📊 Malloy Dashboard Rendering: `{query_name}`\n\n"
                if res_data is not None and row_count > 0:
                    rendered_viz += _dataset_to_markdown_preview(res_data, max_rows=15)
                else:
                    rendered_viz += "*No rows returned for dashboard rendering.*"

                metadata = {
                    "dashboard_name": f"{query_name}_dashboard",
                    "file_path": str(file_path),
                    "rendering_engine": "Malloy HTML / Chart Renderer",
                    "preview": MetadataValue.md(rendered_viz),
                    "execution_duration_seconds": round(duration, 4),
                }

                yield MaterializeResult(asset_key=target_key, metadata=metadata)

    return _malloy_multi_asset


def malloy_assets(
    path: Union[str, Path],
    translator: Optional[MalloyTranslator] = None,
    group_name: Optional[str] = None,
    create_dashboards: bool = True,
) -> Callable:
    """Decorator version of load_malloy_assets."""
    def decorator(fn: Callable) -> AssetsDefinition:
        return load_malloy_assets(
            path=path,
            translator=translator,
            create_dashboards=create_dashboards,
        )

    return decorator
