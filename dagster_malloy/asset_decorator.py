"""Multi-asset factory decorators for registering Malloy models in Dagster."""

import shutil
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableSchema,
    multi_asset,
)

import polars as pl

from dagster_malloy.parser import MalloyParser
from dagster_malloy.resource import MalloyResource
from dagster_malloy.translator import MalloyTranslator, MalloyTranslatorData


def _dataset_to_table_schema(data: Any) -> TableSchema:
    """Converts a polars DataFrame or list of dicts into a Dagster TableSchema."""
    columns = []
    if isinstance(data, pl.DataFrame):
        for col_name, dtype in data.schema.items():
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
    """Renders a polars DataFrame or list of dicts as a Markdown preview table."""
    if isinstance(data, pl.DataFrame):
        data = data.head(max_rows).to_dicts()

    if isinstance(data, list) and len(data) > 0:
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
            return "\n".join([header_line, sep_line, *body_lines])
    return "*No preview available.*"


def _get_row_count(data: Any) -> int:
    """Safely extracts row count from DataFrame or list."""
    if hasattr(data, "__len__"):
        try:
            return len(data)
        except Exception:  # noqa: BLE001 - len() may raise on exotic types; fallback to 0
            return 0
    return 0


def load_malloy_assets(
    path: Union[str, Path],
    translator: Optional[MalloyTranslator] = None,
    name: Optional[str] = None,
    create_dashboards: bool = True,
    include_sources: bool = True,
    can_subset: bool = True,
    manifest_path: Optional[Union[str, Path]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
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

    # Staleness check and auto-recompilation
    if auto_recompile_if_stale and shutil.which("node") and (use_manifest_if_exists or manifest_path):
        manifest_file = (
            Path(manifest_path).resolve()
            if manifest_path
            else (
                path_obj / "malloy_manifest.json"
                if path_obj.is_dir()
                else (
                    path_obj.with_suffix(".malloy.json")
                    if path_obj.with_suffix(".malloy.json").exists()
                    else path_obj.parent / "malloy_manifest.json"
                )
            )
        )
        is_stale = not manifest_file.exists() or (
            malloy_files
            and max(f.stat().st_mtime for f in malloy_files) > manifest_file.stat().st_mtime
        )
        if is_stale:
            parser.build_manifest(path_obj, output_path=manifest_path)

    # Determine manifest loading
    manifest_dict = None
    if manifest_path:
        m_file = Path(manifest_path).resolve()
        if not m_file.exists():
            raise FileNotFoundError(f"Manifest file not found: {m_file}")
        manifest_dict = parser.load_manifest(m_file)
    elif use_manifest_if_exists:
        if path_obj.is_dir():
            candidate = path_obj / "malloy_manifest.json"
            if candidate.exists():
                manifest_dict = parser.load_manifest(candidate)
        else:
            candidates = [
                path_obj.parent / "malloy_manifest.json",
                path_obj.with_suffix(".malloy.json"),
            ]
            for candidate in candidates:
                if candidate.exists():
                    manifest_dict = parser.load_manifest(candidate)
                    break

    models_map = manifest_dict.get("models", manifest_dict) if manifest_dict else None

    specs: List[AssetSpec] = []
    asset_query_map: Dict[AssetKey, Dict[str, Any]] = {}

    for file in malloy_files:
        parsed_model = None
        if models_map:
            ast_data = None
            keys_to_try = [str(file.resolve()), file.name, str(file)]
            if path_obj.is_dir():
                try:
                    keys_to_try.insert(0, str(file.relative_to(path_obj)))
                except ValueError:
                    pass
            for k in keys_to_try:
                if k in models_map:
                    ast_data = models_map[k]
                    break
            if ast_data:
                parsed_model = parser.from_ast_dict(ast_data, file_path=file)

        if parsed_model is None:
            parsed_model = parser.parse_file(file)

        # 1. Register Source Semantic Model Specs if requested
        if include_sources:
            for s_name, s_info in parsed_model.sources.items():
                source_spec = translator.get_source_asset_spec(s_name, file, parsed_model)
                specs.append(source_spec)
                asset_query_map[source_spec.key] = {
                    "type": "source",
                    "file_path": file,
                    "source_name": s_name,
                    "source_info": s_info,
                }

        # 2. Register Query Asset Specs (skipping quality checks)
        for q_name, q_info in parsed_model.queries.items():
            if q_info.is_check or "check" in q_info.tags or q_name.startswith("check_"):
                continue  # Data quality check queries are registered as AssetCheck definitions, not asset nodes

            trans_data = MalloyTranslatorData(
                query_info=q_info,
                parsed_model=parsed_model,
                file_path=file,
                dialect=q_info.source_name and parsed_model.sources.get(q_info.source_name, {}).connection if parsed_model.sources.get(q_info.source_name) else None,
                table_dependencies=parsed_model.table_dependencies,
                include_sources=include_sources,
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
                dash_kinds = {"dashboard", "malloy"}
                if query_spec.kinds:
                    for k in query_spec.kinds:
                        if k not in {"query", "sql", "malloy", "semantic_model", "🔍 query", "🔍  query", "⚙️ Query"}:
                            dash_kinds.add(k)

                dash_spec = AssetSpec(
                    key=dash_key,
                    deps=[query_spec.key],
                    description=f"Interactive Dashboard asset for Malloy query '{q_name}'",
                    group_name=query_spec.group_name or "malloy",
                    kinds=dash_kinds,
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

    def _type_order(info: Dict[str, Any]) -> int:
        asset_t = info.get("type", "query")
        if asset_t == "source":
            return 0
        elif asset_t == "query":
            return 1
        return 2

    @multi_asset(
        specs=specs,
        name=multi_asset_name,
        can_subset=can_subset,
        required_resource_keys={"malloy"},
    )
    def _malloy_multi_asset(context: AssetExecutionContext):
        malloy_res: MalloyResource = getattr(context.resources, "malloy", None)
        if malloy_res is None:
            malloy_res = MalloyResource()

        from graphlib import TopologicalSorter

        specs_by_key = {spec.key: spec for spec in specs}
        selected_set = set(context.selected_asset_keys)
        ts = TopologicalSorter()

        for k in selected_set:
            spec = specs_by_key.get(k)
            if spec:
                dep_keys = {dep.asset_key for dep in spec.deps} & selected_set
                ts.add(k, *dep_keys)
            else:
                ts.add(k)

        selected_keys = list(ts.static_order())

        for target_key in selected_keys:
            if target_key not in asset_query_map:
                continue

            info = asset_query_map[target_key]
            asset_type = info.get("type", "query")
            file_path = info["file_path"]

            start_time = time.time()

            if asset_type == "source":
                s_info = info.get("source_info")
                s_name = info.get("source_name")
                metadata = {
                    "file_path": str(file_path),
                    "source_name": s_name,
                    "status": "Semantic Model Registered",
                }
                if s_info and s_info.table_or_sql:
                    metadata["table_or_sql"] = s_info.table_or_sql
                if s_info and s_info.raw_code:
                    metadata["malloy_source_code"] = MetadataValue.md(
                        f"```malloy\n{s_info.raw_code}\n```"
                    )
                yield MaterializeResult(asset_key=target_key, metadata=metadata)

            elif asset_type == "query":
                query_name = info["query_name"]
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
                query_name = info["query_name"]
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
    include_sources: bool = True,
    can_subset: bool = True,
    manifest_path: Optional[Union[str, Path]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
) -> Callable:
    """Decorator version of load_malloy_assets."""
    def decorator(fn: Callable) -> AssetsDefinition:
        return load_malloy_assets(
            path=path,
            translator=translator,
            name=group_name,
            create_dashboards=create_dashboards,
            include_sources=include_sources,
            can_subset=can_subset,
            manifest_path=manifest_path,
            use_manifest_if_exists=use_manifest_if_exists,
            auto_recompile_if_stale=auto_recompile_if_stale,
        )

    return decorator
