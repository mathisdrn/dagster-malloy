"""Multi-asset factory decorators for registering Malloy models in Dagster."""

import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSpec,
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
from dagster_malloy.project import MalloyProject
from dagster_malloy.resource import MalloyResource
from dagster_malloy.translator import MalloyTranslator, MalloyTranslatorData


def _get_project_root(path_val: Path) -> Path:
    """Resolve the project root directory containing data, pyproject.toml, definitions.py, etc."""
    resolved = path_val.resolve()
    start_dir = resolved if resolved.is_dir() else resolved.parent
    for parent in [start_dir] + list(start_dir.parents):
        if any((parent / indicator).exists() for indicator in ["data", "pyproject.toml", "uv.lock", "definitions.py", "dagster.yaml", ".git"]):
            return parent
    return start_dir


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


def _get_db_connection_ctx(context: AssetExecutionContext, db_resource_key: Optional[str] = None) -> Any:
    """Finds a database resource or connection from context.resources."""
    if db_resource_key and hasattr(context.resources, db_resource_key):
        res = getattr(context.resources, db_resource_key)
        if hasattr(res, "get_connection"):
            return res.get_connection()
        if hasattr(res, "get_client"):
            return res.get_client()
        return res

    for key in ["duckdb", "db", "database", "snowflake", "bigquery", "postgres", "sql"]:
        if hasattr(context.resources, key):
            res = getattr(context.resources, key)
            if hasattr(res, "get_connection"):
                return res.get_connection()
            if hasattr(res, "get_client"):
                return res.get_client()
            return res

    raise ValueError(
        "Warehouse execution requested, but no database resource was found in context.resources. "
        "Provide a 'db_resource_key' or add a database resource (e.g., DuckDBResource) to your Definitions."
    )


def load_malloy_assets(
    path: Optional[Union[str, Path, MalloyProject]] = None,
    translator: Optional[MalloyTranslator] = None,
    name: Optional[str] = None,
    include_sources: bool = True,
    include_checks: bool = True,
    can_subset: bool = True,
    manifest_path: Optional[Union[str, Path]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
    project: Optional[Union[str, Path, MalloyProject]] = None,
    execution_mode: Optional[str] = None,
    materialization_mode: str = "table",
    db_resource_key: Optional[str] = None,
) -> AssetsDefinition:
    """Loads Malloy queries and models from a file, directory, or MalloyProject as Dagster Software-Defined Assets."""
    target_project = project or path
    if target_project is None:
        raise ValueError("Either 'path' or 'project' must be specified for load_malloy_assets.")

    if isinstance(target_project, MalloyProject):
        project_obj = target_project
    else:
        project_obj = MalloyProject(
            path=target_project,
            manifest_path=manifest_path,
            use_manifest_if_exists=use_manifest_if_exists,
            auto_recompile_if_stale=auto_recompile_if_stale,
        )

    path_obj = project_obj.path_obj
    malloy_files = project_obj.get_malloy_files()
    if not malloy_files:
        raise ValueError(f"No .malloy or .malloynb files found in path: {path_obj}")

    translator = translator or MalloyTranslator()
    parser = MalloyParser()

    manifest_dict = project_obj.load_manifest()
    models_map = manifest_dict.get("models", manifest_dict) if manifest_dict else None

    specs: List[AssetSpec] = []
    check_specs: List[AssetCheckSpec] = []
    asset_query_map: Dict[AssetKey, Dict[str, Any]] = {}
    inline_checks_map: Dict[AssetKey, List[Dict[str, Any]]] = {}

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

        # First pass to identify query asset keys in this file
        file_query_keys: List[AssetKey] = []
        check_queries: List[Tuple[str, Any]] = []

        for q_name, q_info in parsed_model.queries.items():
            if q_info.is_check or "check" in q_info.tags or q_name.startswith("check_") or q_name.startswith("test_") or q_name.startswith("assert_"):
                check_queries.append((q_name, q_info))
                continue

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
            file_query_keys.append(query_spec.key)

            # Register query asset specification (1-to-1 mapping with Malloy query name)
            asset_query_map[query_spec.key] = {
                "type": "dashboard" if (q_info.is_dashboard or "dashboard" in q_info.tags) else "query",
                "file_path": file,
                "query_name": q_name,
                "query_info": q_info,
            }

        # 2. Register Inline Asset Checks if requested
        if include_checks and check_queries:
            for q_name, q_info in check_queries:
                target_key = None
                if q_info.source_name and include_sources:
                    source_key = translator.get_source_asset_key(q_info.source_name, file)
                    if source_key in asset_query_map:
                        target_key = source_key

                if target_key is None and file_query_keys:
                    target_key = file_query_keys[0]

                if target_key is None:
                    target_key = AssetKey([file.stem.replace(".", "_"), q_name])

                chk_spec = AssetCheckSpec(
                    name=q_name,
                    asset=target_key,
                    description=f"Malloy data quality check '{q_name}' from {file.name}",
                )
                check_specs.append(chk_spec)

                chk_info = {
                    "file_path": file,
                    "check_name": q_name,
                    "query_info": q_info,
                    "target_asset_key": target_key,
                }
                if target_key not in inline_checks_map:
                    inline_checks_map[target_key] = []
                inline_checks_map[target_key].append(chk_info)

    multi_asset_name = name or f"malloy_assets_{path_obj.stem.replace('.', '_')}"

    # Setup resource requirements
    req_resources = {"malloy"}
    if db_resource_key:
        req_resources.add(db_resource_key)

    @multi_asset(
        specs=specs,
        check_specs=check_specs if include_checks else None,
        name=multi_asset_name,
        can_subset=can_subset,
        required_resource_keys=req_resources,
    )
    def _malloy_multi_asset(context: AssetExecutionContext):
        malloy_res: MalloyResource = getattr(context.resources, "malloy", None)
        if malloy_res is None:
            malloy_res = MalloyResource()

        mode = execution_mode or malloy_res.execution_mode
        is_warehouse = (mode == "warehouse")

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

            elif asset_type in ("query", "dashboard"):
                query_name = info["query_name"]
                q_info = info.get("query_info")

                if is_warehouse:
                    target_table_name = target_key.path[-1]
                    ddl, sql, dialect = malloy_res.compile_ctas(
                        file_path=file_path,
                        target_table=target_table_name,
                        query_name=query_name,
                        mode=materialization_mode,
                    )
                    db_conn = _get_db_connection_ctx(context, db_resource_key)

                    root_path = _get_project_root(Path(malloy_res.project_dir or malloy_res.home_dir or file_path))
                    if hasattr(db_conn, "execute"):
                        try:
                            db_conn.execute(f"SET file_search_path = '{root_path}';")
                        except Exception:
                            pass

                    # Execute DDL
                    row_count = 0
                    if hasattr(db_conn, "execute"):
                        res_exec = db_conn.execute(ddl)
                        try:
                            c_res = db_conn.execute(f"SELECT COUNT(*) FROM {target_table_name}").fetchone()
                            row_count = c_res[0] if c_res else 0
                        except Exception:
                            row_count = 0
                    elif hasattr(db_conn, "cursor"):
                        cur = db_conn.cursor()
                        cur.execute(ddl)
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {target_table_name}")
                            c_res = cur.fetchone()
                            row_count = c_res[0] if c_res else 0
                        except Exception:
                            row_count = 0

                    duration = time.time() - start_time
                    metadata = {
                        "file_path": str(file_path),
                        "query_name": query_name,
                        "dialect": dialect,
                        "materialization_mode": materialization_mode,
                        "target_table": target_table_name,
                        "compiled_sql": MetadataValue.text(sql) if sql else "N/A",
                        "ddl": MetadataValue.text(ddl),
                        "dagster/row_count": row_count,
                        "execution_duration_seconds": round(duration, 4),
                    }

                    # Fetch table sample preview and schema metadata from warehouse
                    try:
                        if hasattr(db_conn, "execute"):
                            s_res = db_conn.execute(f"SELECT * FROM {target_table_name} LIMIT 5")
                            if hasattr(s_res, "fetchall") and s_res.description:
                                sample_cols = [c[0] for c in s_res.description]
                                sample_rows = s_res.fetchall()
                                if sample_cols and sample_rows:
                                    table_schema = TableSchema(columns=[TableColumn(name=str(col), type="string") for col in sample_cols])
                                    metadata["dagster/column_schema"] = MetadataValue.table_schema(table_schema)
                                    header = "| " + " | ".join(sample_cols) + " |"
                                    sep = "| " + " | ".join(["---"] * len(sample_cols)) + " |"
                                    r_lines = ["| " + " | ".join(str(val) for val in row) + " |" for row in sample_rows]
                                    preview_md = f"### Warehouse Table Preview (`{target_table_name}`)\n\n" + "\n".join([header, sep] + r_lines)
                                    metadata["preview"] = MetadataValue.md(preview_md)
                    except Exception:
                        pass

                    if q_info and q_info.raw_code:
                        metadata["malloy_source_code"] = MetadataValue.md(
                            f"```malloy\n{q_info.raw_code}\n```"
                        )
                    yield MaterializeResult(asset_key=target_key, metadata=metadata)

                else:
                    sql, dialect = malloy_res.compile_query(file_path=file_path, query_name=query_name)
                    res_data = malloy_res.execute_query(file_path=file_path, query_name=query_name)
                    duration = time.time() - start_time
                    row_count = _get_row_count(res_data)

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

            # Evaluate inline asset checks for this target asset if any exist
            if include_checks and target_key in inline_checks_map:
                for chk in inline_checks_map[target_key]:
                    chk_file = chk["file_path"]
                    chk_q_name = chk["check_name"]

                    if is_warehouse:
                        chk_sql, _ = malloy_res.compile_query(file_path=chk_file, query_name=chk_q_name)
                        db_conn = _get_db_connection_ctx(context, db_resource_key)

                        root_path = _get_project_root(Path(malloy_res.project_dir or malloy_res.home_dir or chk_file))
                        if hasattr(db_conn, "execute"):
                            try:
                                db_conn.execute(f"SET file_search_path = '{root_path}';")
                            except Exception:
                                pass

                        passed = True
                        row_count = 0
                        desc = f"Malloy check '{chk_q_name}' passed."

                        try:
                            if hasattr(db_conn, "execute"):
                                rel = db_conn.execute(chk_sql)
                                if hasattr(rel, "fetchall"):
                                    rows = rel.fetchall()
                                    cols = [c[0] for c in rel.description] if rel.description else []
                                    row_count = len(rows)
                                    if rows and cols:
                                        first = dict(zip(cols, rows[0]))
                                        if "invalid_count" in first:
                                            inv = int(first["invalid_count"])
                                            passed = (inv == 0)
                                            desc = f"Check '{chk_q_name}' returned {inv} invalid records."
                                        elif "fail_count" in first:
                                            f_val = int(first["fail_count"])
                                            passed = (f_val == 0)
                                            desc = f"Check '{chk_q_name}' returned {f_val} failed records."
                                        else:
                                            passed = (row_count == 0)
                                            desc = f"Check '{chk_q_name}' returned {row_count} rows."
                                    else:
                                        passed = (row_count == 0)
                        except Exception as e:
                            passed = False
                            desc = f"Check '{chk_q_name}' failed to execute SQL: {e}"

                        yield AssetCheckResult(
                            asset_key=target_key,
                            check_name=chk_q_name,
                            passed=passed,
                            description=desc,
                            metadata={
                                "file_path": str(chk_file),
                                "query_name": chk_q_name,
                                "returned_rows": row_count,
                            },
                        )

                    else:
                        res_chk = malloy_res.execute_query(file_path=chk_file, query_name=chk_q_name)

                        row_count = _get_row_count(res_chk)
                        first_row = {}
                        if isinstance(res_chk, pl.DataFrame):
                            if not res_chk.is_empty():
                                first_row = res_chk.row(0, named=True)
                        elif isinstance(res_chk, list) and len(res_chk) > 0:
                            if isinstance(res_chk[0], dict):
                                first_row = res_chk[0]

                        passed = True
                        desc = f"Malloy check '{chk_q_name}' passed."

                        if "invalid_count" in first_row:
                            inv_val = int(first_row["invalid_count"])
                            passed = (inv_val == 0)
                            desc = f"Check '{chk_q_name}' returned {inv_val} invalid records."
                        elif "fail_count" in first_row:
                            fail_val = int(first_row["fail_count"])
                            passed = (fail_val == 0)
                            desc = f"Check '{chk_q_name}' returned {fail_val} failed records."
                        else:
                            passed = (row_count == 0)
                            desc = f"Check '{chk_q_name}' returned {row_count} rows."

                        yield AssetCheckResult(
                            asset_key=target_key,
                            check_name=chk_q_name,
                            passed=passed,
                            description=desc,
                            metadata={
                                "file_path": str(chk_file),
                                "query_name": chk_q_name,
                                "returned_rows": row_count,
                            },
                        )

    return _malloy_multi_asset


def malloy_assets(
    path: Optional[Union[str, Path, MalloyProject]] = None,
    translator: Optional[MalloyTranslator] = None,
    group_name: Optional[str] = None,
    include_sources: bool = True,
    include_checks: bool = True,
    can_subset: bool = True,
    manifest_path: Optional[Union[str, Path]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
    project: Optional[Union[str, Path, MalloyProject]] = None,
    execution_mode: Optional[str] = None,
    materialization_mode: str = "table",
    db_resource_key: Optional[str] = None,
) -> Callable:
    """Decorator version of load_malloy_assets."""
    def decorator(fn: Callable) -> AssetsDefinition:
        return load_malloy_assets(
            path=path,
            project=project,
            translator=translator,
            name=group_name,
            include_sources=include_sources,
            include_checks=include_checks,
            can_subset=can_subset,
            manifest_path=manifest_path,
            use_manifest_if_exists=use_manifest_if_exists,
            auto_recompile_if_stale=auto_recompile_if_stale,
            execution_mode=execution_mode,
            materialization_mode=materialization_mode,
            db_resource_key=db_resource_key,
        )

    return decorator

