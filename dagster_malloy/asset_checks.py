"""Asset check builders for evaluating Malloy data quality assertions in Dagster."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Optional, Union

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetKey,
    asset_check,
)

from dagster_malloy._compat import HAS_POLARS, pl

from dagster_malloy.parser import MalloyParser
from dagster_malloy.project import MalloyProject
from dagster_malloy.resource import MalloyResource


def _get_project_root(path_val: Path) -> Path:
    """Resolve the project root directory containing data, pyproject.toml, definitions.py, etc."""
    resolved = path_val.resolve()
    start_dir = resolved if resolved.is_dir() else resolved.parent
    for parent in [start_dir] + list(start_dir.parents):
        if any((parent / indicator).exists() for indicator in ["data", "pyproject.toml", "uv.lock", "definitions.py", "dagster.yaml", ".git"]):
            return parent
    return start_dir


def _get_db_connection(context: AssetCheckExecutionContext, db_resource_key: Optional[str] = None) -> Any:
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
        "Warehouse check execution requested, but no database resource was found in context.resources."
    )


def build_malloy_asset_checks(
    file_path: Union[str, Path, MalloyProject],
    target_asset_key: AssetKey,
    resource_key: str = "malloy",
    manifest_path: Optional[Union[str, Path]] = None,
    manifest_dict: Optional[Dict[str, Any]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
    execution_mode: Optional[str] = None,
    db_resource_key: Optional[str] = None,
) -> Sequence:
    """Discovers Malloy check queries (queries named check_* or annotated with # @check) and returns Dagster asset checks."""
    if isinstance(file_path, MalloyProject):
        project_obj = file_path
        if manifest_dict is not None:
            project_obj.manifest_dict = manifest_dict
    else:
        project_obj = MalloyProject(
            path=file_path,
            manifest_path=manifest_path,
            manifest_dict=manifest_dict,
            use_manifest_if_exists=use_manifest_if_exists,
            auto_recompile_if_stale=auto_recompile_if_stale,
        )

    path_obj = project_obj.path_obj
    if not path_obj.exists():
        raise FileNotFoundError(f"Malloy file not found: {path_obj}")

    parser = MalloyParser()
    manifest_dict = project_obj.load_manifest()

    models_map = manifest_dict.get("models", manifest_dict) if manifest_dict else None

    parsed_model = None
    if models_map:
        keys_to_try = [str(path_obj.resolve()), path_obj.name, str(path_obj)]
        for k in keys_to_try:
            if k in models_map:
                parsed_model = parser.from_ast_dict(models_map[k], file_path=path_obj)
                break

    if parsed_model is None:
        parsed_model = parser.parse_file(path_obj)

    checks = []

    req_resources = {resource_key}
    if db_resource_key:
        req_resources.add(db_resource_key)

    for q_name, q_info in parsed_model.queries.items():
        if q_info.is_check or "check" in q_info.tags or q_name.startswith("check_") or q_name.startswith("test_") or q_name.startswith("assert_"):

            def _make_check_fn(file_path_val: Path, q_name_val: str):
                @asset_check(
                    name=q_name_val,
                    asset=target_asset_key,
                    description=f"Malloy data quality check '{q_name_val}' from {file_path_val.name}",
                    required_resource_keys=req_resources,
                )
                def _malloy_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
                    malloy_res: MalloyResource = getattr(context.resources, resource_key, None)
                    if malloy_res is None:
                        malloy_res = MalloyResource()

                    mode = execution_mode or malloy_res.execution_mode
                    if mode == "warehouse":
                        chk_sql, chk_dialect = malloy_res.compile_query(file_path=file_path_val, query_name=q_name_val)
                        db_conn = _get_db_connection(context, db_resource_key)

                        root_path = _get_project_root(Path(malloy_res.project_dir or malloy_res.home_dir or file_path_val))
                        if chk_dialect and chk_dialect.lower() == "duckdb" and hasattr(db_conn, "execute"):
                            try:
                                db_conn.execute(f"SET file_search_path = '{root_path}';")
                            except Exception:
                                pass

                        passed = True
                        row_count = 0
                        description = f"Malloy check '{q_name_val}' passed."

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
                                            description = f"Check '{q_name_val}' returned {inv} invalid records."
                                        elif "fail_count" in first:
                                            f_val = int(first["fail_count"])
                                            passed = (f_val == 0)
                                            description = f"Check '{q_name_val}' returned {f_val} failed records."
                                        else:
                                            passed = (row_count == 0)
                                            description = f"Check '{q_name_val}' returned {row_count} rows."
                                    else:
                                        passed = (row_count == 0)
                        except Exception as e:
                            passed = False
                            description = f"Check '{q_name_val}' failed to execute SQL: {e}"

                        return AssetCheckResult(
                            passed=passed,
                            description=description,
                            metadata={
                                "file_path": str(file_path_val),
                                "query_name": q_name_val,
                                "returned_rows": row_count,
                            },
                        )

                    res_data = malloy_res.execute_query(file_path=file_path_val, query_name=q_name_val)

                    row_count = 0
                    first_row = {}

                    if HAS_POLARS and isinstance(res_data, pl.DataFrame):
                        row_count = len(res_data)
                        if not res_data.is_empty():
                            first_row = res_data.row(0, named=True)
                    elif isinstance(res_data, list):
                        row_count = len(res_data)
                        if row_count > 0 and isinstance(res_data[0], dict):
                            first_row = res_data[0]

                    passed = True
                    description = f"Malloy check '{q_name_val}' passed."

                    if "invalid_count" in first_row:
                        invalid_val = int(first_row["invalid_count"])
                        passed = (invalid_val == 0)
                        description = f"Check '{q_name_val}' returned {invalid_val} invalid records."
                    elif "fail_count" in first_row:
                        fail_val = int(first_row["fail_count"])
                        passed = (fail_val == 0)
                        description = f"Check '{q_name_val}' returned {fail_val} failed records."
                    else:
                        passed = (row_count == 0)
                        description = f"Check '{q_name_val}' returned {row_count} rows."

                    return AssetCheckResult(
                        passed=passed,
                        description=description,
                        metadata={
                            "file_path": str(file_path_val),
                            "query_name": q_name_val,
                            "returned_rows": row_count,
                        },
                    )

                return _malloy_check

            checks.append(_make_check_fn(path_obj, q_name))

    return checks

