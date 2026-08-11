"""Asset check builders for evaluating Malloy data quality assertions in Dagster."""

import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, Union

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetKey,
    asset_check,
)

import polars as pl

from dagster_malloy.parser import MalloyParser
from dagster_malloy.resource import MalloyResource


def build_malloy_asset_checks(
    file_path: Union[str, Path],
    target_asset_key: AssetKey,
    resource_key: str = "malloy",
    manifest_path: Optional[Union[str, Path]] = None,
    use_manifest_if_exists: bool = True,
    auto_recompile_if_stale: bool = True,
) -> Sequence:
    """Discovers Malloy check queries (queries named check_* or annotated with # @check) and returns Dagster asset checks."""
    path_obj = Path(file_path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"Malloy file not found: {path_obj}")

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
        malloy_files = (
            list(path_obj.glob("**/*.malloy")) + list(path_obj.glob("**/*.malloynb"))
            if path_obj.is_dir()
            else [path_obj]
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
        if m_file.exists():
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

    for q_name, q_info in parsed_model.queries.items():
        if q_info.is_check or "check" in q_info.tags or q_name.startswith("check_"):

            def _make_check_fn(file_path_val: Path, q_name_val: str):
                @asset_check(
                    name=q_name_val,
                    asset=target_asset_key,
                    description=f"Malloy data quality check '{q_name_val}' from {file_path_val.name}",
                    required_resource_keys={resource_key},
                )
                def _malloy_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
                    malloy_res: MalloyResource = getattr(context.resources, resource_key, None)
                    if malloy_res is None:
                        malloy_res = MalloyResource()

                    res_data = malloy_res.execute_query(file_path=file_path_val, query_name=q_name_val)

                    row_count = 0
                    first_row = {}

                    if isinstance(res_data, pl.DataFrame):
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
