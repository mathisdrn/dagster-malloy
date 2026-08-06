"""Asset check builders for evaluating Malloy data quality assertions in Dagster."""

from pathlib import Path
from typing import Optional, Sequence, Union

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetCheckExecutionContext,
    AssetKey,
    AssetCheckEvaluation,
    asset_check,
)

try:
    import pandas as pd
except ImportError:
    pd = None

from dagster_malloy.parser import MalloyParser
from dagster_malloy.resource import MalloyResource


def build_malloy_asset_checks(
    file_path: Union[str, Path],
    target_asset_key: AssetKey,
    resource_key: str = "malloy",
) -> Sequence:
    """Discovers Malloy check queries (queries named check_* or annotated with # @check) and returns Dagster asset checks."""
    path_obj = Path(file_path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"Malloy file not found: {path_obj}")

    parser = MalloyParser()
    parsed_model = parser.parse_file(path_obj)

    checks = []

    for q_name, q_info in parsed_model.queries.items():
        if q_info.is_check or "check" in q_info.tags or q_name.startswith("check_"):
            check_name = q_name

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

                    if pd is not None and isinstance(res_data, pd.DataFrame):
                        row_count = len(res_data)
                        if not res_data.empty:
                            first_row = res_data.iloc[0].to_dict()
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
