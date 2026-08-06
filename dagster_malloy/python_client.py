"""Python SDK Client wrapping malloy.Runtime for in-process compilation and query execution."""

from pathlib import Path
from typing import Any, Optional, Tuple, Union

import polars as pl

import importlib.util

MALLOY_SDK_AVAILABLE = importlib.util.find_spec("malloy") is not None


class MalloyPythonError(Exception):
    """Raised when malloy Python SDK execution fails."""


class MalloyPythonClient:
    """Client for executing Malloy queries using the official in-process malloy Python SDK."""

    def __init__(
        self,
        home_dir: Optional[Union[str, Path]] = None,
    ):
        self.home_dir = str(home_dir) if home_dir else None

    def _get_runtime(self):
        """Imports and initializes malloy.Runtime."""
        try:
            import malloy
            return malloy.Runtime()
        except ImportError:
            raise MalloyPythonError(
                "The 'malloy' Python package is not installed. Install it via `pip install dagster-malloy[python-backend]`."
            )

    def compile(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Compile a Malloy query into SQL using malloy Python SDK."""
        runtime = self._get_runtime()
        try:
            model = runtime.load_model(str(file_path))
            sql = model.compile_sql(query_name) if query_name else model.compile_sql()
            dialect = getattr(model, "dialect", "sql")
            return sql, dialect
        except Exception as e:
            raise MalloyPythonError(f"Malloy Python SDK compilation failed: {e}") from e

    def run(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
    ) -> Any:
        """Execute a Malloy query using malloy Python SDK."""
        runtime = self._get_runtime()
        try:
            model = runtime.load_model(str(file_path))
            result = model.run(query_name) if query_name else model.run()

            if hasattr(result, "to_dict"):
                return pl.DataFrame(result.to_dict())
            if hasattr(result, "to_dataframe"):
                try:
                    return pl.from_pandas(result.to_dataframe())
                except Exception:
                    pass
            if hasattr(result, "to_json"):
                import json
                return pl.DataFrame(json.loads(result.to_json()))
            return pl.DataFrame()
        except Exception as e:
            raise MalloyPythonError(f"Malloy Python SDK run failed: {e}") from e
