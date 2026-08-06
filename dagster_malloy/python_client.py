"""Python SDK Client wrapping malloy.Runtime for in-process compilation and query execution."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import malloy
    MALLOY_SDK_AVAILABLE = True
except ImportError:
    MALLOY_SDK_AVAILABLE = False


class MalloyPythonError(Exception):
    """Raised when malloy Python SDK execution fails."""
    pass


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

            if hasattr(result, "to_dataframe") and pd is not None:
                return result.to_dataframe()
            elif hasattr(result, "to_dict"):
                data = result.to_dict()
                if pd is not None:
                    return pd.DataFrame(data)
                return data
            elif hasattr(result, "to_json"):
                import json
                parsed = json.loads(result.to_json())
                if pd is not None:
                    return pd.DataFrame(parsed)
                return parsed

            if pd is not None:
                return pd.DataFrame()
            return []
        except Exception as e:
            raise MalloyPythonError(f"Malloy Python SDK run failed: {e}") from e
