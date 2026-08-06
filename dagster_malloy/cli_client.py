"""CLI Execution Client wrapping malloy-cli for compilation and query execution."""

import json
from pathlib import Path
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import pandas as pd
except ImportError:
    pd = None


class MalloyCliError(Exception):
    """Raised when malloy-cli execution fails."""
    pass


class MalloyCliClient:
    """Client for compiling and executing Malloy models via malloy-cli binary or npx."""

    def __init__(
        self,
        cli_path: Optional[str] = None,
        config_path: Optional[Union[str, Path]] = None,
        project_dir: Optional[Union[str, Path]] = None,
    ):
        self.cli_path = cli_path or self._discover_cli()
        self.config_path = str(config_path) if config_path else None
        self.project_dir = str(project_dir) if project_dir else None

    def _discover_cli(self) -> str:
        """Find malloy-cli or npx malloy-cli executable."""
        cli = shutil.which("malloy-cli")
        if cli:
            return cli
        npx = shutil.which("npx")
        if npx:
            return "npx malloy-cli"
        return "npx malloy-cli"

    def _build_base_cmd(self, action: str) -> List[str]:
        """Construct base command list."""
        cmd = self.cli_path.split()
        cmd.append(action)

        if self.config_path:
            cmd.extend(["--config", self.config_path])
        if self.project_dir:
            cmd.extend(["--project-dir", self.project_dir])

        return cmd

    def compile(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
        query_index: Optional[int] = None,
    ) -> Tuple[str, str]:
        """Compile a Malloy file or query into SQL using malloy-cli.

        Returns:
            Tuple[str, str]: (compiled_sql, dialect)
        """
        cmd = self._build_base_cmd("compile")
        cmd.extend(["--json", str(file_path)])

        if query_name:
            cmd.extend(["--name", query_name])
        elif query_index is not None:
            cmd.extend(["--index", str(query_index)])

        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        if proc.returncode != 0:
            raise MalloyCliError(f"malloy-cli compile failed (code {proc.returncode}): {proc.stderr or proc.stdout}")

        raw_output = proc.stdout.strip()
        try:
            parsed = json.loads(raw_output)
            if isinstance(parsed, dict):
                sql = parsed.get("sql", str(parsed))
                dialect = parsed.get("dialect", "sql")
                return sql, dialect
            elif isinstance(parsed, list) and parsed:
                first = parsed[0]
                if isinstance(first, dict):
                    return first.get("sql", str(first)), first.get("dialect", "sql")
            return raw_output, "sql"
        except json.JSONDecodeError:
            # Fallback if raw text SQL output
            return raw_output, "sql"

    def run(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
        query_index: Optional[int] = None,
        raw_query: Optional[str] = None,
        row_limit: Optional[int] = None,
    ) -> Any:
        """Execute a Malloy file or query using malloy-cli and return a DataFrame or list of dicts.

        Returns:
            Union[pd.DataFrame, List[Dict[str, Any]]]: Query result dataset.
        """
        cmd = self._build_base_cmd("run")
        cmd.extend(["--json", str(file_path)])

        if query_name:
            cmd.extend(["--name", query_name])
        elif query_index is not None:
            cmd.extend(["--index", str(query_index)])

        if raw_query:
            cmd.append(raw_query)

        if row_limit is not None:
            cmd.extend(["--row-limit", str(row_limit)])

        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        if proc.returncode != 0:
            raise MalloyCliError(f"malloy-cli run failed (code {proc.returncode}): {proc.stderr or proc.stdout}")

        raw_output = proc.stdout.strip()
        if not raw_output:
            return pd.DataFrame() if pd is not None else []

        try:
            parsed = json.loads(raw_output)
            rows = []
            if isinstance(parsed, list):
                rows = parsed
            elif isinstance(parsed, dict) and "data" in parsed:
                rows = parsed["data"]
            elif isinstance(parsed, dict):
                rows = [parsed]

            if pd is not None:
                return pd.DataFrame(rows)
            return rows
        except json.JSONDecodeError:
            raise MalloyCliError(f"Failed to parse malloy-cli JSON response: {raw_output}")
