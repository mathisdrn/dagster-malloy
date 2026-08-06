"""CLI Execution Client wrapping malloy-cli for compilation and query execution."""

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

try:
    import pandas as pd
except ImportError:
    pd = None


class MalloyCliError(Exception):
    """Raised when malloy-cli execution fails."""


def _format_cli_error(raw_output: str) -> str:
    """Parses JSON error structures from malloy-cli and formats them into a clean error string."""
    raw_str = (raw_output or "").strip()
    if not raw_str:
        return "Unknown malloy-cli execution error."

    try:
        data = json.loads(raw_str)
        if isinstance(data, dict) and "error" in data:
            err_msg = str(data["error"]).strip()
            return f"Malloy Compiler Error:\n{err_msg}"
    except (json.JSONDecodeError, TypeError):
        pass

    json_match = re.search(r'\{\s*"error"\s*:\s*".*?"\s*\}', raw_str, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(0))
            if isinstance(data, dict) and "error" in data:
                return f"Malloy Compiler Error:\n{data['error'].strip()}"
        except Exception:  # noqa: BLE001 - best-effort JSON parse of error message
            pass

    return raw_str


class MalloyCliClient:
    """Client for compiling and executing Malloy models via malloy-cli binary or npx."""

    def __init__(
        self,
        cli_path: Optional[str] = None,
        config_path: Optional[Union[str, Path]] = None,
        project_dir: Optional[Union[str, Path]] = None,
    ):
        self.cli_path = cli_path or self._discover_cli()
        self.config_path = str(Path(config_path).resolve()) if config_path else None
        self.project_dir = str(Path(project_dir).resolve()) if project_dir else None

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

    def _get_cwd(self, file_path: Optional[Union[str, Path]] = None) -> Optional[str]:
        """Determine the working directory to use for executing malloy-cli."""
        if self.project_dir:
            return self.project_dir
        if self.config_path:
            # First, check if any parent of the config file contains project root indicators (.git, pyproject.toml, uv.lock, definitions.py)
            start_dir = Path(self.config_path).resolve().parent
            for parent in [start_dir] + list(start_dir.parents):
                if any((parent / indicator).exists() for indicator in [".git", "pyproject.toml", "uv.lock", "definitions.py", "dagster.yaml"]):
                    return str(parent)
            return str(start_dir)

        if file_path:
            start_dir = Path(file_path).resolve().parent
            # First look for a project root containing git/pyproject/uv.lock/definitions.py
            for parent in [start_dir] + list(start_dir.parents):
                if any((parent / indicator).exists() for indicator in [".git", "pyproject.toml", "uv.lock", "definitions.py", "dagster.yaml"]):
                    return str(parent)
            # If not found, look for directory with malloy-config.json
            for parent in [start_dir] + list(start_dir.parents):
                if (parent / "malloy-config.json").exists():
                    return str(parent)
            return str(start_dir)
        return None

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
        abs_file_path = str(Path(file_path).resolve())
        cmd = self._build_base_cmd("compile")
        cmd.extend(["--json", abs_file_path])

        if query_name:
            cmd.extend(["--name", query_name])
        elif query_index is not None:
            cmd.extend(["--index", str(query_index)])

        cwd = self._get_cwd(file_path)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            cwd=cwd,
        )

        if proc.returncode != 0:
            formatted_err = _format_cli_error(proc.stderr or proc.stdout)
            raise MalloyCliError(f"malloy-cli compile failed (code {proc.returncode}):\n{formatted_err}")

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
        abs_file_path = str(Path(file_path).resolve())
        cmd = self._build_base_cmd("run")
        cmd.extend(["--json", abs_file_path])

        if query_name:
            cmd.extend(["--name", query_name])
        elif query_index is not None:
            cmd.extend(["--index", str(query_index)])

        if raw_query:
            cmd.append(raw_query)

        if row_limit is not None:
            cmd.extend(["--row-limit", str(row_limit)])

        cwd = self._get_cwd(file_path)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            cwd=cwd,
        )

        if proc.returncode != 0:
            formatted_err = _format_cli_error(proc.stderr or proc.stdout)
            raise MalloyCliError(f"malloy-cli run failed (code {proc.returncode}):\n{formatted_err}")

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
