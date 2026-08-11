"""Malloy Resource for managing connections, runtime options, and query execution in Dagster."""

from pathlib import Path
from typing import Any, Optional, Tuple, Union

from dagster import ConfigurableResource
from pydantic import Field

from dagster_malloy.cli_client import MalloyCliClient


class MalloyResource(ConfigurableResource):
    """Dagster ConfigurableResource for compiling and executing Malloy queries.

    Attributes:
        execution_mode (str): Engine to use ('cli' or 'auto'). Default is 'auto'.
        cli_path (Optional[str]): Path to malloy-cli binary or 'npx malloy-cli'.
        config_path (Optional[str]): Path to malloy-config.json.
        project_dir (Optional[str]): Root directory for Malloy project.
        home_dir (Optional[str]): Home directory for file resolution (e.g. DuckDB data files).
    """

    execution_mode: str = Field(
        default="auto",
        description="Execution engine mode ('cli' or 'auto'). Defaults to CLI execution.",
    )
    cli_path: Optional[str] = Field(
        default=None,
        description="Path to malloy-cli binary or command string (e.g., 'npx malloy-cli').",
    )
    config_path: Optional[str] = Field(
        default=None,
        description="Path to malloy connection config JSON file.",
    )
    project_dir: Optional[str] = Field(
        default=None,
        description="Root project directory for Malloy CLI config resolution.",
    )
    home_dir: Optional[str] = Field(
        default=None,
        description="Directory for local database files (e.g., DuckDB).",
    )

    def get_cli_client(self) -> MalloyCliClient:
        """Instantiate MalloyCliClient."""
        return MalloyCliClient(
            cli_path=self.cli_path,
            config_path=self.config_path,
            project_dir=self.project_dir or self.home_dir,
        )

    def compile_query(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
        raw_code: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Compiles a Malloy query into (SQL, dialect)."""
        cli = self.get_cli_client()
        return cli.compile(file_path=file_path, query_name=query_name)

    def execute_query(
        self,
        file_path: Union[str, Path],
        query_name: Optional[str] = None,
        raw_code: Optional[str] = None,
    ) -> Any:
        """Executes a Malloy query and returns a polars DataFrame."""
        cli = self.get_cli_client()
        return cli.run(file_path=file_path, query_name=query_name, raw_query=raw_code)

