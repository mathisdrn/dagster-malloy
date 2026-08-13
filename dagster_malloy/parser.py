"""AST parser for Malloy (.malloy) models and (.malloynb) notebooks using official @malloydata/malloy compiler."""

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Union

from dagster_malloy.cli_client import MalloyCliClient


@dataclass
class MalloySourceInfo:
    """Introspected metadata for a Malloy source definition."""

    name: str
    connection: Optional[str] = None
    table_or_sql: Optional[str] = None
    base_source_name: Optional[str] = None
    line_number: int = 1
    raw_code: str = ""
    joined_sources: Set[str] = field(default_factory=set)


@dataclass
class MalloyQueryInfo:
    """Introspected metadata for a Malloy query definition."""

    name: str
    source_name: Optional[str] = None
    view_name: Optional[str] = None
    description: Optional[str] = None
    line_number: int = 1
    raw_code: str = ""
    is_check: bool = False
    is_dashboard: bool = False
    is_notebook_cell: bool = False
    cell_index: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    nested_views: List[str] = field(default_factory=list)


@dataclass
class MalloyParsedModel:
    """Aggregated AST structure extracted from a Malloy file or notebook."""

    file_path: Path
    sources: Dict[str, MalloySourceInfo] = field(default_factory=dict)
    queries: Dict[str, MalloyQueryInfo] = field(default_factory=dict)
    imports: List[str] = field(default_factory=list)
    table_dependencies: Set[str] = field(default_factory=set)


class MalloyParser:
    """Parses Malloy files (.malloy) and Malloy notebooks (.malloynb) via official AST compiler."""

    @classmethod
    def parse_file(
        cls, file_path: Union[str, Path], cli_client: Optional[MalloyCliClient] = None
    ) -> MalloyParsedModel:
        """Parse a .malloy or .malloynb file into a MalloyParsedModel using official AST compiler."""
        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        client = cli_client or MalloyCliClient()
        ast_data = client.parse_ast(path)
        return cls.from_ast_dict(ast_data, file_path=path)

    @classmethod
    def parse_malloy_code(
        cls,
        code: str,
        file_path: Optional[Path] = None,
        cli_client: Optional[MalloyCliClient] = None,
    ) -> MalloyParsedModel:
        """Parse raw Malloy syntax code using official AST compiler."""
        path = file_path or Path("inline.malloy")
        suffix = path.suffix if path.suffix in [".malloy", ".malloynb"] else ".malloy"
        with tempfile.NamedTemporaryFile(
            suffix=suffix, mode="w", encoding="utf-8", delete=False
        ) as tmp:
            tmp.write(code)
            tmp_path = Path(tmp.name)

        try:
            client = cli_client or MalloyCliClient()
            ast_data = client.parse_ast(tmp_path)
            return cls.from_ast_dict(ast_data, file_path=path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    @classmethod
    def parse_notebook(
        cls, notebook_path: Path, cli_client: Optional[MalloyCliClient] = None
    ) -> MalloyParsedModel:
        """Parse a Malloy notebook (.malloynb JSON format)."""
        return cls.parse_file(notebook_path, cli_client=cli_client)

    @classmethod
    def from_ast_dict(cls, data: dict, file_path: Path) -> MalloyParsedModel:
        """Construct MalloyParsedModel from AST dictionary returned by parse_malloy_ast.js."""
        parsed = MalloyParsedModel(file_path=file_path)

        for s_name, s_data in data.get("sources", {}).items():
            base_name = s_data.get("base_source_name")
            conn = s_data.get("connection")
            tbl = s_data.get("table_or_sql")

            if base_name and base_name in parsed.sources:
                parent_info = parsed.sources[base_name]
                if not conn:
                    conn = parent_info.connection
                if not tbl:
                    tbl = parent_info.table_or_sql

            parsed.sources[s_name] = MalloySourceInfo(
                name=s_data.get("name", s_name),
                connection=conn,
                table_or_sql=tbl,
                base_source_name=base_name,
                line_number=s_data.get("line_number", 1),
                raw_code=s_data.get("raw_code", ""),
                joined_sources=set(s_data.get("joined_sources", [])),
            )

        for q_name, q_data in data.get("queries", {}).items():
            parsed.queries[q_name] = MalloyQueryInfo(
                name=q_data.get("name", q_name),
                source_name=q_data.get("source_name"),
                view_name=q_data.get("view_name"),
                description=q_data.get("description"),
                line_number=q_data.get("line_number", 1),
                raw_code=q_data.get("raw_code", ""),
                is_check=q_data.get("is_check", False),
                is_dashboard=q_data.get("is_dashboard", False),
                is_notebook_cell=q_data.get("is_notebook_cell", False),
                cell_index=q_data.get("cell_index"),
                tags=q_data.get("tags", []),
                nested_views=q_data.get("nested_views", []),
            )

        parsed.imports = data.get("imports", [])
        parsed.table_dependencies = set(data.get("table_dependencies", []))
        return parsed

    @classmethod
    def build_manifest(
        cls,
        target_path: Union[str, Path],
        output_path: Optional[Union[str, Path]] = None,
        cli_client: Optional[MalloyCliClient] = None,
    ) -> Path:
        """Compile AST metadata for all .malloy/.malloynb files in target_path into a JSON manifest."""
        import json

        path_obj = Path(target_path).resolve()
        if not path_obj.exists():
            raise FileNotFoundError(f"Path does not exist: {path_obj}")

        if path_obj.is_file():
            malloy_files = [path_obj]
            base_dir = path_obj.parent
        else:
            malloy_files = list(path_obj.glob("**/*.malloy")) + list(path_obj.glob("**/*.malloynb"))
            base_dir = path_obj

        if not malloy_files:
            raise ValueError(f"No .malloy or .malloynb files found in path: {path_obj}")

        client = cli_client or MalloyCliClient()
        manifest_models = {}
        batch_results = client.parse_ast_batch(malloy_files)

        for file in malloy_files:
            abs_path = str(file.resolve())
            ast_data = batch_results.get(abs_path) or batch_results.get(str(file))
            if not ast_data:
                ast_data = client.parse_ast(file)

            rel_path = str(file.relative_to(base_dir))
            filename = file.name

            manifest_models[rel_path] = ast_data
            manifest_models[abs_path] = ast_data
            manifest_models[filename] = ast_data

        manifest_data = {
            "version": "1.0",
            "models": manifest_models,
        }

        if output_path:
            out_file = Path(output_path).resolve()
        else:
            if path_obj.is_dir():
                out_file = path_obj / "malloy_manifest.json"
            else:
                out_file = path_obj.parent / "malloy_manifest.json"

        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)

        return out_file

    @classmethod
    def load_manifest(cls, manifest_path: Union[str, Path]) -> dict:
        """Load an AST manifest JSON file."""
        import json

        p = Path(manifest_path).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Manifest file not found: {p}")
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

