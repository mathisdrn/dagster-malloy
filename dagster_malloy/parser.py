"""AST and regex parser for Malloy (.malloy) models and (.malloynb) notebooks."""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Dict, List, Optional, Set, Tuple, Union


@dataclass
class MalloySourceInfo:
    """Introspected metadata for a Malloy source definition."""

    name: str
    connection: Optional[str] = None
    table_or_sql: Optional[str] = None
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


@dataclass
class MalloyParsedModel:
    """Aggregated AST structure extracted from a Malloy file or notebook."""

    file_path: Path
    sources: Dict[str, MalloySourceInfo] = field(default_factory=dict)
    queries: Dict[str, MalloyQueryInfo] = field(default_factory=dict)
    imports: List[str] = field(default_factory=list)
    table_dependencies: Set[str] = field(default_factory=set)


class MalloyParser:
    """Parses Malloy files (.malloy) and Malloy notebooks (.malloynb)."""

    # Regex patterns for parsing Malloy construct declarations
    SOURCE_PATTERN = re.compile(
        r"source\s*:\s*([a-zA-Z0-9_]+)\s+is\s+([a-zA-Z0-9_\.]+)\s*\(\s*['\"]([^'\"]+)['\"]",
        re.IGNORECASE,
    )
    SOURCE_SIMPLE_PATTERN = re.compile(
        r"source\s*:\s*([a-zA-Z0-9_]+)\s+is\s+([a-zA-Z0-9_]+)",
        re.IGNORECASE,
    )
    VIEW_PATTERN = re.compile(
        r"view\s*:\s*([a-zA-Z0-9_]+)\s+is",
        re.IGNORECASE,
    )
    QUERY_PATTERN = re.compile(
        r"query\s*:\s*([a-zA-Z0-9_]+)\s+is\s+([a-zA-Z0-9_]+)(?:\s*->\s*(.*))?",
        re.IGNORECASE,
    )
    RUN_PATTERN = re.compile(
        r"run\s*:\s*([a-zA-Z0-9_]+)\s*->\s*([a-zA-Z0-9_]+|\{[^}]*\})",
        re.IGNORECASE,
    )
    IMPORT_PATTERN = re.compile(
        r"import\s+['\"]([^'\"]+)['\"]",
        re.IGNORECASE,
    )
    ANNOTATION_PATTERN = re.compile(
        r"#\s*@([a-zA-Z0-9_-]+)(?:\s+(.*))?",
    )
    CHECK_PATTERN = re.compile(
        r"(?:check_|test_|assert_)([a-zA-Z0-9_]+)",
        re.IGNORECASE,
    )
    JOIN_PATTERN = re.compile(
        r"join_(?:one|many|cross)\s*:\s*([a-zA-Z0-9_]+)",
        re.IGNORECASE,
    )
    DASHBOARD_TAGS: ClassVar[set[str]] = {
        "dashboard",
        "bar_chart",
        "line_chart",
        "scatter_chart",
        "shape_map",
        "segment_map",
        "render",
        "report",
        "viz",
    }

    @classmethod
    def _extract_multiline_block(cls, lines: List[str], start_idx: int) -> Tuple[str, int]:
        """Extracts a single-line or multi-line Malloy declaration, balancing braces if needed."""
        block_lines = [lines[start_idx]]
        first_line = lines[start_idx]

        open_braces = first_line.count("{")
        close_braces = first_line.count("}")
        brace_depth = open_braces - close_braces

        idx = start_idx + 1
        while brace_depth > 0 and idx < len(lines):
            next_line = lines[idx]
            block_lines.append(next_line)
            brace_depth += next_line.count("{") - next_line.count("}")
            idx += 1

        return "\n".join(block_lines).rstrip(), len(block_lines)

    @classmethod
    def parse_file(cls, file_path: Union[str, Path]) -> MalloyParsedModel:
        """Parse a .malloy or .malloynb file into a MalloyParsedModel."""
        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if path.suffix == ".malloynb":
            return cls.parse_notebook(path)

        code = path.read_text(encoding="utf-8")
        return cls.parse_malloy_code(code, file_path=path)

    @classmethod
    def parse_malloy_code(
        cls, code: str, file_path: Optional[Path] = None
    ) -> MalloyParsedModel:
        """Parse raw Malloy syntax code."""
        parsed = MalloyParsedModel(file_path=file_path or Path("inline.malloy"))
        lines = code.splitlines()

        current_description: List[str] = []
        current_tags: List[str] = []
        in_check_mode = False
        in_dashboard_mode = False

        # Extract imports
        for match in cls.IMPORT_PATTERN.finditer(code):
            parsed.imports.append(match.group(1))

        # Extract sources and queries with multiline brace matching
        i = 0
        while i < len(lines):
            line_idx = i + 1
            line = lines[i]
            line_str = line.strip()

            # Comment / annotation parsing
            if line_str.startswith("#"):
                ann_match = cls.ANNOTATION_PATTERN.match(line_str)
                if ann_match:
                    tag_name = ann_match.group(1).lower()
                    current_tags.append(tag_name)
                    if tag_name in ["check", "test", "assert"]:
                        in_check_mode = True
                    if tag_name in cls.DASHBOARD_TAGS:
                        in_dashboard_mode = True
                else:
                    comment_text = line_str.lstrip("#").strip()
                    if comment_text:
                        current_description.append(comment_text)
                i += 1
                continue

            # Source definition: source: foo is orca.table('file.parquet')
            source_match = cls.SOURCE_PATTERN.search(line_str)
            if source_match:
                source_name = source_match.group(1)
                conn_raw = source_match.group(2)
                conn = conn_raw.split(".")[0] if "." in conn_raw else conn_raw
                table_or_sql = source_match.group(3)
                raw_block, consumed_count = cls._extract_multiline_block(lines, i)
                joined = set(cls.JOIN_PATTERN.findall(raw_block))

                parsed.sources[source_name] = MalloySourceInfo(
                    name=source_name,
                    connection=conn,
                    table_or_sql=table_or_sql,
                    line_number=line_idx,
                    raw_code=raw_block,
                    joined_sources=joined,
                )
                if table_or_sql:
                    parsed.table_dependencies.add(table_or_sql)
                current_description = []
                current_tags = []
                in_check_mode = False
                in_dashboard_mode = False
                i += consumed_count
                continue

            # Simple source declaration: source: foo is bar
            source_simple = cls.SOURCE_SIMPLE_PATTERN.search(line_str)
            if source_simple and source_simple.group(1) not in parsed.sources:
                source_name = source_simple.group(1)
                raw_block, consumed_count = cls._extract_multiline_block(lines, i)

                parsed.sources[source_name] = MalloySourceInfo(
                    name=source_name,
                    line_number=line_idx,
                    raw_code=raw_block,
                )
                current_description = []
                current_tags = []
                in_check_mode = False
                in_dashboard_mode = False
                i += consumed_count
                continue

            # Named query: query: my_query is source -> view
            query_match = cls.QUERY_PATTERN.search(line_str)
            if query_match:
                q_name = query_match.group(1)
                s_name = query_match.group(2)
                v_name = query_match.group(3) if query_match.lastindex and query_match.lastindex >= 3 else None
                raw_block, consumed_count = cls._extract_multiline_block(lines, i)

                is_check = (
                    in_check_mode
                    or bool(cls.CHECK_PATTERN.match(q_name))
                    or "check" in current_tags
                    or "test" in current_tags
                )

                is_dashboard = (
                    in_dashboard_mode
                    or bool(set(current_tags) & cls.DASHBOARD_TAGS)
                )

                desc = "\n".join(current_description) if current_description else None

                parsed.queries[q_name] = MalloyQueryInfo(
                    name=q_name,
                    source_name=s_name,
                    view_name=v_name.strip() if v_name else None,
                    description=desc,
                    line_number=line_idx,
                    raw_code=raw_block,
                    is_check=is_check,
                    is_dashboard=is_dashboard,
                    tags=list(current_tags),
                )

                current_description = []
                current_tags = []
                in_check_mode = False
                in_dashboard_mode = False
                i += consumed_count
                continue

            i += 1

        return parsed

    @classmethod
    def parse_notebook(cls, notebook_path: Path) -> MalloyParsedModel:
        """Parse a Malloy notebook (.malloynb JSON format)."""
        content = notebook_path.read_text(encoding="utf-8")
        notebook_json = json.loads(content)

        parsed = MalloyParsedModel(file_path=notebook_path)

        cells = notebook_json.get("cells", [])
        for cell_idx, cell in enumerate(cells):
            cell_type = cell.get("cell_type") or cell.get("kind")
            if cell_type in ["code", 2]:
                source_lines = cell.get("source", [])
                source_code = "".join(source_lines) if isinstance(source_lines, list) else str(source_lines)

                cell_parsed = cls.parse_malloy_code(source_code, file_path=notebook_path)

                # Merge sources
                parsed.sources.update(cell_parsed.sources)
                parsed.table_dependencies.update(cell_parsed.table_dependencies)
                parsed.imports.extend(cell_parsed.imports)

                # Mark query notebook cell metadata
                for q_name, q_info in cell_parsed.queries.items():
                    q_info.is_notebook_cell = True
                    q_info.cell_index = cell_idx
                    parsed.queries[q_name] = q_info

                # Parse run statements inside notebook cell
                for line in source_code.splitlines():
                    run_match = cls.RUN_PATTERN.search(line.strip())
                    if run_match:
                        s_name = run_match.group(1)
                        cell_query_name = f"cell_{cell_idx}_run"
                        parsed.queries[cell_query_name] = MalloyQueryInfo(
                            name=cell_query_name,
                            source_name=s_name,
                            description=f"Notebook Cell #{cell_idx} execution",
                            line_number=cell_idx,
                            raw_code=line.strip(),
                            is_notebook_cell=True,
                            cell_index=cell_idx,
                            is_dashboard=True,
                        )

        return parsed
