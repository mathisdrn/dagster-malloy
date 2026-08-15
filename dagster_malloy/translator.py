"""Translator classes for mapping Malloy models/queries to Dagster AssetSpecs."""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set, Union

from dagster import (
    AssetKey,
    AssetSpec,
    CodeReferencesMetadataValue,
    LocalFileCodeReference,
    MetadataValue,
)

from dagster_malloy.parser import MalloyParsedModel, MalloyQueryInfo

VALID_DIALECT_KINDS = {
    "duckdb",
    "bigquery",
    "snowflake",
    "postgres",
    "trino",
    "presto",
    "mysql",
    "sqlite",
    "motherduck",
    "ducklake",
}


def _load_malloy_config(start_path: Path, explicit_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Finds and parses malloy-config.json."""
    if explicit_path:
        p = Path(explicit_path).resolve()
        if p.is_file():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}

    start_dir = start_path if start_path.is_dir() else start_path.parent
    for parent in [start_dir] + list(start_dir.parents):
        for name in ["malloy-config.json", ".malloyconfig.json"]:
            cand = parent / name
            if cand.exists():
                try:
                    with open(cand, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    return {}
    return {}


def _resolve_dialect(
    conn_name: Optional[str] = None,
    explicit_dialect: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    file_path: Optional[Path] = None,
    config_path: Optional[Union[str, Path]] = None,
    db_resource_key: Optional[str] = None,
) -> Optional[str]:
    """Resolves underlying dialect/engine from connection name, config, or db_resource_key."""
    val = (explicit_dialect or conn_name or "").lower().strip()
    if val in VALID_DIALECT_KINDS:
        return val
    if val in ("bq", "bigquery"):
        return "bigquery"
    if val in ("postgresql", "postgres"):
        return "postgres"

    if conn_name:
        cfg = config if config is not None else (_load_malloy_config(file_path, config_path) if file_path else {})
        connections = cfg.get("connections", cfg) if isinstance(cfg, dict) else {}
        conn_entry = {}
        if isinstance(connections, dict):
            conn_entry = connections.get(conn_name) or connections.get(conn_name.lower(), {})
        elif isinstance(connections, list):
            conn_entry = next((c for c in connections if isinstance(c, dict) and str(c.get("name", "")).lower() == conn_name.lower()), {})

        if isinstance(conn_entry, dict):
            target_is = str(conn_entry.get("is") or conn_entry.get("dialect") or "").lower().strip()
            if target_is in VALID_DIALECT_KINDS:
                return target_is
            if target_is in ("bq", "bigquery"):
                return "bigquery"
            if target_is in ("postgresql", "postgres"):
                return "postgres"

    if db_resource_key:
        clean_res = db_resource_key.lower().strip()
        for d in VALID_DIALECT_KINDS:
            if d in clean_res:
                return d

    return explicit_dialect.lower().strip() if explicit_dialect else None


@dataclass
class MalloyTranslatorData:
    """Contextual data passed to MalloyTranslator for creating an AssetSpec."""

    query_info: MalloyQueryInfo
    parsed_model: MalloyParsedModel
    file_path: Path
    compiled_sql: Optional[str] = None
    dialect: Optional[str] = None
    table_dependencies: Set[str] = field(default_factory=set)
    include_sources: bool = True
    connection_name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    config_path: Optional[Union[str, Path]] = None
    db_resource_key: Optional[str] = None


def _table_to_asset_key(table_str: str) -> AssetKey:
    """Converts a table name, catalog reference (e.g. DuckLake), or file path string to a Dagster AssetKey."""
    table = table_str.strip("'\"")
    known_extensions = {".parquet", ".csv", ".json", ".duckdb", ".db", ".orc", ".avro", ".tsv"}

    path_obj = Path(table)

    if "." in table and path_obj.suffix.lower() not in known_extensions:
        parts = [p.strip() for p in table.split(".") if p.strip()]
        return AssetKey(parts)

    clean_parts = [p for p in path_obj.parts if p not in ("/", "\\", "")]
    if clean_parts:
        clean_parts[-1] = path_obj.stem
        parts_to_use = [clean_parts[-1]] if path_obj.is_absolute() else clean_parts
        return AssetKey(parts_to_use)

    return AssetKey([table.replace(".", "_")])


def _get_all_joined_sources(
    source_name: str,
    sources_map: Mapping[str, Any],
    visited: Optional[Set[str]] = None,
) -> Set[str]:
    """Recursively resolves all joined and base sources for a given source definition."""
    if visited is None:
        visited = set()
    if source_name in visited or source_name not in sources_map:
        return set()
    visited.add(source_name)

    source_info = sources_map[source_name]
    all_joins: Set[str] = set()

    if getattr(source_info, "base_source_name", None):
        base_name = source_info.base_source_name
        if base_name in sources_map:
            all_joins.add(base_name)
            all_joins.update(_get_all_joined_sources(base_name, sources_map, visited))

    joined = getattr(source_info, "joined_sources", set())
    for j_name in joined:
        if j_name in sources_map:
            all_joins.add(j_name)
            all_joins.update(_get_all_joined_sources(j_name, sources_map, visited))

    return all_joins


class MalloyTranslator:
    """Base translator class mapping Malloy queries/models to Dagster AssetSpecs."""

    def get_asset_key(self, data: MalloyTranslatorData) -> AssetKey:
        """Computes the Dagster AssetKey for a Malloy query asset."""
        stem = data.file_path.stem.replace(".", "_")
        return AssetKey([stem, data.query_info.name])

    def get_source_asset_key(self, source_name: str, file_path: Path) -> AssetKey:
        """Computes the Dagster AssetKey for a Malloy source definition (semantic model)."""
        stem = file_path.stem.replace(".", "_")
        return AssetKey([stem, source_name])

    def get_deps(self, data: MalloyTranslatorData) -> Iterable[AssetKey]:
        """Computes upstream AssetKey dependencies for a Malloy query asset."""
        deps = []

        if data.include_sources and data.query_info.source_name:
            source_key = self.get_source_asset_key(data.query_info.source_name, data.file_path)
            deps.append(source_key)

            all_joined = _get_all_joined_sources(data.query_info.source_name, data.parsed_model.sources)
            for joined_name in sorted(all_joined):
                if joined_name != data.query_info.source_name:
                    j_key = self.get_source_asset_key(joined_name, data.file_path)
                    if j_key not in deps:
                        deps.append(j_key)
        else:
            for table in data.table_dependencies:
                key = _table_to_asset_key(table)
                if key not in deps:
                    deps.append(key)

        if data.query_info and data.query_info.nested_views:
            for ref_view in data.query_info.nested_views:
                if ref_view in data.parsed_model.queries:
                    ref_q_info = data.parsed_model.queries[ref_view]
                    ref_data = MalloyTranslatorData(
                        query_info=ref_q_info,
                        parsed_model=data.parsed_model,
                        file_path=data.file_path,
                        config=data.config,
                        config_path=data.config_path,
                        db_resource_key=data.db_resource_key,
                    )
                    ref_key = self.get_asset_key(ref_data)
                    self_key = self.get_asset_key(data)
                    if ref_key not in deps and ref_key != self_key:
                        deps.append(ref_key)

        return deps

    def get_description(self, data: MalloyTranslatorData) -> Optional[str]:
        """Computes asset description from Malloy docstrings or comments."""
        if data.query_info.description:
            return data.query_info.description
        return f"Malloy query '{data.query_info.name}' from {data.file_path.name}"

    def get_group_name(self, data: MalloyTranslatorData) -> Optional[str]:
        """Computes the Dagster group name for the asset."""
        return "malloy"

    def get_kinds(self, data: MalloyTranslatorData) -> Set[str]:
        """Computes kind badges for the asset UI, restricting engine badges to valid dialects."""
        kinds = {"malloy"}
        if data.query_info.is_dashboard or "dashboard" in data.query_info.tags:
            kinds.add("dashboard")
        else:
            kinds.add("⚙️\N{NO-BREAK SPACE}Query")

        conn_name = data.connection_name
        if not conn_name and data.query_info.source_name:
            source_info = data.parsed_model.sources.get(data.query_info.source_name)
            if source_info and source_info.connection:
                conn_name = source_info.connection

        dialect = _resolve_dialect(
            conn_name=conn_name,
            explicit_dialect=data.dialect,
            config=data.config,
            file_path=data.file_path,
            config_path=data.config_path,
            db_resource_key=data.db_resource_key,
        )

        if dialect and dialect in VALID_DIALECT_KINDS:
            kinds.add(dialect)

        return kinds

    def get_tags(self, data: MalloyTranslatorData) -> Mapping[str, str]:
        """Computes tags for the asset."""
        tags = {
            "dagster-malloy/file": data.file_path.name,
            "dagster-malloy/query": data.query_info.name,
        }
        if data.query_info.is_notebook_cell:
            tags["dagster-malloy/is_notebook"] = "true"
            if data.query_info.cell_index is not None:
                tags["dagster-malloy/cell_index"] = str(data.query_info.cell_index)

        for tag in data.query_info.tags:
            tags[f"malloy/{tag}"] = "true"

        return tags

    def get_owners(self, data: MalloyTranslatorData) -> Optional[Sequence[str]]:
        """Computes owners for the asset."""
        return None

    def get_metadata(self, data: MalloyTranslatorData) -> Mapping[str, Any]:
        """Computes Dagster metadata for the asset including lineage and code references."""
        metadata: Dict[str, Any] = {
            "file_path": str(data.file_path),
            "query_name": data.query_info.name,
        }

        source_info = (
            data.parsed_model.sources.get(data.query_info.source_name)
            if data.query_info.source_name
            else None
        )
        conn_name = data.connection_name or (source_info.connection if source_info else None)

        if data.query_info.source_name:
            metadata["source_name"] = data.query_info.source_name
        if data.query_info.view_name:
            metadata["view_name"] = data.query_info.view_name
        if data.query_info.nested_views:
            metadata["composed_views"] = MetadataValue.text(", ".join(data.query_info.nested_views))
        if data.compiled_sql:
            metadata["compiled_sql"] = data.compiled_sql

        dialect = _resolve_dialect(
            conn_name=conn_name,
            explicit_dialect=data.dialect,
            config=data.config,
            file_path=data.file_path,
            config_path=data.config_path,
            db_resource_key=data.db_resource_key,
        )

        if dialect:
            metadata["dialect"] = dialect
            metadata["malloy/dialect"] = dialect
            metadata["dagster/storage_kind"] = dialect

        if conn_name:
            metadata["malloy/connection"] = conn_name

        if source_info and source_info.table_or_sql:
            metadata["dagster/table_name"] = source_info.table_or_sql.strip("'\"")
        else:
            metadata["dagster/table_name"] = data.query_info.name

        # Extract extra connection parameters from config if present
        cfg = data.config if data.config is not None else (_load_malloy_config(data.file_path, data.config_path) if data.file_path else {})
        if conn_name and cfg:
            connections = cfg.get("connections", cfg)
            c_info = (connections.get(conn_name) or connections.get(conn_name.lower(), {})) if isinstance(connections, dict) else {}
            if isinstance(c_info, dict):
                for k in ["database", "schema", "dataset", "projectId", "catalog"]:
                    if k in c_info and c_info[k]:
                        norm_k = "project_id" if k == "projectId" else k
                        metadata[f"malloy/{norm_k}"] = str(c_info[k])

        if data.query_info.raw_code:
            metadata["malloy_source_code"] = MetadataValue.md(
                f"```malloy\n{data.query_info.raw_code}\n```"
            )

        metadata["dagster/code_references"] = CodeReferencesMetadataValue(
            code_references=[
                LocalFileCodeReference(
                    file_path=str(data.file_path),
                    line_number=data.query_info.line_number,
                )
            ]
        )

        return metadata

    def get_asset_spec(self, data: MalloyTranslatorData) -> AssetSpec:
        """Constructs the complete Dagster AssetSpec for a Malloy query."""
        return AssetSpec(
            key=self.get_asset_key(data),
            deps=self.get_deps(data),
            description=self.get_description(data),
            group_name=self.get_group_name(data),
            kinds=self.get_kinds(data),
            tags=self.get_tags(data),
            owners=self.get_owners(data),
            metadata=self.get_metadata(data),
        )

    def get_source_asset_spec(
        self,
        source_name: str,
        file_path: Path,
        parsed_model: MalloyParsedModel,
        config: Optional[Dict[str, Any]] = None,
        config_path: Optional[Union[str, Path]] = None,
        db_resource_key: Optional[str] = None,
        **kwargs: Any,
    ) -> AssetSpec:
        """Constructs an AssetSpec for a Malloy source semantic model."""
        source_info = parsed_model.sources.get(source_name)
        deps = []
        if source_info:
            if source_info.base_source_name and source_info.base_source_name in parsed_model.sources:
                base_key = self.get_source_asset_key(source_info.base_source_name, file_path)
                if base_key not in deps:
                    deps.append(base_key)
            elif source_info.table_or_sql:
                key = _table_to_asset_key(source_info.table_or_sql)
                if key not in deps:
                    deps.append(key)

        kinds = {"semantic_model", "malloy"}
        conn_name = source_info.connection if source_info else None
        dialect = _resolve_dialect(
            conn_name=conn_name,
            config=config,
            file_path=file_path,
            config_path=config_path,
            db_resource_key=db_resource_key,
        )

        if dialect and dialect in VALID_DIALECT_KINDS:
            kinds.add(dialect)

        metadata: Dict[str, Any] = {
            "file_path": str(file_path),
            "source_name": source_name,
        }
        if conn_name:
            metadata["malloy/connection"] = conn_name
        if dialect:
            metadata["malloy/dialect"] = dialect
            metadata["dialect"] = dialect
            metadata["dagster/storage_kind"] = dialect

        if source_info and source_info.table_or_sql:
            raw_table = source_info.table_or_sql.strip("'\"")
            metadata["table_or_sql"] = source_info.table_or_sql
            metadata["dagster/table_name"] = raw_table

        cfg = config if config is not None else _load_malloy_config(file_path, config_path)
        if conn_name and cfg:
            connections = cfg.get("connections", cfg)
            c_info = (connections.get(conn_name) or connections.get(conn_name.lower(), {})) if isinstance(connections, dict) else {}
            if isinstance(c_info, dict):
                for k in ["database", "schema", "dataset", "projectId", "catalog"]:
                    if k in c_info and c_info[k]:
                        norm_k = "project_id" if k == "projectId" else k
                        metadata[f"malloy/{norm_k}"] = str(c_info[k])

        if source_info and source_info.raw_code:
            metadata["malloy_source_code"] = MetadataValue.md(
                f"```malloy\n{source_info.raw_code}\n```"
            )

        metadata["dagster/code_references"] = CodeReferencesMetadataValue(
            code_references=[
                LocalFileCodeReference(
                    file_path=str(file_path),
                    line_number=source_info.line_number if source_info else 1,
                )
            ]
        )

        return AssetSpec(
            key=self.get_source_asset_key(source_name, file_path),
            deps=deps,
            description=f"Malloy semantic model '{source_name}' defined in {file_path.name}",
            group_name="malloy",
            kinds=kinds,
            metadata=metadata,
        )
