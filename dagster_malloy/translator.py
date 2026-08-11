"""Translator classes for mapping Malloy models/queries to Dagster AssetSpecs."""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

from dagster import (
    AssetKey,
    AssetSpec,
    CodeReferencesMetadataValue,
    LocalFileCodeReference,
    MetadataValue,
)

from dagster_malloy.parser import MalloyParsedModel, MalloyQueryInfo


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


class MalloyTranslator:
    """Base translator class mapping Malloy queries/models to Dagster AssetSpecs.

    Subclass this to customize AssetKeys, tags, group names, metadata, kinds, or dependencies.
    """

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

            primary_source = data.parsed_model.sources.get(data.query_info.source_name)
            if primary_source:
                for joined_name in sorted(primary_source.joined_sources):
                    if joined_name in data.parsed_model.sources:
                        j_key = self.get_source_asset_key(joined_name, data.file_path)
                        if j_key not in deps:
                            deps.append(j_key)
        else:
            for table in data.table_dependencies:
                clean_table = table.strip("'\"")
                path_obj = Path(clean_table)
                parts = list(path_obj.parts)
                if parts:
                    parts[-1] = path_obj.stem
                    key = AssetKey(parts)
                    if key not in deps:
                        deps.append(key)

        # Resolve dependencies to nested queries if matching top-level query assets exist
        nested_query_deps = []
        if data.query_info and data.query_info.nested_views:
            for ref_view in data.query_info.nested_views:
                if ref_view in data.parsed_model.queries:
                    ref_q_info = data.parsed_model.queries[ref_view]
                    ref_data = MalloyTranslatorData(
                        query_info=ref_q_info,
                        parsed_model=data.parsed_model,
                        file_path=data.file_path,
                    )
                    ref_key = self.get_asset_key(ref_data)
                    self_key = self.get_asset_key(data)
                    if ref_key not in nested_query_deps and ref_key != self_key:
                        nested_query_deps.append(ref_key)

        if nested_query_deps:
            return nested_query_deps

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
        """Computes the kind badges (e.g. 'malloy', 'duckdb', 'dashboard') for the asset UI."""
        kinds = {"malloy"}
        if data.query_info.is_dashboard or "dashboard" in data.query_info.tags:
            kinds.add("dashboard")
        else:
            kinds.add("⚙️\N{NO-BREAK SPACE}Query")

        dialect = data.dialect
        if not dialect and data.query_info.source_name:
            source_info = data.parsed_model.sources.get(data.query_info.source_name)
            if source_info and source_info.connection:
                dialect = source_info.connection.lower()

        if dialect:
            clean_dialect = dialect.lower().strip()
            if "duckdb" in clean_dialect:
                kinds.add("duckdb")
            elif "bigquery" in clean_dialect or "bq" in clean_dialect:
                kinds.add("bigquery")
            elif "snowflake" in clean_dialect:
                kinds.add("snowflake")
            elif "postgres" in clean_dialect:
                kinds.add("postgres")
            elif "trino" in clean_dialect:
                kinds.add("trino")
            else:
                kinds.add(clean_dialect)

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
        """Computes Dagster metadata for the asset including Malloy source code and code references."""
        metadata: Dict[str, Any] = {
            "file_path": str(data.file_path),
            "query_name": data.query_info.name,
        }

        if data.query_info.source_name:
            metadata["source_name"] = data.query_info.source_name
        if data.query_info.view_name:
            metadata["view_name"] = data.query_info.view_name
        if data.query_info.nested_views:
            metadata["composed_views"] = MetadataValue.text(", ".join(data.query_info.nested_views))
        if data.compiled_sql:
            metadata["compiled_sql"] = data.compiled_sql
        if data.dialect:
            metadata["dialect"] = data.dialect

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
        self, source_name: str, file_path: Path, parsed_model: MalloyParsedModel
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
        if source_info and source_info.connection:
            clean_conn = source_info.connection.lower().strip()
            if "duckdb" in clean_conn:
                kinds.add("duckdb")
            else:
                kinds.add(clean_conn)

        metadata = {
            "file_path": str(file_path),
            "source_name": source_name,
        }
        if source_info and source_info.table_or_sql:
            metadata["table_or_sql"] = source_info.table_or_sql
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
