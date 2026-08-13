# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.6] - 2026-08-13

### Added

- **Single-Pass Batch AST Parsing**: `MalloyCliClient.parse_ast_batch()` and `parse_malloy_ast.js` now compile AST metadata for multiple files in a single Node process invocation, significantly accelerating `MalloyParser.build_manifest()`.
- **In-Memory Manifest Support**: Added `manifest_dict` parameter to `MalloyProject`, `load_malloy_assets()`, `@malloy_assets`, and `build_malloy_asset_checks()`, enabling pre-compiled AST dictionary loading for serverless and read-only environments without writing JSON manifest files.

### Fixed

- **Dialect-Aware DDL Identifier Quoting**: `generate_ddl()` now quotes SQL table/view names based on the target database dialect (`"table"` for Postgres/DuckDB/Snowflake, `` `table` `` for BigQuery/MySQL).
- **Dialect-Gated `SET file_search_path`**: Restricted DuckDB-specific search path execution to `duckdb` connections, avoiding syntax errors on PostgreSQL, Snowflake, BigQuery, or Trino.
- **Transitive Join & Asset Key Lineage**: `MalloyTranslator.get_deps()` now recursively resolves transitive joined sources, and unifies raw table dependency `AssetKey` resolution with `_table_to_asset_key()`.

## [0.2.5] - 2026-08-11

### Changed

- `polars` is now an **optional** dependency. Install via `pip install dagster-malloy[execution]`
  to enable query execution and in-memory DataFrame results. The core package (asset graph
  loading, manifest parsing, read-only Dagster webserver boot) no longer requires polars.
- `duckdb` removed from core dependencies. It was never imported by the library itself
  (only by the demo and warehouse-mode tests). It remains in the `[test]` optional extra.
- Added `dagster_malloy._compat` internal module as the single entry point for the optional
  polars import, with a clear `ImportError` message pointing to the `[execution]` extra.

### Migration

If you call `MalloyResource.execute_query()` or `MalloyCliClient.run()`, you now need polars:

```bash
pip install 'dagster-malloy[execution]'
```

Warehouse mode (`execution_mode="warehouse"`) compiles SQL and runs it directly against your
database — it never calls `run()` and does not require polars.

Read-only Dagster webserver with a pre-built manifest requires only the base package:

```bash
pip install dagster-malloy
```

## [0.2.4] - 2026-07-01

- Initial public release.
