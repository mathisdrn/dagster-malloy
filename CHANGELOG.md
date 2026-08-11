# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
