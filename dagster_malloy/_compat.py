"""Optional dependency helpers for dagster-malloy.

Polars is an optional dependency (install via ``pip install dagster-malloy[execution]``).
All library code should import polars through this module rather than directly, so that
the Dagster webserver can boot in read-only / manifest mode without polars installed.
"""

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    pl = None  # type: ignore[assignment]
    HAS_POLARS = False


def require_polars(feature: str = "query execution"):
    """Return the polars module, raising a clear ImportError if it is not installed.

    Args:
        feature: Human-readable description of what requires polars (used in the error message).

    Returns:
        The ``polars`` module.

    Raises:
        ImportError: If polars is not installed.
    """
    if not HAS_POLARS:
        raise ImportError(
            f"polars is required for {feature}. "
            "Install it with: pip install 'dagster-malloy[execution]'"
        )
    return pl
