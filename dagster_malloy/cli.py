"""CLI interface for dagster-malloy."""

import argparse
import sys
from pathlib import Path

from dagster_malloy.cli_client import MalloyCliError, MalloyEnvironmentError
from dagster_malloy.parser import MalloyParser


def build_manifest_command(args: argparse.Namespace) -> int:
    """Execute build-manifest subcommand."""
    target_path = Path(args.path).resolve()
    output_path = Path(args.output).resolve() if args.output else None

    try:
        manifest_file = MalloyParser.build_manifest(
            target_path=target_path,
            output_path=output_path,
        )
        print(f"✅ Successfully compiled Malloy AST manifest to '{manifest_file}'")
        return 0
    except (FileNotFoundError, ValueError, MalloyCliError, MalloyEnvironmentError) as e:
        print(f"❌ Error compiling Malloy manifest: {e}", file=sys.stderr)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dagster-malloy",
        description="CLI tool for dagster-malloy AST manifest compilation and management.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # build-manifest subcommand
    build_parser = subparsers.add_parser(
        "build-manifest",
        help="Compile .malloy and .malloynb AST metadata into a JSON manifest file.",
    )
    build_parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Target file or directory containing .malloy / .malloynb models (default: current directory).",
    )
    build_parser.add_argument(
        "-o",
        "--output",
        help="Output manifest JSON file path (default: <path>/malloy_manifest.json).",
    )

    args = parser.parse_args()

    if args.command == "build-manifest":
        sys.exit(build_manifest_command(args))
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
