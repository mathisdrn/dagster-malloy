"""MalloyProject class for managing Malloy project paths, manifest artifacts, and dev re-compilation."""

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dagster_malloy.parser import MalloyParser


@dataclass
class MalloyProject:
    """Represents a Malloy project or file, managing paths, manifests, and auto-compilation.

    Attributes:
        path (Union[str, Path]): Path to a `.malloy`/`.malloynb` file or project directory.
        manifest_path (Optional[Union[str, Path]]): Path to `malloy_manifest.json`.
        manifest_dict (Optional[Dict[str, Any]]): Pre-loaded AST manifest dictionary. Default None.
        project_dir (Optional[Union[str, Path]]): Root project directory.
        use_manifest_if_exists (bool): Whether to use manifest when available. Default True.
        auto_recompile_if_stale (bool): Whether to recompile manifest if stale in dev. Default True.
    """

    path: Union[str, Path]
    manifest_path: Optional[Union[str, Path]] = None
    manifest_dict: Optional[Dict[str, Any]] = None
    project_dir: Optional[Union[str, Path]] = None
    use_manifest_if_exists: bool = True
    auto_recompile_if_stale: bool = True
    _parser: MalloyParser = field(default_factory=MalloyParser, repr=False, init=False)

    @property
    def path_obj(self) -> Path:
        """Resolved Path object for the target file or directory."""
        return Path(self.path).resolve()

    @property
    def root_dir(self) -> Path:
        """Root directory for the project."""
        if self.project_dir:
            return Path(self.project_dir).resolve()
        p = self.path_obj
        return p if p.is_dir() else p.parent

    @property
    def manifest_file(self) -> Path:
        """Resolved Path object for the manifest JSON file."""
        if self.manifest_path:
            return Path(self.manifest_path).resolve()
        p = self.path_obj
        if p.is_dir():
            return p / "malloy_manifest.json"
        candidates = [
            p.parent / "malloy_manifest.json",
            p.with_suffix(".malloy.json"),
        ]
        for c in candidates:
            if c.exists():
                return c
        return p.parent / "malloy_manifest.json"

    def get_malloy_files(self) -> List[Path]:
        """Returns all `.malloy` and `.malloynb` files in the project path."""
        p = self.path_obj
        if not p.exists():
            raise FileNotFoundError(f"Path does not exist: {p}")
        if p.is_file():
            return [p]
        files = list(p.glob("**/*.malloy")) + list(p.glob("**/*.malloynb"))
        return sorted(files)

    @property
    def is_stale(self) -> bool:
        """Checks if the manifest file is missing or older than any Malloy source file."""
        if self.manifest_dict is not None:
            return False
        m_file = self.manifest_file
        if not m_file.exists():
            return True
        try:
            files = self.get_malloy_files()
            if not files:
                return False
            max_mtime = max(f.stat().st_mtime for f in files)
            return max_mtime > m_file.stat().st_mtime
        except Exception:
            return False

    def prepare_if_dev(self) -> Optional[Path]:
        """Compiles or updates the manifest if in development mode and the manifest is stale."""
        if self.manifest_dict is not None:
            return None
        if self.auto_recompile_if_stale and shutil.which("node") and self.is_stale:
            try:
                m_path = self._parser.build_manifest(self.path_obj, output_path=self.manifest_path)
                return Path(m_path)
            except Exception:
                pass
        return None

    def load_manifest(self) -> Optional[Dict[str, Any]]:
        """Loads manifest dictionary if available, auto-compiling if stale."""
        if self.manifest_dict is not None:
            return self.manifest_dict
        self.prepare_if_dev()
        m_file = self.manifest_file
        if m_file.exists():
            return self._parser.load_manifest(m_file)
        return None
