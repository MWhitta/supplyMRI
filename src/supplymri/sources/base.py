from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


class DataSourceClient:
    """Common utilities shared by SupplyMRI data source clients."""

    source_id: str
    default_destination: Path

    def __init__(self, source_id: str, *, default_destination: Path | None = None) -> None:
        self.source_id = source_id
        if default_destination is None:
            default_destination = Path("data") / source_id
        self.default_destination = default_destination

    def resolve_destination(self, destination: Path | None = None) -> Path:
        """Normalize an output path, falling back to the client default."""
        base = destination if destination is not None else self.default_destination
        return base.expanduser().resolve()


@dataclass(frozen=True)
class WorkflowResult:
    """Summary returned by high-level download workflows."""

    source: str
    saved_paths: Sequence[Path]
    details: dict[str, object] | None = None

    @property
    def count(self) -> int:
        return len(self.saved_paths)

    def extend(self, paths: Iterable[Path]) -> "WorkflowResult":
        return WorkflowResult(
            source=self.source,
            saved_paths=list(self.saved_paths) + list(paths),
            details=self.details,
        )
