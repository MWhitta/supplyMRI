from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

from .edgar import EdgarClient, EdgarDocument
from .msha import MshaClient
from .sources import WorkflowResult


def download_edgar_documents(
    client: EdgarClient,
    documents: Sequence[EdgarDocument] | Iterable[EdgarDocument],
    *,
    destination: Optional[Path] = None,
    include_metadata: bool = True,
    overwrite: bool = False,
) -> WorkflowResult:
    docs: Sequence[EdgarDocument]
    if isinstance(documents, Sequence):
        docs = documents
    else:
        docs = list(documents)
    saved_paths = client.download(
        docs,
        destination,
        include_metadata=include_metadata,
        overwrite=overwrite,
    )
    details = {
        "requested": len(docs),
        "saved": len(saved_paths),
        "destination": str(client.resolve_destination(destination)),
    }
    return WorkflowResult(source=client.source_id, saved_paths=saved_paths, details=details)


def download_msha_dataset(
    client: MshaClient,
    agency: str,
    endpoint: str,
    *,
    destination: Optional[Path] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    chunk_size: int = 1000,
    fmt: str = "json",
    include_metadata: bool = True,
    overwrite: bool = False,
    filter_object: Optional[Mapping[str, object]] = None,
    extra_params: Optional[Mapping[str, object]] = None,
) -> WorkflowResult:
    saved_paths = client.download(
        agency,
        endpoint,
        destination=destination,
        limit=limit,
        offset=offset,
        chunk_size=chunk_size,
        fmt=fmt,
        include_metadata=include_metadata,
        overwrite=overwrite,
        filter_object=filter_object,
        extra_params=extra_params,
    )
    details = {
        "agency": agency,
        "endpoint": endpoint,
        "saved": len(saved_paths),
        "destination": str((client.resolve_destination(destination) / agency.lower() / endpoint)),
    }
    return WorkflowResult(source=client.source_id, saved_paths=saved_paths, details=details)
