from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import requests


_DISPLAY_CIK_RE = re.compile(r"\s+\(CIK \d{10}\)$")


@dataclass(frozen=True)
class EdgarDocument:
    """Structured metadata for a single EDGAR filing document."""

    adsh: str
    file_name: str
    cik: str
    ciks: List[str]
    company_names: List[str]
    form: str
    root_forms: List[str]
    file_type: Optional[str]
    file_description: Optional[str]
    file_date: Optional[str]
    period_ending: Optional[str]
    file_numbers: List[str]
    film_numbers: List[str]
    items: List[str]
    biz_states: List[str]
    biz_locations: List[str]
    inc_states: List[str]
    url: str
    score: Optional[float] = None


class EdgarDownloader:
    """Query EDGAR's full-text search endpoint and download matching documents."""

    SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
    DEFAULT_USER_AGENT = "supplyMRI/0.1 (contact@supplymri.example)"

    def __init__(self, user_agent: Optional[str] = None, throttle_seconds: float = 0.3) -> None:
        if user_agent is None:
            user_agent = self.DEFAULT_USER_AGENT
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )
        self.throttle_seconds = throttle_seconds
        self._last_request_ts: Optional[float] = None

    def search_documents(
        self,
        query: str,
        limit: int = 20,
        *,
        forms: Optional[Iterable[str]] = None,
        description_filter: Optional[str] = None,
        start: int = 0,
        date_range: str = "all",
    ) -> List[EdgarDocument]:
        """
        Search EDGAR for documents matching the supplied query.

        Args:
            query: Full-text search term (e.g., 'S-K 1300').
            limit: Maximum number of documents to yield.
            forms: Optional set of form types (10-K, 8-K, EX-96, etc.).
            description_filter: Case-insensitive substring that must appear in the
                file description or type.
            start: Starting offset within the search results.
            date_range: EDGAR dateRange filter (all, today, 10d, 1m, custom, ...).
        """
        results: List[EdgarDocument] = []
        offset = max(start, 0)
        normalized_filter = description_filter.lower() if description_filter else None

        while len(results) < limit:
            params = {
                "q": query,
                "dateRange": date_range,
                "category": "custom",
                "from": offset,
            }
            if forms:
                params["forms"] = ",".join(sorted({f.upper() for f in forms if f}))

            payload = self._get_json(self.SEARCH_URL, params=params)
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                doc = self._hit_to_document(hit)
                if normalized_filter:
                    haystack = " ".join(
                        filter(
                            None,
                            (
                                doc.file_description,
                                doc.file_type,
                                Path(doc.file_name).suffix,
                            ),
                        )
                    ).lower()
                    if normalized_filter not in haystack:
                        continue
                results.append(doc)
                if len(results) >= limit:
                    break

            offset += len(hits)
            if len(hits) < 100:
                break

        return results

    def download_document(
        self,
        document: EdgarDocument,
        destination: Path,
        *,
        include_metadata: bool = True,
        overwrite: bool = False,
    ) -> Path:
        """Download a single document and return the saved path."""
        destination = destination.expanduser().resolve()
        accession_dir = destination / document.cik / document.adsh.replace("-", "")
        accession_dir.mkdir(parents=True, exist_ok=True)

        target_file = accession_dir / Path(document.file_name).name
        if overwrite or not target_file.exists():
            response = self._request(document.url, stream=True)
            with target_file.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=32768):
                    if chunk:
                        handle.write(chunk)
            response.close()

        if include_metadata:
            metadata_path = accession_dir / (Path(document.file_name).name + ".metadata.json")
            with metadata_path.open("w", encoding="utf-8") as metadata_file:
                json.dump(asdict(document), metadata_file, indent=2, sort_keys=True)

        return target_file

    def download_documents(
        self,
        documents: Iterable[EdgarDocument],
        destination: Path,
        *,
        include_metadata: bool = True,
        overwrite: bool = False,
    ) -> List[Path]:
        """Download every document in the iterable, returning their file paths."""
        saved: List[Path] = []
        for doc in documents:
            saved.append(
                self.download_document(
                    doc,
                    destination,
                    include_metadata=include_metadata,
                    overwrite=overwrite,
                )
            )
        return saved

    def _hit_to_document(self, hit: dict) -> EdgarDocument:
        source = hit.get("_source", {})
        identifier = hit.get("_id", "")
        if ":" in identifier:
            adsh, file_name = identifier.split(":", 1)
        else:
            adsh, file_name = source.get("adsh", ""), identifier

        raw_ciks = [str(cik) for cik in source.get("ciks", [])]
        primary_cik = raw_ciks[0] if raw_ciks else ""
        try:
            path_cik = str(int(primary_cik))
        except (ValueError, TypeError):
            path_cik = primary_cik.strip()

        def _to_list(value) -> List[str]:
            if value is None:
                return []
            if isinstance(value, list):
                return [str(v) for v in value]
            return [str(value)]

        company_names = [self._strip_display_cik(name) for name in source.get("display_names", [])]

        return EdgarDocument(
            adsh=adsh,
            file_name=file_name,
            cik=primary_cik or path_cik,
            ciks=raw_ciks,
            company_names=company_names,
            form=source.get("form", ""),
            root_forms=_to_list(source.get("root_forms")),
            file_type=source.get("file_type"),
            file_description=source.get("file_description"),
            file_date=source.get("file_date"),
            period_ending=source.get("period_ending"),
            file_numbers=_to_list(source.get("file_num")),
            film_numbers=_to_list(source.get("film_num")),
            items=_to_list(source.get("items")),
            biz_states=_to_list(source.get("biz_states")),
            biz_locations=_to_list(source.get("biz_locations")),
            inc_states=_to_list(source.get("inc_states")),
            url=f"https://www.sec.gov/Archives/edgar/data/{path_cik}/{adsh.replace('-', '')}/{file_name}",
            score=hit.get("_score"),
        )

    def _strip_display_cik(self, display_name: str) -> str:
        return _DISPLAY_CIK_RE.sub("", display_name or "").strip()

    def _get_json(self, url: str, params: Optional[dict] = None) -> dict:
        response = self._request(url, params=params)
        try:
            return response.json()
        finally:
            response.close()

    def _request(self, url: str, **kwargs) -> requests.Response:
        self._respect_throttle()
        response = self.session.get(url, timeout=30, **kwargs)
        response.raise_for_status()
        self._last_request_ts = time.monotonic()
        return response

    def _respect_throttle(self) -> None:
        if self._last_request_ts is None:
            return
        elapsed = time.monotonic() - self._last_request_ts
        delay = self.throttle_seconds - elapsed
        if delay > 0:
            time.sleep(delay)
