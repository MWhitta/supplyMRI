from __future__ import annotations

import csv
import json
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import requests
from requests import Response
from urllib.parse import urljoin


class MshaDownloader:
    """
    Helper for interacting with the MSHA Mine Data Retrieval System (MDRS) API.

    The MDRS datasets are served via the Department of Labor Open Data portal and
    require an API key. This helper mirrors the EDGAR downloader by providing a
    simple interface for fetching data, metadata, and optionally persisting the
    results to disk.
    """

    BASE_URL = "https://apiprod.dol.gov/v4"
    AGENCY_ENDPOINTS_CSV_URL = (
        "https://dol.gov/sites/dolgov/files/Data-Governance/Open%20Data%20Portal/agency-endpoint.csv"
    )
    DEFAULT_THROTTLE = 0.3

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = BASE_URL,
        throttle_seconds: float = DEFAULT_THROTTLE,
        session: Optional[requests.Session] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        if not api_key:
            raise ValueError("An API key is required to query the MSHA MDRS API.")

        if session is None:
            session = requests.Session()

        headers = {
            "X-API-KEY": api_key,
            "Accept": "application/json",
        }
        if user_agent:
            headers["User-Agent"] = user_agent

        session.headers.update(headers)

        self.session = session
        self.base_url = base_url.rstrip("/")
        self.throttle_seconds = max(throttle_seconds, 0.0)
        self._last_request_ts: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Public API                                                        #
    # ------------------------------------------------------------------ #
    def list_endpoints(self, *, agency: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Return the agency/endpoint catalog published by DOL.

        Args:
            agency: Optional agency abbreviation (e.g. "msha") to filter rows.
        """
        response = self._request(self.AGENCY_ENDPOINTS_CSV_URL, expect_json=False)
        text = response.text
        response.close()

        rows: List[Dict[str, str]] = []
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            if agency and row.get("agency", "").lower() != agency.lower():
                continue
            rows.append(row)
        return rows

    def fetch_metadata(self, agency: str, endpoint: str, *, fmt: str = "json") -> Dict[str, Any]:
        """Return the dataset metadata JSON for the given agency/endpoint."""
        if fmt.lower() != "json":
            raise ValueError("Metadata retrieval currently only supports JSON format.")
        path = f"/get/{agency}/{endpoint}/{fmt}/metadata"
        return self._get_json(path)

    def fetch_page(
        self,
        agency: str,
        endpoint: str,
        *,
        fmt: str = "json",
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Fetch a single page of dataset results."""
        if fmt.lower() != "json":
            raise ValueError("Only JSON downloads are currently supported.")
        path = f"/get/{agency}/{endpoint}/{fmt}"
        return self._get_json(path, params=params)

    def download_dataset(
        self,
        agency: str,
        endpoint: str,
        destination: Path,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
        chunk_size: int = 1000,
        fmt: str = "json",
        include_metadata: bool = True,
        overwrite: bool = False,
        filter_object: Optional[Mapping[str, Any]] = None,
        extra_params: Optional[Mapping[str, Any]] = None,
    ) -> List[Path]:
        """
        Download data in chunks and persist each response as JSON.

        Returns a list of file paths written within the destination directory.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive.")

        dest_dir = destination.expanduser().resolve() / agency.lower() / endpoint
        dest_dir.mkdir(parents=True, exist_ok=True)

        saved_paths: List[Path] = []
        dataset_metadata: Optional[Dict[str, Any]] = None

        if include_metadata:
            dataset_metadata = self.fetch_metadata(agency, endpoint, fmt=fmt)
            metadata_path = dest_dir / f"{endpoint}_dataset_metadata.json"
            if overwrite or not metadata_path.exists():
                metadata_path.write_text(json.dumps(dataset_metadata, indent=2, sort_keys=True))

        total_downloaded = 0
        while True:
            current_limit = chunk_size
            if limit is not None:
                remaining = limit - total_downloaded
                if remaining <= 0:
                    break
                current_limit = min(chunk_size, remaining)

            params: Dict[str, Any] = {
                "limit": current_limit,
                "offset": offset + total_downloaded,
            }
            if filter_object:
                params["filter_object"] = json.dumps(filter_object)
            if extra_params:
                params.update(extra_params)

            response_payload = self.fetch_page(agency, endpoint, fmt=fmt, params=params)
            rows = self._extract_rows(response_payload)
            record_count = len(rows)
            if record_count == 0:
                break

            chunk_offset = offset + total_downloaded
            file_stem = f"{endpoint}_offset_{chunk_offset:09d}"
            data_path = dest_dir / f"{file_stem}.json"

            if overwrite or not data_path.exists():
                data_path.write_text(json.dumps(response_payload, indent=2))
            saved_paths.append(data_path)

            if include_metadata:
                metadata_payload: Dict[str, Any] = {
                    "agency": agency,
                    "endpoint": endpoint,
                    "format": fmt,
                    "requested_params": dict(params),
                    "record_count": record_count,
                    "offset": chunk_offset,
                    "chunk_size": current_limit,
                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                }
                if dataset_metadata is not None:
                    metadata_payload["dataset_metadata_path"] = (
                        dest_dir / f"{endpoint}_dataset_metadata.json"
                    ).name

                metadata_path = data_path.with_suffix(data_path.suffix + ".metadata.json")
                if overwrite or not metadata_path.exists():
                    metadata_path.write_text(json.dumps(metadata_payload, indent=2, sort_keys=True))

            total_downloaded += record_count
            if record_count < current_limit:
                break

        return saved_paths

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _extract_rows(self, payload: Any) -> Sequence[Any]:
        """
        Attempt to locate the list containing dataset records within a response.
        """
        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            for key in (
                "data",
                "Data",
                "results",
                "Results",
                "items",
                "Items",
                "records",
                "Records",
            ):
                value = payload.get(key)
                if isinstance(value, list):
                    return value

            result_obj = payload.get("result")
            if isinstance(result_obj, dict):
                for key in ("records", "data", "Results"):
                    value = result_obj.get(key)
                    if isinstance(value, list):
                        return value

            for value in payload.values():
                if isinstance(value, list):
                    return value

        return []

    def _get_json(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        response = self._request(path, params=params, expect_json=True)
        try:
            return response.json()
        finally:
            response.close()

    def _request(
        self,
        path_or_url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        expect_json: bool = True,
    ) -> Response:
        self._respect_throttle()

        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
        else:
            url = urljoin(f"{self.base_url}/", path_or_url.lstrip("/"))

        response = self.session.get(url, params=params, timeout=60)
        response.raise_for_status()

        if expect_json and "application/json" not in response.headers.get("Content-Type", ""):
            response.close()
            raise ValueError(f"Expected JSON response but received {response.headers.get('Content-Type')}")

        self._last_request_ts = time.monotonic()
        return response

    def _respect_throttle(self) -> None:
        if self._last_request_ts is None:
            return
        elapsed = time.monotonic() - self._last_request_ts
        delay = self.throttle_seconds - elapsed
        if delay > 0:
            time.sleep(delay)
