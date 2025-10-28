from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .location_utils import (
    ResolvedCoordinate,
    create_coordinate_from_text,
    extract_location_hints,
    extract_text_from_html,
    load_gazetteer,
    match_gazetteer,
)


@dataclass
class EdgarProject:
    metadata_path: Path
    document_path: Path
    company: str
    project: str
    jurisdiction: Optional[str]
    location_hints: List[str]
    resolved: Optional[ResolvedCoordinate] = None


def iter_edgar_exhibits(root: Path) -> Iterable[Dict]:
    for metadata_path in sorted(root.rglob("*.metadata.json")):
        try:
            payload = json.loads(metadata_path.read_text())
        except json.JSONDecodeError:
            continue
        document_path = Path(str(metadata_path)[: -len(".metadata.json")])
        if not document_path.exists():
            continue
        yield {
            "metadata_path": metadata_path,
            "document_path": document_path,
            "payload": payload,
        }


def infer_project_name(payload: Dict, text: str) -> str:
    description = payload.get("file_description") or payload.get("file_type") or ""
    candidate = _first_match(
        text,
        [
            r"([A-Z][A-Za-z0-9\s\-]+ Project)",
            r"([A-Z][A-Za-z0-9\s\-]+ Mine)",
            r"([A-Z][A-Za-z0-9\s\-]+ Property)",
        ],
    )
    if candidate:
        return candidate
    if description and any(token in description.upper() for token in ("EX", "TRS", "TECHNICAL")):
        # Use the first strong tag content as fallback
        heading = _first_match(
            text,
            [
                r"([A-Z][A-Za-z0-9\s\-]+ (?:Project|Mine|Deposit|Property))",
            ],
        )
        if heading:
            return heading
    file_name = payload.get("file_name") or payload.get("file_description") or ""
    return Path(file_name).stem


def infer_jurisdiction(payload: Dict, text: str) -> Optional[str]:
    countries = payload.get("inc_states") or payload.get("biz_locations") or []
    if countries:
        return countries[0]
    match = re.search(r"(?:State|Department|Province|Region) of ([A-Za-z\s]+)", text)
    if match:
        return match.group(1).strip()
    match = re.search(r"([A-Z][A-Za-z\s]+?,\s*[A-Z][A-Za-z\s]+)", text)
    if match:
        return match.group(1).strip()
    return None


def build_projects(edgar_root: Path, *, limit: Optional[int] = None) -> List[EdgarProject]:
    projects: List[EdgarProject] = []
    for exhibit in iter_edgar_exhibits(edgar_root):
        text = extract_text_from_html(exhibit["document_path"])
        payload = exhibit["payload"]

        company = _first_company(payload) or "Unknown Company"
        project_name = infer_project_name(payload, text)
        jurisdiction = infer_jurisdiction(payload, text)
        hints = extract_location_hints(text)

        resolved = create_coordinate_from_text(text)

        projects.append(
            EdgarProject(
                metadata_path=exhibit["metadata_path"],
                document_path=exhibit["document_path"],
                company=company,
                project=project_name,
                jurisdiction=jurisdiction,
                location_hints=hints,
                resolved=resolved,
            )
        )
        if limit and len(projects) >= limit:
            break
    return projects


def resolve_with_gazetteer(
    projects: List[EdgarProject],
    gazetteer_path: Optional[Path],
) -> None:
    if not gazetteer_path:
        return
    gazetteer = load_gazetteer(gazetteer_path)
    for project in projects:
        if project.resolved and project.resolved.method == "direct_text":
            continue
        aliases = project.location_hints
        result = match_gazetteer(project.project, project.jurisdiction, aliases, gazetteer)
        if result:
            project.resolved = result


def _first_company(payload: Dict) -> Optional[str]:
    company_names = payload.get("company_names") or []
    if company_names:
        return company_names[0]
    ciks = payload.get("cik") or payload.get("ciks")
    if isinstance(ciks, list) and ciks:
        return ciks[0]
    if isinstance(ciks, str):
        return ciks
    return None


def _first_match(text: str, patterns: List[str]) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    return None


__all__ = ["EdgarProject", "build_projects", "resolve_with_gazetteer"]
