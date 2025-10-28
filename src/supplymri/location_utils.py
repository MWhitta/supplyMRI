from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass, field
from difflib import get_close_matches
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


COORDINATE_PATTERN = re.compile(
    r"""
    (?P<lat>[+-]?\d{1,2}(?:\.\d+)?)
    [\s°,;]*
    (?P<north>[NS])?
    [\s°,;]*
    (?P<lon>[+-]?\d{1,3}(?:\.\d+)?)
    [\s°,;]*
    (?P<east>[EW])?
    """,
    re.IGNORECASE | re.VERBOSE,
)

LOCATION_KEYWORDS = (
    "project",
    "mine",
    "deposit",
    "property",
    "operation",
    "complex",
    "concession",
    "shaft",
    "pit",
    "district",
)


@dataclass
class GazetteerEntry:
    name: str
    latitude: float
    longitude: float
    aliases: List[str] = field(default_factory=list)
    jurisdiction: Optional[str] = None
    source: Optional[str] = None

    def as_tuple(self) -> Tuple[float, float]:
        return (self.latitude, self.longitude)


@dataclass
class ResolvedCoordinate:
    latitude: float
    longitude: float
    confidence: str
    score: float
    method: str
    source: Optional[str] = None
    candidate: Optional[GazetteerEntry] = None


def normalise_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9\s\-]", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def extract_text_from_html(path: Path) -> str:
    """
    Very lightweight HTML to text conversion for EDGAR exhibits.
    """
    raw = path.read_text(errors="ignore")
    # Drop script/style content
    raw = re.sub(r"<script.*?</script>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    raw = re.sub(r"<style.*?</style>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    # Replace tags with spaces
    text = re.sub(r"<[^>]+>", " ", raw)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_coordinate_candidates(text: str) -> List[Tuple[float, float]]:
    """
    Identify latitude/longitude pairs embedded in free-form text.
    """
    matches: List[Tuple[float, float]] = []
    for match in COORDINATE_PATTERN.finditer(text):
        lat = float(match.group("lat"))
        lon = float(match.group("lon"))
        north = match.group("north")
        east = match.group("east")

        if north:
            lat = abs(lat) if north.upper() == "N" else -abs(lat)
        if east:
            lon = abs(lon) if east.upper() == "E" else -abs(lon)

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        matches.append((lat, lon))
    return matches


def extract_location_hints(text: str, max_phrases: int = 10) -> List[str]:
    """
    Harvest short phrases surrounding mining keywords.
    """
    hints: List[str] = []
    lowered = text.lower()
    for keyword in LOCATION_KEYWORDS:
        index = 0
        while True:
            index = lowered.find(keyword, index)
            if index == -1:
                break
            start = max(0, index - 80)
            end = min(len(text), index + 80)
            snippet = text[start:end].strip()
            if snippet not in hints:
                hints.append(snippet)
                if len(hints) >= max_phrases:
                    return hints
            index += len(keyword)
    return hints


def load_gazetteer(path: Path) -> List[GazetteerEntry]:
    """
    Load a gazetteer file. Supports CSV (with columns name, latitude, longitude)
    and JSON (list of dicts).
    """
    if not path.exists():
        raise FileNotFoundError(path)

    entries: List[GazetteerEntry] = []
    if path.suffix.lower() in {".json", ".geojson"}:
        payload = json.loads(path.read_text())
        if isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
            for feature in payload.get("features", []):
                geometry = feature.get("geometry") or {}
                coords = geometry.get("coordinates") or []
                if len(coords) < 2:
                    continue
                properties = feature.get("properties") or {}
                entries.append(
                    GazetteerEntry(
                        name=str(properties.get("name") or properties.get("title") or "Unnamed Mine"),
                        latitude=float(coords[1]),
                        longitude=float(coords[0]),
                        aliases=_split_aliases(properties.get("aliases")),
                        jurisdiction=properties.get("jurisdiction"),
                        source=properties.get("source") or "geojson",
                    )
                )
        elif isinstance(payload, list):
            for item in payload:
                try:
                    entries.append(
                        GazetteerEntry(
                            name=str(item["name"]),
                            latitude=float(item["latitude"]),
                            longitude=float(item["longitude"]),
                            aliases=_split_aliases(item.get("aliases")),
                            jurisdiction=item.get("jurisdiction"),
                            source=item.get("source"),
                        )
                    )
                except KeyError:
                    continue
    else:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    entries.append(
                        GazetteerEntry(
                            name=row["name"],
                            latitude=float(row["latitude"]),
                            longitude=float(row["longitude"]),
                            aliases=_split_aliases(row.get("aliases")),
                            jurisdiction=row.get("jurisdiction"),
                            source=row.get("source") or path.name,
                        )
                    )
                except (KeyError, ValueError):
                    continue

    return entries


def _split_aliases(value: Optional[str]) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [alias.strip() for alias in re.split(r"[;|,]", str(value)) if alias.strip()]


def match_gazetteer(
    project_name: str,
    jurisdiction: Optional[str],
    aliases: Iterable[str],
    gazetteer: Sequence[GazetteerEntry],
) -> Optional[ResolvedCoordinate]:
    """
    Match a project to a gazetteer entry using fuzzy logic and jurisdiction filtering.
    """
    if not gazetteer:
        return None

    search_terms = [project_name, *(aliases or [])]
    normalized_terms = [normalise_name(term) for term in search_terms if term]

    best_candidate: Optional[ResolvedCoordinate] = None
    for entry in gazetteer:
        target_names = [entry.name, *entry.aliases]
        target_norm = [normalise_name(name) for name in target_names]

        score = max(
            (
                _token_similarity(term, candidate)
                for term in normalized_terms
                for candidate in target_norm
            ),
            default=0.0,
        )

        if score < 0.65:
            continue

        if jurisdiction:
            jurisdiction_norm = normalise_name(jurisdiction)
            coverage = normalise_name(entry.jurisdiction or "")
            if jurisdiction_norm and jurisdiction_norm not in coverage:
                # Accept if coordinate is within ~75km of a known hint later
                continue

        candidate = ResolvedCoordinate(
            latitude=entry.latitude,
            longitude=entry.longitude,
            confidence="matched_gazetteer",
            score=score,
            method="gazetteer",
            source=entry.source,
            candidate=entry,
        )
        if best_candidate is None or candidate.score > best_candidate.score:
            best_candidate = candidate

    return best_candidate


def _token_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    matches = get_close_matches(a, [b], n=1, cutoff=0.0)
    if not matches:
        return 0.0
    ratio = len(set(a.split()) & set(b.split())) / max(len(a.split()), 1)
    if len(a) > len(b):
        ratio = max(ratio, len(b) / len(a))
    else:
        ratio = max(ratio, len(a) / len(b))
    # Weighted average between difflib proportion and token overlap
    return (ratio + (len(matches[0]) / max(len(a), len(b)))) / 2.0


def create_coordinate_from_text(text: str) -> Optional[ResolvedCoordinate]:
    coords = extract_coordinate_candidates(text)
    if not coords:
        return None
    lat, lon = coords[0]
    return ResolvedCoordinate(
        latitude=lat,
        longitude=lon,
        confidence="direct_coordinate",
        score=1.0,
        method="direct_text",
    )


def haversine_distance_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    hav = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0 * (2 * math.asin(math.sqrt(hav)))


__all__ = [
    "GazetteerEntry",
    "ResolvedCoordinate",
    "extract_text_from_html",
    "extract_coordinate_candidates",
    "extract_location_hints",
    "create_coordinate_from_text",
    "load_gazetteer",
    "match_gazetteer",
    "normalise_name",
    "haversine_distance_km",
]
