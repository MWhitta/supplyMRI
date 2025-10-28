#!/usr/bin/env python3
"""
Generate spatial outputs for EDGAR mining exhibits.
"""

import argparse
import sys
from pathlib import Path
from typing import List

from supplymri.edgar_locations import EdgarProject, build_projects, resolve_with_gazetteer
from supplymri.location_utils import ResolvedCoordinate
from supplymri.mapping import export_folium_map, export_geojson


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve mine coordinates from EDGAR exhibits and prepare map artifacts."
    )
    parser.add_argument(
        "--edgar-root",
        type=Path,
        default=Path("data/edgar"),
        help="Directory containing downloaded EDGAR filings (default: data/edgar).",
    )
    parser.add_argument(
        "--gazetteer",
        type=Path,
        help="Optional CSV/JSON/GeoJSON gazetteer file with mine coordinates.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of exhibits to process (default: %(default)s).",
    )
    parser.add_argument(
        "--geojson-output",
        type=Path,
        default=Path("data/edgar/mine_sites.geojson"),
        help="Path for the generated GeoJSON file.",
    )
    parser.add_argument(
        "--html-output",
        type=Path,
        default=Path("data/edgar/mine_sites.html"),
        help="Path for the generated HTML map (requires folium).",
    )
    return parser.parse_args()


def project_to_record(project: EdgarProject, resolved: ResolvedCoordinate) -> dict:
    record = {
        "company": project.company,
        "project": project.project,
        "jurisdiction": project.jurisdiction,
        "latitude": resolved.latitude,
        "longitude": resolved.longitude,
        "confidence": resolved.confidence,
        "score": resolved.score,
        "method": resolved.method,
        "metadata_path": str(project.metadata_path),
        "document_path": str(project.document_path),
    }
    if resolved.source:
        record["source"] = resolved.source
    if project.location_hints:
        record["hints"] = "; ".join(project.location_hints[:3])
    return record


def main() -> int:
    args = parse_args()
    if not args.edgar_root.exists():
        print(f"error: EDGAR directory {args.edgar_root} does not exist", file=sys.stderr)
        return 1

    projects = build_projects(args.edgar_root, limit=args.limit)
    resolve_with_gazetteer(projects, args.gazetteer)

    resolved_records: List[dict] = []
    for project in projects:
        if not project.resolved:
            print(f"[warn] Unable to resolve coordinates for {project.project}", file=sys.stderr)
            continue
        resolved_records.append(project_to_record(project, project.resolved))

    if not resolved_records:
        print("No projects with resolved coordinates were found.", file=sys.stderr)
        return 1

    geojson_path = export_geojson(resolved_records, args.geojson_output)
    html_path = export_folium_map(resolved_records, args.html_output)

    print(f"GeoJSON saved to {geojson_path}")
    if html_path:
        print(f"Interactive map saved to {html_path}")
    else:
        print("folium is not installed; skipped HTML map generation.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
