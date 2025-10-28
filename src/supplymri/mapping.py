from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping, Optional


def build_feature(record: Mapping[str, object]) -> dict:
    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        raise ValueError("Record missing latitude/longitude keys")
    properties = {
        key: value
        for key, value in record.items()
        if key not in {"latitude", "longitude"}
    }
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
    }


def export_geojson(records: Iterable[Mapping[str, object]], destination: Path) -> Path:
    features = [build_feature(record) for record in records]
    payload = {"type": "FeatureCollection", "features": features}
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2))
    return destination


def export_folium_map(
    records: Iterable[Mapping[str, object]],
    destination: Path,
    *,
    tiles: str = "OpenStreetMap",
    default_zoom: int = 4,
    default_location: Optional[tuple[float, float]] = None,
) -> Optional[Path]:
    try:
        import folium  # type: ignore
    except ImportError:
        return None

    points = [(float(rec["latitude"]), float(rec["longitude"])) for rec in records]
    if not points:
        return None

    if default_location is None:
        avg_lat = sum(pt[0] for pt in points) / len(points)
        avg_lon = sum(pt[1] for pt in points) / len(points)
        default_location = (avg_lat, avg_lon)

    fmap = folium.Map(location=default_location, zoom_start=default_zoom, tiles=tiles)
    for record in records:
        lat = float(record["latitude"])
        lon = float(record["longitude"])
        popup_lines = [
            f"<strong>{key}</strong>: {value}"
            for key, value in record.items()
            if key not in {"latitude", "longitude"}
        ]
        folium.CircleMarker(
            location=(lat, lon),
            radius=6,
            color="#ff6600",
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup("<br>".join(popup_lines), max_width=400),
        ).add_to(fmap)

    destination.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(destination))
    return destination


__all__ = ["export_geojson", "export_folium_map"]
