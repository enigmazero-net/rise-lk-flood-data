#!/usr/bin/env python3
"""
Scrape Sri Lanka DMC Flood_Map ArcGIS service and build dashboard cards.

- Loops selected layer IDs from the Flood_Map FeatureServer.
- For each feature, normalises a few key fields into "cards".
- Designed to be run by GitHub Actions on a schedule.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import os
import requests

# ==== CONFIG =================================================================

# Base ArcGIS FeatureServer for Flood_Map (DMC)
# You can override via env var FLOOD_ARCGIS_BASE_URL if needed.
DEFAULT_BASE_URL = (
    "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/"
    "Flood_Map/FeatureServer"
)
FLOOD_ARCGIS_BASE_URL = os.getenv("FLOOD_ARCGIS_BASE_URL", DEFAULT_BASE_URL)

# Layer IDs you want to scrape – update this list to match the layers you care about.
# Example: 11 = River_Basin (from your earlier JSON), adjust as necessary.
LAYER_IDS = [11]  # add more like [11, 12, 13, 14, 16] when ready

# Output path
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUT_PATH = DATA_DIR / "flood_cards.json"


# ==== HELPERS ================================================================

def arcgis_query(layer_id: int) -> Dict[str, Any]:
    """
    Run a basic query on a Flood_Map layer:
    - where=1=1
    - no geometry
    - all attributes
    """
    url = f"{FLOOD_ARCGIS_BASE_URL}/{layer_id}/query"
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "cacheHint": "true",
    }

    print(f"[info] Requesting layer {layer_id}: {url}")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error on layer {layer_id}: {data['error']}")

    return data


def extract_cards_from_layer(layer_id: int, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Turn ArcGIS features for one layer into cards.

    We try common field names you’ve already used:
    - OBJECTID
    - Wshed_Name / SubRivBasN
    - StationName / dsd_name / district_n
    - plus keep all attributes in `raw`.
    """
    features = data.get("features", []) or []
    cards: List[Dict[str, Any]] = []

    for f in features:
        attrs = f.get("attributes", {}) or {}

        object_id = attrs.get("OBJECTID")

        basin = (
            attrs.get("Wshed_Name")
            or attrs.get("SubRivBasN")
            or attrs.get("basin")
        )

        station = (
            attrs.get("StationName")
            or attrs.get("dsd_name")
            or attrs.get("district_n")
        )

        # If you later find fields like "Status", "Flood_Statu", "WaterLevel",
        # you can add them here.
        status = (
            attrs.get("Status")
            or attrs.get("Flood_Status")
            or attrs.get("status")
            or "UNKNOWN"
        )

        card = {
            "id": f"{layer_id}-{object_id}" if object_id is not None else f"{layer_id}-{len(cards)}",
            "layer_id": layer_id,
            "basin": basin,
            "station": station,
            "status": status,
            "raw": attrs,
        }
        cards.append(card)

    print(f"[info] Layer {layer_id}: extracted {len(cards)} cards")
    return cards


def build_payload(all_cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Stable sort: by basin, then station, then layer_id
    all_cards.sort(
        key=lambda c: (
            str(c.get("basin") or ""),
            str(c.get("station") or ""),
            str(c.get("layer_id") or ""),
        )
    )

    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "last_updated_utc": now_utc,
        "source": "rise_lk_flood_scraper",
        "source_url": FLOOD_ARCGIS_BASE_URL,
        "card_count": len(all_cards),
        "cards": all_cards,
    }


def write_output(payload: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = OUTPUT_PATH.with_suffix(".json.tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    tmp_path.replace(OUTPUT_PATH)
    print(f"[info] Wrote {OUTPUT_PATH} ({payload.get('card_count', 0)} cards)")


def main() -> None:
    all_cards: List[Dict[str, Any]] = []

    for layer_id in LAYER_IDS:
        try:
            data = arcgis_query(layer_id)
            cards = extract_cards_from_layer(layer_id, data)
            all_cards.extend(cards)
        except Exception as e:  # noqa: BLE001
            print(f"[error] Failed to process layer {layer_id}: {e}")

    payload = build_payload(all_cards)
    write_output(payload)


if __name__ == "__main__":
    main()
