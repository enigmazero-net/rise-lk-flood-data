#!/usr/bin/env python3
"""
Scrape Sri Lanka DMC Flood_Map ArcGIS service and build dashboard cards.

- Loops selected layer IDs from the Flood_Map FeatureServer.
- For each feature, normalises a few key fields into "cards".
- Designed to be run by GitHub Actions on a schedule.
- Configure layer IDs via FLOOD_LAYER_IDS (comma-separated) and base URL via FLOOD_ARCGIS_BASE_URL.
- Fails fast if no cards are produced to avoid committing empty data.
- Filters out features older than FLOOD_MAX_AGE_DAYS (default 7) when timestamps are present.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==== CONFIG =================================================================

# Base ArcGIS FeatureServer for Flood_Map (DMC)
# You can override via env var FLOOD_ARCGIS_BASE_URL if needed.
DEFAULT_BASE_URL = (
    "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/"
    "Flood_Map/FeatureServer"
)

# Layer IDs you want to scrape – update this list to match the layers you care about.
# You can override via env var FLOOD_LAYER_IDS="11,12,13".
DEFAULT_LAYER_IDS = [11]

# Timeouts / retries
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 25
MAX_RETRIES = 3

# Drop cards older than this (days). Set FLOOD_MAX_AGE_DAYS="" to disable filtering.
DEFAULT_MAX_AGE_DAYS = 7

# Output path
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUT_PATH = DATA_DIR / "flood_cards.json"
TIME_FIELDS = (
    "timestamp",
    "Timestamp",
    "time",
    "Time",
    "date",
    "Date",
    "datetime",
    "Datetime",
    "DATETIME",
    "last_update",
    "LastUpdate",
)


def _resolve_base_url() -> str:
    raw = os.getenv("FLOOD_ARCGIS_BASE_URL")
    if raw is None:
        return DEFAULT_BASE_URL

    cleaned = raw.strip()
    if not cleaned:
        print("[warn] FLOOD_ARCGIS_BASE_URL is empty; using default.")
        return DEFAULT_BASE_URL

    if "://" not in cleaned:
        raise SystemExit(
            "[error] FLOOD_ARCGIS_BASE_URL must include a scheme "
            "(e.g. https://services3.arcgis.com/...)."
        )

    return cleaned.rstrip("/")


FLOOD_ARCGIS_BASE_URL = _resolve_base_url()


# ==== HELPERS ================================================================

def build_session() -> requests.Session:
    """Create a requests session with limited retries/backoff."""
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def parse_layer_ids(raw: str) -> List[int]:
    """Parse comma-separated layer IDs from env."""
    ids: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        ids.append(int(part))
    return ids


def discover_layer_ids(base_url: str) -> List[int]:
    """Ask the FeatureServer root for its published layer IDs."""
    url = f"{base_url}?f=json"
    try:
        resp = requests.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Failed to auto-discover layers from {url}: {exc}")
        return []

    layers = data.get("layers") or []
    ids: List[int] = []
    for layer in layers:
        lid = layer.get("id")
        if isinstance(lid, int):
            ids.append(lid)
    return ids


def parse_max_age_days() -> int | None:
    raw = os.getenv("FLOOD_MAX_AGE_DAYS", str(DEFAULT_MAX_AGE_DAYS))
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"[error] Invalid FLOOD_MAX_AGE_DAYS: {raw}") from exc


def coerce_datetime(value: Any) -> datetime | None:
    """Try to parse ArcGIS-style timestamps (epoch ms/s) or ISO strings."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except Exception:
            try:
                seconds = float(stripped)
                if seconds > 1e12:
                    seconds /= 1000.0
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
            except Exception:
                return None
    return None


def extract_feature_time(attrs: Dict[str, Any]) -> datetime | None:
    for key in TIME_FIELDS:
        if key in attrs:
            dt = coerce_datetime(attrs.get(key))
            if dt:
                return dt
    return None


def is_recent(attrs: Dict[str, Any], now_utc: datetime, max_age_days: int | None) -> bool:
    """Keep features that are newer than max_age_days when a timestamp is available."""
    if max_age_days is None:
        return True
    feature_time = extract_feature_time(attrs)
    if feature_time is None:
        return True  # no timestamp; keep
    cutoff = now_utc - timedelta(days=max_age_days)
    return feature_time >= cutoff


def arcgis_query(session: requests.Session, layer_id: int) -> Dict[str, Any]:
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
    resp = session.get(
        url,
        params=params,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error on layer {layer_id}: {data['error']}")

    return data


def extract_cards_from_layer(
    layer_id: int, data: Dict[str, Any], now_utc: datetime, max_age_days: int | None
) -> List[Dict[str, Any]]:
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

        if not is_recent(attrs, now_utc, max_age_days):
            continue

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
    raw_layer_ids = os.getenv("FLOOD_LAYER_IDS")
    if raw_layer_ids:
        layer_ids = parse_layer_ids(raw_layer_ids)
    else:
        discovered = discover_layer_ids(FLOOD_ARCGIS_BASE_URL)
        if discovered:
            layer_ids = discovered
            print(f"[info] Auto-discovered layers: {layer_ids}")
        else:
            layer_ids = list(DEFAULT_LAYER_IDS)
            print(f"[warn] Discovery returned 0 layers; using default: {layer_ids}")

    if not layer_ids:
        raise SystemExit("[error] No layer IDs configured (set FLOOD_LAYER_IDS).")

    all_cards: List[Dict[str, Any]] = []
    session = build_session()
    max_age_days = parse_max_age_days()
    print(f"[info] Using layers: {layer_ids}")
    print(f"[info] Max age days: {max_age_days if max_age_days is not None else 'disabled'}")
    now_utc = datetime.now(timezone.utc)

    for layer_id in layer_ids:
        try:
            data = arcgis_query(session, layer_id)
            cards = extract_cards_from_layer(layer_id, data, now_utc, max_age_days)
            if not cards:
                print(f"[warn] Layer {layer_id} returned 0 features.")
            all_cards.extend(cards)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to process layer {layer_id}") from exc

    if not all_cards:
        raise SystemExit("[error] No cards extracted from any layer. Failing run.")

    payload = build_payload(all_cards)
    write_output(payload)


if __name__ == "__main__":
    main()
