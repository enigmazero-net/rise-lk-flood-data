#!/usr/bin/env python3
"""
Pull the public gauges_2_view layer and persist the latest gauge readings.

- Fetches all rows (paged) from the FeatureServer layer.
- Normalises timestamp fields to ISO 8601 strings.
- Writes both JSON (metadata + records) and CSV for easy use elsewhere.
- Intended to run on CI every 10 minutes.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==== CONFIG =================================================================

# gauges_2_view layer used by the public dashboard (water level / rainfall)
DEFAULT_GAUGE_FEATURE_LAYER_URL = (
    "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/"
    "gauges_2_view/FeatureServer/0"
)

# Timeouts / retries
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 25
MAX_RETRIES = 3

# Paging
PAGE_SIZE = 1000

# HTTP headers to look like a browser (helps avoid being blocked)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Referer": "https://slirrigation.maps.arcgis.com/",
}

# Output paths
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
JSON_OUTPUT_PATH = DATA_DIR / "gauges_2_view.json"
CSV_OUTPUT_PATH = DATA_DIR / "gauges_2_view.csv"


# ==== HELPERS ================================================================

def _resolve_gauge_url() -> str:
    raw = os.getenv("GAUGE_FEATURE_LAYER_URL")
    if raw is None:
        return DEFAULT_GAUGE_FEATURE_LAYER_URL

    cleaned = raw.strip()
    if not cleaned:
        print("[warn] GAUGE_FEATURE_LAYER_URL is empty; using default.")
        return DEFAULT_GAUGE_FEATURE_LAYER_URL

    if "://" not in cleaned:
        raise SystemExit(
            "[error] GAUGE_FEATURE_LAYER_URL must include a scheme "
            "(e.g. https://services3.arcgis.com/.../FeatureServer/0)."
        )

    return cleaned.rstrip("/")


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
    session.headers.update(DEFAULT_HEADERS)
    return session


def coerce_datetime(value: Any) -> str | None:
    """
    ArcGIS stores timestamps as epoch milliseconds.
    Also handle epoch seconds and ISO strings.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
        except Exception:
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return datetime.fromisoformat(stripped.replace("Z", "+00:00")).isoformat()
        except Exception:
            try:
                seconds = float(stripped)
                if seconds > 1e12:
                    seconds /= 1000.0
                return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
            except Exception:
                return None
    return None


def normalise_record(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert timestamp-ish fields to ISO strings so they are easy to use later.
    """
    record: Dict[str, Any] = dict(attrs)
    for key, value in list(record.items()):
        if "date" in key.lower() or "time" in key.lower():
            iso = coerce_datetime(value)
            if iso:
                record[key] = iso
    return record


def paged_gauge_query(session: requests.Session, url: str, where: str) -> List[Dict[str, Any]]:
    """
    Query the gauges_2_view layer with paging (maxRecordCount=1000).
    """
    features: List[Dict[str, Any]] = []
    result_offset = 0

    while True:
        params = {
            "f": "json",
            "where": where or "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": PAGE_SIZE,
            "resultOffset": result_offset,
            "cacheHint": "true",
            "orderByFields": "CreationDate DESC",
        }

        resp = session.get(
            f"{url}/query",
            params=params,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error on gauges query: {data['error']}")

        page = data.get("features", []) or []
        features.extend(page)
        if len(page) < PAGE_SIZE:
            break
        result_offset += PAGE_SIZE

    return features


def build_json_payload(records: List[Dict[str, Any]], source_url: str, where: str) -> Dict[str, Any]:
    return {
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        "source": "rise_lk_gauges_scraper",
        "source_url": source_url,
        "where": where,
        "record_count": len(records),
        "records": records,
    }


def write_json(payload: Dict[str, Any], path: Path) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)
    print(f"[info] Wrote {path} ({payload.get('record_count', 0)} records)")


def write_csv(records: Iterable[Dict[str, Any]], path: Path) -> None:
    records_list = list(records)
    if not records_list:
        return
    fieldnames: List[str] = sorted({k for rec in records_list for k in rec.keys()})
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records_list)
    tmp_path.replace(path)
    print(f"[info] Wrote {path} ({len(records_list)} records)")


# ==== MAIN ===================================================================

def main() -> None:
    gauge_url = _resolve_gauge_url()
    where = os.getenv("GAUGE_WHERE", "1=1")

    session = build_session()
    print(f"[info] Querying gauges layer: {gauge_url} with where={where!r}")
    features = paged_gauge_query(session, gauge_url, where)
    if not features:
        raise SystemExit("[error] No records returned from gauges_2_view")

    records = [normalise_record(f.get("attributes", {}) or {}) for f in features]
    payload = build_json_payload(records, source_url=gauge_url, where=where)
    write_json(payload, JSON_OUTPUT_PATH)
    write_csv(records, CSV_OUTPUT_PATH)


if __name__ == "__main__":
    main()
