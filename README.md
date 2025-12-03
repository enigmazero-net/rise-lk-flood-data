# rise-lk-flood-data

Realtime water-level scraper for Sri Lanka DMC Flood_Map (ArcGIS) that produces:
- `data/flood_cards.json` (Flood_Map layers)
- `data/alert_level_stations.json` (gauges above alert/minor/major thresholds from the public dashboard)

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install requests

# Optional overrides (defaults: auto-discover all layers from the service)
export FLOOD_ARCGIS_BASE_URL="https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/Flood_Map/FeatureServer"
export FLOOD_LAYER_IDS="11,12,13"  # comma-separated layer IDs to limit/override
export FLOOD_MAX_AGE_DAYS="7"      # set "" to disable time filtering
export GAUGE_FEATURE_LAYER_URL="https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0"
export ALERT_LEVEL_LOOKBACK_DAYS="4"  # set "" to disable time filtering for alert feed

python scripts/update_flood_data.py
cat data/flood_cards.json
cat data/alert_level_stations.json
```

The script fails fast if no cards are produced so CI does not commit empty data.

## GitHub Actions

- Scheduled every 10 minutes and manually triggerable via `workflow_dispatch`.
- Configure repository variables `FLOOD_ARCGIS_BASE_URL` and `FLOOD_LAYER_IDS` if the DMC service changes.
- Set `FLOOD_MAX_AGE_DAYS` (default 7) to drop stale features when timestamps exist; set to empty string to keep everything.
- Alert-level feed: override `GAUGE_FEATURE_LAYER_URL` or `ALERT_LEVEL_LOOKBACK_DAYS` if the gauges service changes.
- The workflow only commits when `card_count > 0` and `data/flood_cards.json` actually changed.

## Using in Next.js

Fetch the published JSON (e.g. from `raw.githubusercontent.com/<owner>/<repo>/main/data/flood_cards.json`) and render the cards:

```ts
export type FloodCard = {
  id: string;
  layer_id: number;
  basin: string | null;
  station: string | null;
  status: string;
  raw: Record<string, unknown>;
};

export async function getFloodCards(): Promise<FloodCard[]> {
  const res = await fetch(
    "https://raw.githubusercontent.com/<owner>/<repo>/main/data/flood_cards.json",
    { next: { revalidate: 120 } } // ISR-friendly
  );
  if (!res.ok) throw new Error("Failed to load flood cards");
  const data = await res.json();
  return data.cards ?? [];
}
```

Alert-level feed (`data/alert_level_stations.json`) contains `records` with:

- `station`, `basin`, `water_level_m`, `alert_threshold_m`, `minor_threshold_m`, `major_threshold_m`
- `severity` (`alert` | `minor` | `major`) based on the thresholds above
- `observed_at_utc` ISO timestamp and `raw` attributes from `gauges_2_view`
