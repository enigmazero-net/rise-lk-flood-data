# rise-lk-flood-data

Realtime water-level scraper for Sri Lanka DMC Flood_Map (ArcGIS) that produces `data/flood_cards.json` for a Next.js dashboard.

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install requests

# Optional overrides
export FLOOD_ARCGIS_BASE_URL="https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/Flood_Map/FeatureServer"
export FLOOD_LAYER_IDS="11,12,13"  # comma-separated layer IDs

python scripts/update_flood_data.py
cat data/flood_cards.json
```

The script fails fast if no cards are produced so CI does not commit empty data.

## GitHub Actions

- Scheduled every 10 minutes and manually triggerable via `workflow_dispatch`.
- Configure repository variables `FLOOD_ARCGIS_BASE_URL` and `FLOOD_LAYER_IDS` if the DMC service changes.
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
