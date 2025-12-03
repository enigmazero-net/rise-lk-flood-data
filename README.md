# rise-lk-flood-data

Realtime water-level scraper for Sri Lanka DMC’s public `gauges_2_view` layer.  
Outputs every 10 minutes (via GitHub Actions):
- `data/gauges_2_view.json` — metadata + all gauge rows
- `data/gauges_2_view.csv` — same data as CSV

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install requests

# Optional overrides
export GAUGE_FEATURE_LAYER_URL="https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0"
export GAUGE_WHERE="1=1"  # set a custom where clause if you need filtering

python scripts/update_flood_data.py
cat data/gauges_2_view.json
head data/gauges_2_view.csv
```

The script fails fast if no rows are returned to avoid committing empty data.

## GitHub Actions

- Scheduled every 10 minutes and manually triggerable via `workflow_dispatch`.
- Configure repository variables `GAUGE_FEATURE_LAYER_URL` or `GAUGE_WHERE` if the service changes.
- The workflow only commits when `record_count > 0` and output files actually changed.

## Using in Next.js

Fetch the published JSON (e.g. from `raw.githubusercontent.com/<owner>/<repo>/main/data/gauges_2_view.json`) and render the gauges:

```ts
export type GaugeRecord = {
  objectid: number;
  basin: string | null;
  gauge: string | null;
  water_level: number | null;
  rain_fall: number | null;
  CreationDate?: string;
  [key: string]: unknown;
};

export async function getGaugeRecords(): Promise<GaugeRecord[]> {
  const res = await fetch(
    "https://raw.githubusercontent.com/<owner>/<repo>/main/data/gauges_2_view.json",
    { next: { revalidate: 120 } } // ISR-friendly
  );
  if (!res.ok) throw new Error("Failed to load gauge data");
  const data = await res.json();
  return data.records ?? [];
}
```

All timestamp-ish fields (e.g. `CreationDate`, `EditDate`) are normalised to ISO 8601 strings.
