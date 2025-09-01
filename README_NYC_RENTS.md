# NYC Rent - Mapbox (Starter)
Quick steps to pull ACS data (median gross rent), roll up to NYC NTAs, CPI-adjust to 2025 USD, and export a GeoJSON ready for Mapbox tiles.

## 0) Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate  # or use your setup
pip install pandas geopandas requests shapely pyproj
# (Mac) brew install tippecanoe
```

## 1) Get a Census API key
Free: https://api.census.gov/data/key_signup.html

## 2) Prepare CPI CSV
Create `data/cpi.csv` with columns:
```
year,cpi
2009,214.537
2010,218.056
...
2023,305.109
2025,???   # Put the most recent annual CPI value; update later when final.
```
(Use CPI-U, US city average. You can replace with a more precise series as needed.)

## 3) Run the script
```bash
python pull_nyc_rent.py \
  --census_key YOUR_API_KEY \
  --start 2009 \
  --end 2023 \
  --cpi_csv data/cpi.csv \
  --out data/nta_rents_2009_2023.geojson
```

## 4) Tippecanoe to tiles
```bash
tippecanoe -o data/nyc_rents.mbtiles data/nta_rents_2009_2023.geojson \
  -zg --drop-densest-as-needed --include=nta_code --include=nta_name
```

Upload `data/nyc_rents.mbtiles` to Mapbox Studio (Tilesets), then add as a source in your style.

## 5) Mapbox GL usage (JS snippet)
```js
map.addSource('rents', { type: 'vector', url: 'mapbox://YOUR_USERNAME.nyc_rents' });
map.addLayer({
  id: 'rents-3d',
  type: 'fill-extrusion',
  source: 'rents',
  'source-layer': 'nta_rents_2009_2023', // set this to your actual layer name
  paint: {
    'fill-extrusion-height': ['get', 'rent_2023'], // or dynamic via UI
    'fill-extrusion-opacity': 0.9
  }
});
```

## Notes
- We use ACS **5-year** because tract-level is only stable there; earliest via API is ~2009.
- If you want pre-2009, consider NYCHVS or Furman Center, and be explicit about caveats.
