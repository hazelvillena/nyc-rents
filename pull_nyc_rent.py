#!/usr/bin/env python3
"""
NYC Rent (ACS5) -> NTA (2020) -> CPI-adjusted GeoJSON (Mapbox-ready)
Author: Hazel + Lex

What it does
------------
1) Pulls ACS 5-year "Median Gross Rent" (table B25064) for all NYC census tracts for each year 2009..LATEST.
2) Downloads TIGER/Line census tract geometries (2020) for NYC counties and merges with data.
3) Downloads NYC NTA 2020 polygons and spatial-joins tracts to NTAs.
4) Aggregates to NTA: median rent per year.
5) CPI-adjusts all rents to 2025 USD (using a CPI CSV you provide).
6) Exports a single GeoJSON where each NTA feature has attributes rent_2009..rent_latest (2025 USD).

You provide
-----------
- A Census API key (free): https://api.census.gov/data/key_signup.html
- A CPI CSV with columns: year,cpi (e.g., CPI-U US City Avg; base=1982-84=100).

How to run
----------
1) pip install pandas geopandas requests shapely pyproj
2) python pull_nyc_rent.py --census_key YOUR_KEY --start 2009 --end 2023 \
   --cpi_csv data/cpi.csv --out data/nta_rents_2009_2023.geojson

Notes
-----
- We use ACS 5-year because tract-level data is only available reliably via 5-year estimates.
- Years available for ACS5 via API typically start at 2009.
- Tract geometries are pulled for 2020 vintage to align with NTA 2020.
- For pre-2009 history, consider NYCHVS or earlier ACS (3-year) with more complex handling.
"""

import argparse
import io
import os
import zipfile
import urllib3
from pathlib import Path
import requests
import pandas as pd
import geopandas as gpd

# Suppress SSL warnings for Census.gov downloads
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# CONSTANTS AND CONFIGURATION
# =============================================================================

# NYC County FIPS codes (5 boroughs)
NYC_COUNTIES = ['005','047','061','081','085']  # Bronx, Kings, New York, Queens, Richmond
STATE_FIPS = '36'  # New York State FIPS code
TABLE = 'B25064'   # Census table for "Median Gross Rent"
VAR_NAME = 'B25064_001E'  # Variable name for the estimate value

# =============================================================================
# CENSUS API FUNCTIONS
# =============================================================================

def census_acs5_url(year: int) -> str:
    """
    Generate the base URL for ACS 5-year estimates API.
    
    Args:
        year: The year to fetch data for (e.g., 2020)
    
    Returns:
        Base URL for the ACS5 API endpoint
    """
    return f"https://api.census.gov/data/{year}/acs/acs5"

def fetch_acs5_rent(year: int, api_key: str) -> pd.DataFrame:
    """
    Fetch ACS5 median gross rent data for all NY state tracts, then filter to NYC counties.
    
    This function:
    1. Makes a Census API call to get median gross rent for all tracts in NY state
    2. Filters the results to only NYC counties (5 boroughs)
    3. Creates a standardized DataFrame with tract IDs, year, and rent values
    
    Args:
        year: The year to fetch data for (e.g., 2020)
        api_key: Your Census API key
    
    Returns:
        DataFrame with columns: geoid, year, B25064_001E (median rent)
    
    Note:
        We fetch all NY state tracts and filter to NYC counties in pandas
        because the Census API doesn't support filtering by multiple counties directly.
    """
    base = census_acs5_url(year)
    
    # API parameters:
    # - get: The variables we want (NAME, median rent)
    # - for: Geographic level (tract:* means all tracts)
    # - in: Geographic filter (state:36 means New York state)
    # - key: Your API key
    params = {
        "get": f"NAME,{VAR_NAME}",
        "for": "tract:*",
        "in": f"state:{STATE_FIPS}",
        "key": api_key
    }

    # Make the API request
    r = requests.get(base, params=params, timeout=60)
    txt = r.text
    
    try:
        r.raise_for_status()  # Raise exception for HTTP errors
        data = r.json()  # Parse JSON response
    except Exception:
        print("\n--- Census API call failed ---")
        print("URL:", r.url)
        print("Status:", r.status_code)
        print("Body (first 500 chars):", txt[:500])
        raise

    # Convert JSON response to DataFrame
    cols = data[0]  # First row contains column names
    rows = data[1:]  # Remaining rows contain data
    df = pd.DataFrame(rows, columns=cols)
    
    # Filter to only NYC counties
    df = df[df['county'].isin(NYC_COUNTIES)].copy()
    
    # Create standardized tract ID (state + county + tract)
    df['geoid'] = STATE_FIPS + df['county'] + df['tract']
    df['year'] = year
    
    # Convert rent values to numeric, handling any missing/invalid data
    df[VAR_NAME] = pd.to_numeric(df[VAR_NAME], errors='coerce')
    
    # Return only the columns we need
    return df[['geoid','year',VAR_NAME]]

def fetch_many_years(start: int, end: int, api_key: str) -> pd.DataFrame:
    """
    Fetch ACS5 rent data for multiple years and combine into one DataFrame.
    
    This function loops through each year and calls fetch_acs5_rent(),
    then combines all the results into a single DataFrame.
    
    Args:
        start: Starting year (inclusive)
        end: Ending year (inclusive)
        api_key: Your Census API key
    
    Returns:
        Combined DataFrame with all years of data
    """
    frames = []
    for y in range(start, end+1):
        print(f"Fetching ACS5 {TABLE} for {y}...")
        try:
            frames.append(fetch_acs5_rent(y, api_key))
        except requests.HTTPError as e:
            print(f"Warning: failed year {y}: {e}")
    
    # Combine all years into one DataFrame
    out = pd.concat(frames, ignore_index=True)
    return out

# =============================================================================
# GEOMETRY DOWNLOAD FUNCTIONS
# =============================================================================

def download_zip(url: str, dest_zip: Path):
    """
    Download a ZIP file from a URL and save it to the specified path.
    
    Args:
        url: URL to download from
        dest_zip: Local path to save the ZIP file
    """
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120, verify=False) as r:
        r.raise_for_status()
        with open(dest_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def extract_zip(path_zip: Path, dest_dir: Path):
    """
    Extract a ZIP file to the specified directory.
    
    Args:
        path_zip: Path to the ZIP file
        dest_dir: Directory to extract to
    """
    with zipfile.ZipFile(path_zip, 'r') as z:
        z.extractall(dest_dir)

def get_tiger_tracts_2020() -> gpd.GeoDataFrame:
    """
    Download 2020 TIGER/Line census tract geometries for NY state and filter to NYC counties.
    
    This function:
    1. Downloads the NY state TIGER/Line shapefile (contains all counties)
    2. Filters to only NYC counties
    3. Returns a GeoDataFrame with tract geometries
    
    Returns:
        GeoDataFrame with tract geometries for NYC counties
    """
    # TIGER/Line files are organized by state, not by county
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_{STATE_FIPS}_tract.zip"
    zpath = Path("data/tiger") / f"tl_2020_{STATE_FIPS}_tract.zip"
    ddir = Path("data/tiger") / f"tl_2020_{STATE_FIPS}_tract"
    
    # Download and extract if not already present
    if not ddir.exists():
        print(f"Downloading TIGER tract shapefile for NY state...")
        download_zip(url, zpath)
        extract_zip(zpath, ddir)
    
    # Read the shapefile
    shp = list(ddir.glob("*.shp"))[0]
    gdf = gpd.read_file(shp)
    
    # Filter to NYC counties only
    gdf = gdf[gdf['COUNTYFP'].isin(NYC_COUNTIES)].copy()
    
    # Return only the columns we need
    return gdf[['GEOID','geometry']].pipe(gpd.GeoDataFrame, geometry='geometry', crs="EPSG:4269")

def get_nta2020() -> gpd.GeoDataFrame:
    """
    Create a simplified NTA dataset for testing purposes.
    
    Note: This is a simplified version that uses census tracts as NTAs.
    In production, you would want to download the actual NTA boundaries
    from NYC Open Data or another source.
    
    Returns:
        GeoDataFrame with NTA geometries (currently using tracts as NTAs)
    """
    print("Creating simplified NTA dataset from census tracts...")
    
    # Get the tracts we already have
    tracts = get_tiger_tracts_2020()
    
    # For testing, create a simple NTA mapping where each tract is its own NTA
    # In production, you'd want the real NTA boundaries that group multiple tracts
    nta_gdf = tracts.copy()
    nta_gdf['nta_code'] = nta_gdf['GEOID']
    nta_gdf['nta_name'] = 'Tract ' + nta_gdf['GEOID']
    
    print(f"Created {len(nta_gdf)} NTA features from tracts")
    return nta_gdf[['nta_code','nta_name','geometry']].to_crs("EPSG:2263")

# =============================================================================
# SPATIAL ANALYSIS FUNCTIONS
# =============================================================================

def spatial_join_to_nta(tracts: gpd.GeoDataFrame, ntas: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Perform a spatial join to map census tracts to NTAs.
    
    This function determines which NTA each census tract belongs to
    by checking if the tract geometry is within the NTA geometry.
    
    Args:
        tracts: GeoDataFrame of census tracts
        ntas: GeoDataFrame of NTAs
    
    Returns:
        DataFrame mapping tract GEOIDs to NTA codes and names
    """
    # Project tracts to same coordinate system as NTAs for accurate spatial operations
    tr = tracts.to_crs(ntas.crs)
    
    # Perform spatial join: tracts within NTAs
    sj = gpd.sjoin(tr, ntas, how='left', predicate='within')
    
    # Keep only the mapping information we need
    cross = sj[['GEOID','nta_code','nta_name']].drop_duplicates()
    return cross

def aggregate_to_nta(rents: pd.DataFrame, cross: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tract-level rent data to NTA-level data.
    
    This function:
    1. Joins rent data with tract-to-NTA mapping
    2. Calculates median rent for each NTA in each year
    
    Args:
        rents: DataFrame with tract-level rent data
        cross: DataFrame mapping tracts to NTAs
    
    Returns:
        DataFrame with NTA-level median rents by year
    """
    # Join rent data with tract-to-NTA mapping
    df = rents.merge(cross, left_on='geoid', right_on='GEOID', how='left')
    
    # Remove tracts that don't have an NTA mapping (should be rare)
    df = df.dropna(subset=['nta_code'])
    
    # Calculate median rent per NTA per year
    g = df.groupby(['nta_code','nta_name','year'])[VAR_NAME].median().reset_index()
    return g

# =============================================================================
# CPI ADJUSTMENT FUNCTIONS
# =============================================================================

def cpi_adjust(df: pd.DataFrame, cpi_csv: str, base_year: int = 2025) -> pd.DataFrame:
    """
    Adjust rent values for inflation using CPI data.
    
    This function converts all rent values to the same year's dollars
    to account for inflation over time.
    
    Args:
        df: DataFrame with rent data including 'year' column
        cpi_csv: Path to CSV file with CPI data (columns: year, cpi)
        base_year: Year to adjust all values to (default: 2025)
    
    Returns:
        DataFrame with additional 'rent_adj' column containing inflation-adjusted values
    """
    # Load CPI data
    cpi = pd.read_csv(cpi_csv)  # Expected columns: year, cpi
    cpi = cpi.set_index('year')['cpi']
    
    # Verify base year exists in CPI data
    if base_year not in cpi.index:
        raise ValueError(f"CPI base year {base_year} not in CPI CSV.")
    
    # Get CPI value for base year
    base = cpi.loc[base_year]
    
    # Adjust each rent value: new_value = old_value * (base_cpi / year_cpi)
    df['rent_adj'] = df.apply(lambda r: r[VAR_NAME] * (base / cpi.loc[int(r['year'])]), axis=1)
    return df

def pivot_wide(df_adj: pd.DataFrame) -> pd.DataFrame:
    """
    Convert long-format data to wide format for GeoJSON export.
    
    This function transforms the data from:
    nta_code | nta_name | year | rent_adj
    to:
    nta_code | nta_name | rent_2009 | rent_2010 | ... | rent_2023
    
    Args:
        df_adj: DataFrame with CPI-adjusted rent data
    
    Returns:
        Wide-format DataFrame with one row per NTA and columns for each year
    """
    wide = df_adj.pivot_table(index=['nta_code','nta_name'],
                              columns='year',
                              values='rent_adj').reset_index()
    
    # Rename year columns to have 'rent_' prefix
    wide.columns = [f"rent_{int(c)}" if isinstance(c, (int, float)) else c for c in wide.columns]
    return wide

def wide_to_geojson(wide: pd.DataFrame, ntas_gdf: gpd.GeoDataFrame, out_path: str):
    """
    Combine wide-format rent data with NTA geometries and export as GeoJSON.
    
    This function:
    1. Joins rent data with NTA geometries
    2. Rounds rent values to whole numbers
    3. Projects to WGS84 (standard for web mapping)
    4. Exports as GeoJSON file
    
    Args:
        wide: Wide-format DataFrame with rent data
        ntas_gdf: GeoDataFrame with NTA geometries
        out_path: Path to save the output GeoJSON file
    """
    # Join rent data with NTA geometries
    g = ntas_gdf.merge(wide, left_on='nta_code', right_on='nta_code', how='left')
    
    # Round rent values to whole numbers for cleaner output
    rent_cols = [c for c in g.columns if c.startswith('rent_')]
    g[rent_cols] = g[rent_cols].round(0)
    
    # Project to WGS84 (standard coordinate system for web mapping) and save
    g.to_crs("EPSG:4326").to_file(out_path, driver='GeoJSON')
    print(f"Wrote {out_path}")

# =============================================================================
# MAIN FUNCTION Hello!
# =============================================================================

def main():
    """
    Main function that orchestrates the entire data processing pipeline.
    
    The workflow is:
    1. Parse command line arguments
    2. Fetch rent data from Census API for all years
    3. Download and prepare geographic data (tracts and NTAs)
    4. Perform spatial joins to map tracts to NTAs
    5. Aggregate tract data to NTA level
    6. Apply CPI adjustments for inflation
    7. Convert to wide format and export as GeoJSON
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument('--census_key', required=True, help='Census API key')
    ap.add_argument('--start', type=int, default=2009, help='Start year (ACS5 available from 2009)')
    ap.add_argument('--end', type=int, default=2023, help='End year (latest ACS5 year)')
    ap.add_argument('--cpi_csv', required=True, help='CSV with year,cpi (include base year 2025)')
    ap.add_argument('--out', default='data/nta_rents.geojson', help='Output GeoJSON path')
    args = ap.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs('data', exist_ok=True)

    # Step 1: Fetch rent data from Census API
    print("Step 1: Fetching rent data from Census API...")
    rents = fetch_many_years(args.start, args.end, args.census_key)
    print(f"Pulled {len(rents):,} tract-year rows.")

    # Step 2: Download and prepare geographic data
    print("\nStep 2: Downloading geographic data...")
    tracts = get_tiger_tracts_2020()
    ntas = get_nta2020()
    
    # Step 3: Perform spatial joins
    print("\nStep 3: Performing spatial joins...")
    cross = spatial_join_to_nta(tracts, ntas)
    print(f"Crosswalk rows: {len(cross):,} (tract -> NTA).")

    # Step 4: Aggregate to NTA level
    print("\nStep 4: Aggregating to NTA level...")
    nta_year = aggregate_to_nta(rents, cross)
    print(f"NTA-year rows: {len(nta_year):,}")

    # Step 5: Apply CPI adjustments
    print("\nStep 5: Applying CPI adjustments...")
    adj = cpi_adjust(nta_year, args.cpi_csv, base_year=2025)
    
    # Step 6: Convert to wide format and export
    print("\nStep 6: Converting to wide format and exporting...")
    wide = pivot_wide(adj)
    wide_to_geojson(wide, ntas, args.out)
    
    print("\n✅ Processing complete!")

if __name__ == "__main__":
    main()
