# Geographic Weather Plot Workflow

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Understanding our Data](#understanding-our-data)
- [Writing our Functions](#writing-our-functions)
  - [1. Get geographic boundaries and stations](#1-get-geographic-boundaries-and-stations)
  - [2. Process weekly temperature data](#2-process-weekly-temperature-data)
  - [3. Plot geographic heatmaps](#3-plot-geographic-heatmaps)
- [Building our Workflow](#building-our-workflow)
  - [1. Set Up our Compute Server](#1-set-up-our-compute-server)
  - [2. Set Up our Data Store](#2-set-up-our-data-store)
  - [3. Add our Functions](#3-add-our-functions)
  - [4. Connect our Functions](#4-connect-our-functions)
  - [5. Finalize our Workflow Configuration](#5-finalize-our-workflow-configuration)
- [Download and Invoke the Workflow](#download-and-invoke-the-workflow)
  - [Download the Workflow](#download-the-workflow)
  - [Register and Invoke the Workflow](#register-and-invoke-the-workflow)
  - [View the Output Data](#view-the-output-data)

## Key Topics

- Using timestamp invocation IDs
- Writing functions
- Adding Python packages

## Introduction

The Geographic Weather Plot Workflow demonstrates a geospatial FaaSr use case using timestamped invocation IDs for plotting historic temperature data. It downloads US state and county boundary data and identifies NOAA Global Historical Climatology Network Daily (GHCND) stations in and around a target county. Then, it plots a heatmap of average temperatures from a recent week.

In this tutorial we will build a three-step workflow that automates these tasks and stores intermediate GeoJSON outputs and a final PNG plot to S3.

```mermaid
flowchart LR
  A["Get Data"] --> B["Process Data"]
  B --> C["Plot Data"]
```

Below is an example of the visualization we will be creating:

## Prerequisites

Complete the FaaSr tutorial first (see `https://faasr.io/FaaSr-Docs/tutorial/`) so you have:

- a FaaSr-workflow repo with the register/invoke GitHub Actions configured
- access to an S3-compatible data store and credentials saved in secrets
- this repository available as your function code source

## Understanding our Data

- Geographic boundaries are sourced from the US Census TIGER/Line 2018 generalized boundary shapefiles:
  - States: `https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_20m.zip`
  - Counties: `https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_5m.zip`
- Station locations and availability come from the NOAA GHCND inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- Station observations are downloaded as CSVs per-station from: `https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{STATION_ID}.csv`

We focus on stations that report both `TMIN` and `TMAX`. Temperatures are reported in tenths of degrees Celsius; we convert to °C.

## Writing our Functions

### 1. Get geographic boundaries and stations

File: `./python/01_get_data.py`

We start by downloading boundary archives and reading them with GeoPandas:

```python
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point, Polygon
from FaaSr_py.client.py_client_stubs import faasr_log, faasr_put_file

def download_data(url: str, output_name: str) -> None:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    with open(output_name, "wb") as f:
        f.write(response.content)
```

We filter the state and county by name, and compute an “outer boundary” box extended by a degree buffer to capture nearby stations:

```python
def get_geo_boundaries(state_name: str, county_name: str) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    states = gpd.read_file("states.zip")
    counties = gpd.read_file("counties.zip")
    state = states[states["NAME"] == state_name]
    county = counties[counties["NAME"] == county_name]
    includes_county = county.geometry.apply(lambda x: state.geometry.contains(x)).values
    county = county[includes_county]
    return state, county

def get_outer_boundary(county: gpd.GeoDataFrame, degree_buffer: float = 0.5) -> gpd.GeoDataFrame:
    min_x = county.bounds["minx"].iloc[0]; min_y = county.bounds["miny"].iloc[0]
    max_x = county.bounds["maxx"].iloc[0]; max_y = county.bounds["maxy"].iloc[0]
    top_left = (min_x - degree_buffer, max_y + degree_buffer)
    top_right = (max_x + degree_buffer, max_y + degree_buffer)
    bottom_right = (max_x + degree_buffer, min_y - degree_buffer)
    bottom_left = (min_x - degree_buffer, min_y - degree_buffer)
    return gpd.GeoDataFrame(geometry=[Polygon([top_left, top_right, bottom_right, bottom_left])])
```

We query the GHCND inventory for stations with both `TMIN` and `TMAX` and `End Date >= year` (using the current year minus 7 days), then clip to our outer boundary, and upload GeoJSON outputs to S3:

```python
def get_stations(year: str) -> gpd.GeoDataFrame:
    df = pd.read_fwf(
        "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt",
        header=None,
        dtype={0: str, 1: float, 2: str, 3: str, 4: str, 5: str},
        colspecs=[(0, 11), (12, 20), (21, 30), (31, 35), (36, 40), (41, 45)],
    )
    df.columns = ["Station ID", "Latitude", "Longitude", "Element Type", "Begin Date", "End Date"]
    ids_with_both = set(df[df["Element Type"] == "TMAX"]["Station ID"]) & set(df[df["Element Type"] == "TMIN"]["Station ID"]) 
    df = df[df["Station ID"].isin(ids_with_both) & (df["End Date"] >= year)]\
         .drop_duplicates(subset=["Station ID"])\
         .drop(columns=["Element Type", "Begin Date", "End Date"]) 
    df["geometry"] = df.apply(lambda r: Point(r["Longitude"], r["Latitude"]), axis=1)
    return gpd.GeoDataFrame(df[["Station ID", "geometry"]])

def get_geo_data_and_stations(folder_name: str, state_name: str, county_name: str) -> None:
    download_data("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_20m.zip", "states.zip")
    download_data("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_5m.zip", "counties.zip")
    state, county = get_geo_boundaries(state_name, county_name)
    outer_boundary = get_outer_boundary(county)
    stations = get_stations((datetime.now() - timedelta(days=7)).strftime("%Y"))
    stations = stations.overlay(outer_boundary, how="intersection")
    # Upload state.geojson, county.geojson, outer_boundary.geojson, stations.geojson
```

Arguments configured in the workflow:

- `folder_name`: GeographicWeatherPlot
- `state_name`: Oregon
- `county_name`: Benton

### 2. Process weekly temperature data

File: `./python/02_process_data.py`

This step downloads station CSVs, filters to a single recent week, averages `TMIN` and `TMAX` by station, converts to °C, and uploads a merged GeoJSON of points with average temps.

Key helpers:

```python
def build_url(station_id: str) -> str: ...
def download_station_data(station_ids: list[str]) -> list[str]: ...
def load_station_data(file_name: str, start_date: str, end_date: str) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: ...

def load_all_station_data(files: list[str], start_date: str, end_date: str) -> gpd.GeoDataFrame:
    # concat, groupby mean per station; merge to keep geometry; divide temps by 10
    return temp_gdf

def process_ghcnd_data(folder_name: str) -> None:
    # fetch stations.geojson from S3
    # download CSVs for all stations
    # compute weekly average TMIN/TMAX per station
    # save/upload temp_gdf.geojson
```

Arguments:

- `folder_name`: GeographicWeatherPlot

Python packages: `geopandas`

### 3. Plot geographic heatmaps

File: `./python/03_plot_data.py`

We interpolate station values over a grid using SciPy and render two subplots: weekly minimum and maximum temperature heatmaps. State and county boundaries are overlaid, plot limits and aspect ratio are set from the region bounds, ticks every 0.5°.

```python
from scipy.interpolate import griddata

def create_grid(gdf):
    minx, miny, maxx, maxy = get_bounds(gdf)
    x = np.linspace(minx, maxx, 100); y = np.linspace(miny, maxy, 100)
    return np.meshgrid(x, y)

def create_heatmap(ax, values, points, X_grid, Y_grid, title, cmap):
    interpolation = griddata(points, values, (X_grid, Y_grid), method="cubic", fill_value=np.nan)
    im = ax.contourf(X_grid, Y_grid, interpolation, levels=20, cmap=cmap, alpha=0.8)
    ax.scatter(points[:,0], points[:,1], c=values, s=50, cmap=cmap, edgecolors="black", linewidth=0.5)
    plt.colorbar(im, ax=ax, label="Temperature (°C)")

def plot_county_weekly_temperature(folder_name: str):
    # load outer_boundary.geojson, temp_gdf.geojson, state.geojson, county.geojson
    # build grid from outer boundary; scatter points from station geometries
    # subplot 1 (TMIN, Blues_r), subplot 2 (TMAX, Reds)
    # overlay boundaries, set limits/aspect, set ticks, save/upload temperature_heatmap.png
```

Arguments:

- `folder_name`: GeographicWeatherPlot

Python packages: `geopandas`, `matplotlib`, `numpy`, `scipy`

## Building our Workflow

You can build this workflow with the FaaSr Workflow Builder: `https://faasr.io/FaaSr-workflow-builder/`.

For reference, see the sample workflow file in this repo: `geographic_weather_plot_for_testing.json`.

### 1. Set Up our Compute Server

Click **Edit Compute Servers** and configure to use your GitHub settings (see the main tutorial). Use `GH` for the default GitHub Actions server.

### 2. Set Up our Data Store

Click **Edit Data Stores** and add your S3-compatible store (endpoint, bucket, region). Ensure it’s writable and selected as default.

### 3. Add our Functions

Create three actions under **Edit Actions/Functions**:

- GetData
  - Function Name: `get_geo_data_and_stations`
  - Language: Python; Compute Server: GH
  - Function's Git Repo/Path: `FaaSr/FaaSr-Functions/GeographicWeatherPlot/python`
  - Arguments:
    - `folder_name`: GeographicWeatherPlot
    - `state_name`: Oregon
    - `county_name`: Benton
  - Python Packages: `geopandas`

- ProcessData
  - Function Name: `process_ghcnd_data`
  - Language: Python; Compute Server: GH
  - Function's Git Repo/Path: `FaaSr/FaaSr-Functions/GeographicWeatherPlot/python`
  - Arguments:
    - `folder_name`: GeographicWeatherPlot
  - Python Packages: `geopandas`

- PlotData
  - Function Name: `plot_county_weekly_temperature`
  - Language: Python; Compute Server: GH
  - Function's Git Repo/Path: `FaaSr/FaaSr-Functions/GeographicWeatherPlot/python`
  - Arguments:
    - `folder_name`: GeographicWeatherPlot
  - Python Packages: `geopandas`, `matplotlib`, `numpy`, `scipy`

### 4. Connect our Functions

- Set `GetData` → InvokeNext: `ProcessData`
- Set `ProcessData` → InvokeNext: `PlotData`

### 5. Finalize our Workflow Configuration

- Workflow Name: `GeographicWeatherPlotWorkflow`
- Entry Point: `GetData`

Optionally, export your configuration by clicking **Download** to save a JSON file (e.g., the included `geographic_weather_plot_for_testing.json`).

## Download and Invoke the Workflow

### Download the Workflow

In the builder, click **Download** and save the JSON. Alternatively, reuse the provided JSON and adjust values as needed.

### Register and Invoke the Workflow

In your `FaaSr-workflow` repo:

1. Go to **Actions** → **(FAASR REGISTER)** → Run workflow with the JSON filename (e.g., `geographic_weather_plot_for_testing.json`).
2. After registration completes, run **(FAASR INVOKE)** with the same filename.
3. Monitor runs for `GeographicWeatherPlot-GetData`, `GeographicWeatherPlot-ProcessData`, and `GeographicWeatherPlot-PlotData` in the left pane.

### View the Output Data

After a successful invocation, your S3 bucket should contain:

```plaintext
your-bucket/
├── FaaSrLog/
└── GeographicWeatherPlot/
    ├── state.geojson
    ├── county.geojson
    ├── outer_boundary.geojson
    ├── stations.geojson
    ├── temp_gdf.geojson
    └── temperature_heatmap.png
```

- `FaaSrLog` contains logs for troubleshooting
- `GeographicWeatherPlot` contains intermediate GeoJSONs and the final heatmap image

That’s it—you now have an automated, reproducible geospatial weather visualization workflow in FaaSr.


