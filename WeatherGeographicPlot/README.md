# Geographic Weather Plot Workflow

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Understanding our Data](#understanding-our-data)
- [Writing our Functions](#writing-our-functions)
  - [1. Get our Data](#1-get-our-data)
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

This tutorial highlights using timestamped invocation IDs and using the workflow's invocation ID to create a unique folder for each workflow run. For example:

```plaintext
bucket-name/
├── 2025-01-01/  # Outputs for workflow run on January 1, 2025
├── 2025-01-08/  # Outputs for workflow run on January 8, 2025
└── 2025-01-15/  # Outputs for workflow run on January 15, 2025
```

Below is an example of the visualization we will be creating:

![Temperature heatmap example plot](../assets/weather-geographic-plot-600px.png)

## Prerequisites

This example function assumes you already completed the FaaSr tutorial ([https://faasr.io/FaaSr-Docs/tutorial/](https://faasr.io/FaaSr-Docs/tutorial/)) and have the necessary repositories and configuration set up. This tutorial will use the FaaSr/FaaSr-Functions repo as the function code source repository, but you may use your own repository as you follow along.

## Understanding our Data

For this tutorial, we are working with two data sources: the [NOAA Global Historical Climatology Network Daily (GHCND) dataset](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) and [US Census Bureau Cartographic Boundary Data](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html).

> ℹ️ Please refer to the **Understanding our Data** section of the [Weather Visualization Tutorial](../WeatherVisualization/README.md#understanding-our-data) readme for details on the GHCND dataset.

To create a geographic plot, we will use the GHCND [inventory metadata](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt) to locate stations nearby our region of interest. Inspecting this file, we can see we have station IDs, geographic coordinates, variables, and the start and end years of data collection, for example:

```plaintext
Station ID    Latitude  Longitude   Data    Start   End
--------------------------------------------------------
USC00351862   44.6342   -123.1900   TMAX    1893    2025
USC00351862   44.6342   -123.1900   TMIN    1893    2025
```

The US Census Bureau Cartographic Boundary Data contains boundaries for states, counties, congressional districts, etc. We will use the state and county geographic boundary data to select our stations and for plotting. For this tutorial, we will use Benton County, Oregon.

## Writing our Functions

### 1. Get our Data

This first function in our workflow will pull the US Census Bureau boundary data and the GHCND inventory metadata, then upload the geographic data needed for the rest of our workflow:

- `county.geojson`: The boundary data of our county of interest.
- `outer_boundary.geojson`: An outer boundary used to select our stations.
- `state.geojson`: The boundary data of the state containing our county.
- `stations.geojson`: The coordinates of each station we will use for our visualization.

First, we will write our imports:

- `datetime`: We will use this to get the current year.
- `geopandas`: A library for working with tabulated geographic data.
- `pandas`: A library for working with tabulated data, which geopandas is based on.
- `requests`: We will use requests to download data from public URLs.
- `faasr_log`: This will write log outputs to S3.
- `faasr_put_file`: We will use this function for storing our output data on S3.
- `faasr_invocation_id`: Retrieves the current invocation ID (in this tutorial, a timestamp), which we will use to make sure each run will not overwrite data from previous runs.
- `Point` and `Polygon`: Data types for manipulating geographic coordinates and boundaries.

```python
from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests
from FaaSr_py.client.py_client_stubs import (
    faasr_invocation_id,
    faasr_log,
    faasr_put_file,
)
from shapely.geometry import Point, Polygon
```

Next we will need some functions for getting and uploading our data. `download_url` handles downloading data from a public URL and saving it as a local file. `put_file` handles uploading data to the workflow's FaaSr bucket. Here we pass `f"{output_folder}/{faasr_invocation_id()}"` as the `remote_folder`, ensuring that data from each run is uploaded to a unique folder based on the invocation ID.

> ℹ️ Note that we wrapped the download in a try/except block. This allows us to use faasr_log to record an error, simplifying troubleshooting if the download fails.

```py
def download_data(url: str, output_name: str) -> None:
    """
    Download data from a URL and save it to a local folder.

    Args:
        url: The URL to download the data from.
        output_name: The name of the file to save the data to.
    """
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()

        with open(output_name, "wb") as f:
            f.write(response.content)

    except Exception as e:
        faasr_log(f"Error downloading data from {url}: {e}")
        raise


def put_file(file_name: str, output_folder: str) -> None:
    """
    Put a file to the FaaSr folder.

    Args:
        file_name: The name of the file to put.
        output_folder: The name of the folder to put the file in.
    """
    faasr_put_file(
        local_file=file_name,
        remote_folder=f"{output_folder}/{faasr_invocation_id()}",
        remote_file=file_name,
    )
```

Next we will need two functions for handling our geographic data. The first, `get_geo_boundaries` retrieves our state's and county's boundaries. [US counties in different states can have the same name](https://www.fws.gov/sites/default/files/documents/Standard_CountyName.pdf), so we use the GeoDataFrame `contains` method to ensure we get only the county within our state boundaries.

> ℹ️ This function uses boolean indexing to locate specific rows. For more information see pandas [Indexing and selecting data](https://pandas.pydata.org/docs/user_guide/indexing.html). \
> ℹ️ This function uses apply for efficient operations on DataFrames. For more information refer to the [pandas documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.apply.html).

```python
def get_geo_boundaries(
    state_name: str,
    county_name: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Get the geographic boundaries for a given state and county. This will load
    `states.zip` and `counties.zip` from the working directory and then filter
    the data to the given state and county.

    Args:
        state_name: The name of the state to get the boundaries for.
        county_name: The name of the county to get the boundaries for.

    Returns:
        A tuple containing the state and county GeoDataFrames.
    """
    states = gpd.read_file("states.zip")
    counties = gpd.read_file("counties.zip")
    state = states[states["NAME"] == state_name]
    county = counties[counties["NAME"] == county_name]

    # Get only the county within the state
    includes_county = county.geometry.apply(lambda x: state.geometry.contains(x)).values
    county = county[includes_county]

    return state, county
```

The second, `get_outer_boundary` adds a buffer to the outer limits of our county bounds. This is illustrated in the following plot, with the outer boundary highlighted in blue. This outer boundary ensures that the heatmap interpolation between our selected stations gives complete coverage for our county.

![Outer boundary example figure](../assets/weather-geographic-outer-boundary-600px.png)

> ℹ️ This function uses `Polygon` and `GeoDataFrame` data types for working with geographic boundaries. Refer to the [Shapely documentation](https://shapely.readthedocs.io/en/stable/manual.html#polygons) and [GeoPandas documentation](https://geopandas.org/en/stable/docs/user_guide/data_structures.html#geodataframe) for more detail.

```python
def get_outer_boundary(
    county: gpd.GeoDataFrame,
    degree_buffer: float = 0.5,
) -> gpd.GeoDataFrame:
    """
    Get the outer boundary for a given county. This adds `degree_buffer` degrees to the
    maximum and minimum latitude and longitude.

    Args:
        county: The county GeoDataFrame.
        degree_buffer: The number of degrees to add to the maximum and minimum latitude
            and longitude.

    Returns:
        A GeoDataFrame containing the outer boundary.
    """

    # Get the minimum and maximum latitude and longitude
    min_x = county.bounds["minx"].iloc[0]
    min_y = county.bounds["miny"].iloc[0]
    max_x = county.bounds["maxx"].iloc[0]
    max_y = county.bounds["maxy"].iloc[0]

    # Add the buffer to the minimum and maximum latitude and longitude
    top_left = (min_x - degree_buffer, max_y + degree_buffer)
    top_right = (max_x + degree_buffer, max_y + degree_buffer)
    bottom_right = (max_x + degree_buffer, min_y - degree_buffer)
    bottom_left = (min_x - degree_buffer, min_y - degree_buffer)

    outer_polygon = Polygon([top_left, top_right, bottom_right, bottom_left])
    return gpd.GeoDataFrame(geometry=[outer_polygon])
```

Next we must get our GHCND inventory metadata. According to the GHCND documentation, the inventory metadata is available as a _fixed width file_, which we read using `pandas.read_fwf`. The `dtype` and `colspecs` arguments define the data types and widths of each column.

We then find all station IDs with maximum temperature or minimum temperature, then taking the _intersection_ of these two sets yields all stations that have _both_ data available for our visualization.

We perform a final transformation that filters the inventory metadata for only stations with data available for the given year or later, initializes a `geometry` column using each station's latitude and longitude, then creates a GeoDataFrame with the station IDs and their coordinates.

> ℹ️ This function uses `Point` for working with geographic coordinates. Refer to the [Shapely documentation](https://shapely.readthedocs.io/en/stable/manual.html#Point) for more information.

```python
def get_stations(year: str) -> gpd.GeoDataFrame:
    """
    Get all stations with TMAX and TMIN data on or after the given year. This will
    download the station inventory data from the NOAA Global Historical Climatology
    Network Daily (GHCND) dataset and filter the data to the given year.

    Args:
        year: The year to get the stations for.

    Returns:
        A GeoDataFrame containing the stations with TMAX and TMIN data on or after
        the given year.
    """

    # Download the station inventory data
    df = pd.read_fwf(
        "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt",
        header=None,
        dtype={0: str, 1: float, 2: str, 3: str, 4: str, 5: str},
        colspecs=[(0, 11), (12, 20), (21, 30), (31, 35), (36, 40), (41, 45)],
    )

    df.columns = [
        "Station ID",
        "Latitude",
        "Longitude",
        "Element Type",
        "Begin Date",
        "End Date",
    ]

    # Get the station IDs with both TMAX and TMIN data
    tmax_ids = df[df["Element Type"] == "TMAX"]["Station ID"].unique()
    tmin_ids = df[df["Element Type"] == "TMIN"]["Station ID"].unique()
    ids_with_both = set(tmax_ids) & set(tmin_ids)

    # Filter the data to the year and only include stations with both TMAX and TMIN data
    df = (
        df[df["Station ID"].isin(ids_with_both) & (df["End Date"] >= year)]
        .drop_duplicates(subset=["Station ID"])
        .drop(columns=["Element Type", "Begin Date", "End Date"])
    )

    # Create a geometry column for the stations
    df["geometry"] = df.apply(
        lambda row: Point(row["Longitude"], row["Latitude"]),
        axis=1,
    )

    return gpd.GeoDataFrame(df[["Station ID", "geometry"]])
```

Finally, we will write a single function that:

1. Downloads the geographic boundary data.
2. Gets our state and county geographic boundary data.
3. Calculates the outer boundary for station selection.
4. Downloads station inventory metadata for the current year.
5. Gets the stations within the outer boundary using the GeoDataFrame `overlay` method.
6. Uploads our data to the FaaSr bucket.

This function will be called by FaaSr, so we will configure the `folder_name`, `state_name`, and `county_name` arguments when building our workflow.

```python
def get_geo_data_and_stations(
    folder_name: str,
    state_name: str,
    county_name: str,
) -> None:
    """
    Get the geographic boundaries and stations for a given state and county. This will
    download the geographic boundary data from the Census Bureau and then filter the
    data to the given state and county. It will then get the stations with TMAX and
    TMIN data on or after the given year.

    Args:
        folder_name: The name of the folder to upload the data to.
        state_name: The name of the state to get the boundaries for.
        county_name: The name of the county to get the boundaries for.
    """
    # 1. Download geographic boundary data
    download_data(
        "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_20m.zip",
        "states.zip",
    )
    download_data(
        "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_5m.zip",
        "counties.zip",
    )
    faasr_log(f"Downloaded boundary data for {state_name} and {county_name} county.")

    # 2. Get geographic boundary data
    state, county = get_geo_boundaries(state_name, county_name)
    faasr_log(f"Retrieved geographic data for {state_name} and {county_name} county.")

    # 3. Calculate the outer boundary for station selection
    outer_boundary = get_outer_boundary(county)

    # 4. Download station data
    year = datetime.now().year
    stations = get_stations(year)
    faasr_log(f"Downloaded {len(stations)} stations with data for {year} or later.")

    # 5. Get stations within the outer boundary
    stations = stations.overlay(outer_boundary, how="intersection")
    faasr_log(f"Filtered stations to {len(stations)} within the outer boundary.")

    # 6. Upload the data
    state.to_file("state.geojson", driver="GeoJSON")
    county.to_file("county.geojson", driver="GeoJSON")
    outer_boundary.to_file("outer_boundary.geojson", driver="GeoJSON")
    stations.to_file("stations.geojson", driver="GeoJSON")

    put_file("state.geojson", folder_name)
    put_file("county.geojson", folder_name)
    put_file("outer_boundary.geojson", folder_name)
    put_file("stations.geojson", folder_name)

    faasr_log("Completed get_geo_data_and_stations function.")
```

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
