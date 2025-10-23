from datetime import datetime, timedelta

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from FaaSr_py.client.py_client_stubs import faasr_log, faasr_put_file
from shapely.geometry import Point, Polygon


def download_geo_data(url: str, output_name: str) -> None:
    try:
        response = requests.get(url, timeout=20, stream=True)
        response.raise_for_status()

        with open(output_name, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    except Exception as e:
        faasr_log(f"Error downloading data from {url}: {e}")
        raise


def get_geo_boundaries(
    state_name: str,
    county_name: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    states = gpd.read_file("states.zip")
    counties = gpd.read_file("counties.zip")
    state = states[states["NAME"] == state_name]
    county = counties[counties["NAME"] == county_name]
    includes_county = county.geometry.apply(lambda x: state.geometry.contains(x)).values
    county = county[includes_county]
    return state, county


def get_outer_boundary(
    county: gpd.GeoDataFrame,
    degree_buffer: float = 0.5,
) -> gpd.GeoDataFrame:
    min_x = county.bounds["minx"].iloc[0]
    min_y = county.bounds["miny"].iloc[0]
    max_x = county.bounds["maxx"].iloc[0]
    max_y = county.bounds["maxy"].iloc[0]
    top_left = (min_x - degree_buffer, max_y + degree_buffer)
    top_right = (max_x + degree_buffer, max_y + degree_buffer)
    bottom_right = (max_x + degree_buffer, min_y - degree_buffer)
    bottom_left = (min_x - degree_buffer, min_y - degree_buffer)
    outer_polygon = Polygon([top_left, top_right, bottom_right, bottom_left])
    return gpd.GeoDataFrame(geometry=[outer_polygon])


def get_stations(year: str) -> gpd.GeoDataFrame:
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

    tmax_ids = df[df["Element Type"] == "TMAX"]["Station ID"].unique()
    tmin_ids = df[df["Element Type"] == "TMIN"]["Station ID"].unique()
    ids_with_both = set(tmax_ids) & set(tmin_ids)

    df = (
        df[df["Station ID"].isin(ids_with_both) & (df["End Date"] >= year)]
        .drop_duplicates(subset=["Station ID"])
        .drop(columns=["Element Type", "Begin Date", "End Date"])
    )

    df["geometry"] = df.apply(
        lambda row: Point(row["Longitude"], row["Latitude"]),
        axis=1,
    )

    return gpd.GeoDataFrame(df[["Station ID", "geometry"]])


def upload_boundaries(
    output_folder: str,
    state: gpd.GeoDataFrame,
    county: gpd.GeoDataFrame,
    outer_boundary: gpd.GeoDataFrame,
) -> None:
    state.to_file("state.geojson", driver="GeoJSON")
    county.to_file("county.geojson", driver="GeoJSON")
    outer_boundary.to_file("outer_boundary.geojson", driver="GeoJSON")
    faasr_put_file(
        local_file="state.geojson",
        remote_folder=output_folder,
        remote_file="state.geojson",
    )
    faasr_put_file(
        local_file="county.geojson",
        remote_folder=output_folder,
        remote_file="county.geojson",
    )
    faasr_put_file(
        local_file="outer_boundary.geojson",
        remote_folder=output_folder,
        remote_file="outer_boundary.geojson",
    )


def upload_stations(output_folder: str, stations: gpd.GeoDataFrame) -> None:
    stations.to_file("stations.geojson", driver="GeoJSON")
    faasr_put_file(
        local_file="stations.geojson",
        remote_folder=output_folder,
        remote_file="stations.geojson",
    )


def upload_boundaries_and_stations_plot(
    output_folder: str,
    state: gpd.GeoDataFrame,
    county: gpd.GeoDataFrame,
    outer_boundary: gpd.GeoDataFrame,
    stations: gpd.GeoDataFrame,
) -> None:
    pd.concat([state, county, outer_boundary, stations]).plot(
        facecolor="none",
        edgecolor="black",
        linewidth=0.5,
    )

    plt.savefig("boundaries_and_stations_plot.png")

    faasr_put_file(
        local_file="boundaries_and_stations_plot.png",
        remote_folder=output_folder,
        remote_file="boundaries_and_stations_plot.png",
    )


def get_geo_data_and_stations(
    output_folder: str,
    state_name: str,
    county_name: str,
) -> None:
    try:
        # 1. Download geographic boundary data
        download_geo_data(
            "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_20m.zip",
            "states.zip",
        )
        download_geo_data(
            "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip",
            "counties.zip",
        )

        faasr_log(
            f"Downloaded geographic boundary data for {state_name} and {county_name} county."
        )

        # 2. Get geographic boundary data
        state, county = get_geo_boundaries(state_name, county_name)
        faasr_log(
            f"Retrieved geographic boundary data for {state_name} and {county_name} county."
        )

        # 3. Calculate the outer boundary for station selection
        outer_boundary = get_outer_boundary(county)

        bbox = outer_boundary.bounds
        min_x, min_y, max_x, max_y = bbox.iloc[0]
        faasr_log("Calculated outer boundary for station selection.")
        faasr_log(
            f"(min_x, min_y, max_x, max_y) = ({min_x}, {min_y}, {max_x}, {max_y})"
        )

        # 4. Download station data
        year = (datetime.now() - timedelta(days=7)).strftime("%Y")
        stations = get_stations(year)
        faasr_log(f"Downloaded {len(stations)} stations with data for {year} or later.")

        # 5. Get stations within the outer boundary
        stations = stations.overlay(outer_boundary, how="intersection")
        faasr_log(f"Filtered stations to {len(stations)} within the outer boundary.")

        # 6. Upload the data
        upload_boundaries(output_folder, state, county, outer_boundary)
        upload_stations(output_folder, stations)
        upload_boundaries_and_stations_plot(
            output_folder,
            state,
            county,
            outer_boundary,
            stations,
        )

        faasr_log("Completed get_geo_data_and_stations function.")

    except Exception as e:
        import traceback

        faasr_log(f"Error in get_geo_data_and_stations function: {e}")
        faasr_log(f"Traceback: {traceback.format_exc()}")
        raise
