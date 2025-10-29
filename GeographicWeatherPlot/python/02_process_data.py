from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
import requests
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log
from shapely.geometry import Point


def get_input_data(output_folder: str, input_name: str) -> None:
    faasr_get_file(
        local_file=input_name,
        remote_folder=output_folder,
        remote_file=input_name,
    )


def build_url(station_id: str) -> str:
    """
    Build the URL for the NOAA Global Historical Climatology Network Daily (GHCND)
    dataset for a specific station.

    Args:
        station_id: The ID of the station to download the data from.

    Returns:
        The URL to download the data from.
    """
    base_url = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
    return f"{base_url}/{station_id}.csv"


def download_data(url: str, output_name: str) -> int:
    """
    Download data from the NOAA Global Historical Climatology Network Daily (GHCND)
    dataset for a specific station and save it to a local file.

    Args:
        url: The URL to download the data from.
        output_name: The name of the file to save the data to.

    Returns:
        The number of rows downloaded.
    """
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()

        with open(output_name, "w") as f:
            f.write(response.text)

        return len(response.text.split("\n")) - 1  # Subtract 1 for the header row

    except Exception as e:
        faasr_log(f"Error downloading data from {url}: {e}")
        raise e


def download_station_data(station_ids: list[str]) -> list[str]:
    files = []

    for station_id in station_ids:
        num_rows = download_data(build_url(station_id), f"{station_id}.csv")
        files.append(f"{station_id}.csv")
        faasr_log(f"Downloaded {num_rows} rows from {station_id}")

    return files


def load_station_data(file: str, start_date: str, end_date: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(
        f"data/{file}",
        dtype={
            "STATION": str,
            "DATE": str,
            "LONGITUDE": float,
            "LATITUDE": float,
            "TMIN": float,
            "TMAX": float,
        },
    )
    df = df[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]
    geometry = df.apply(lambda row: Point(row["LONGITUDE"], row["LATITUDE"]), axis=1)
    min_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMIN"]], geometry=geometry)
    max_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMAX"]], geometry=geometry)
    return min_temp_gdf, max_temp_gdf


def load_all_station_data(
    files: list[str],
    start_date: str,
    end_date: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    min_temp_gdfs: list[gpd.GeoDataFrame] = []
    max_temp_gdfs: list[gpd.GeoDataFrame] = []

    for file in files:
        min_temp_gdf, max_temp_gdf = load_station_data(file, start_date, end_date)
        min_temp_gdfs.append(min_temp_gdf)
        max_temp_gdfs.append(max_temp_gdf)

    return pd.concat(min_temp_gdfs), pd.concat(max_temp_gdfs)


def process_data(output_folder: str) -> None:
    # 1. Load input data
    get_input_data(output_folder, "county.geojson")
    get_input_data(output_folder, "outer_boundary.geojson")
    get_input_data(output_folder, "state.geojson")
    get_input_data(output_folder, "stations.geojson")

    county = gpd.read_file("county.geojson")
    outer_boundary = gpd.read_file("outer_boundary.geojson")
    state = gpd.read_file("state.geojson")
    stations = gpd.read_file("stations.geojson")

    faasr_log(f"Loaded input data from folder {output_folder}")

    # 2. Download station data
    station_ids = stations["Station ID"].tolist()
    files = download_station_data(station_ids)

    faasr_log(f"Downloaded station data for {len(station_ids)} stations")

    last_week = datetime.now() - timedelta(days=7)
    start_date = last_week - timedelta(days=last_week.weekday())
    end_date = start_date + timedelta(days=6)
    min_temp_gdf, max_temp_gdf = load_all_station_data(files, start_date, end_date)

    faasr_log(
        f"Loaded {len(min_temp_gdf)} rows of minimum temperature data and {len(max_temp_gdf)} rows of maximum temperature data for week starting {last_week}"
    )
