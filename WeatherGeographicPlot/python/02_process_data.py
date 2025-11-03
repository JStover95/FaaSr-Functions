from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
import requests
from FaaSr_py.client.py_client_stubs import (
    faasr_get_file,
    faasr_invocation_id,
    faasr_log,
    faasr_put_file,
)
from shapely.geometry import Point


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


def get_file(file_name: str, folder_name: str) -> None:
    """
    Get a file from the FaaSr bucket.
    """
    faasr_get_file(
        local_file=file_name,
        remote_folder=f"{folder_name}/{faasr_invocation_id()}",
        remote_file=file_name,
    )


def put_file(file_name: str, folder_name: str) -> None:
    """
    Put a file to the FaaSr bucket.
    """
    faasr_put_file(
        local_file=file_name,
        remote_folder=f"{folder_name}/{faasr_invocation_id()}",
        remote_file=file_name,
    )


def download_station_data(station_ids: list[str]) -> list[str]:
    files = []

    for station_id in station_ids:
        num_rows = download_data(build_url(station_id), f"{station_id}.csv")
        files.append(f"{station_id}.csv")
        faasr_log(f"Downloaded {num_rows} rows from {station_id}")

    return files


def load_station_data(
    file_name: str,
    start_date: str,
    end_date: str,
) -> gpd.GeoDataFrame:
    df = pd.read_csv(
        file_name,
        dtype={
            "STATION": str,
            "DATE": str,
            "LONGITUDE": float,
            "LATITUDE": float,
            "TMIN": float,
            "TMAX": float,
        },
    )
    faasr_log("--------------- Before filtering ---------------")
    faasr_log(df.head())
    df = df[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]
    faasr_log("--------------- After filtering ---------------")
    faasr_log(df.head())
    geometry = df.apply(lambda row: Point(row["LONGITUDE"], row["LATITUDE"]), axis=1)
    min_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMIN"]], geometry=geometry)
    max_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMAX"]], geometry=geometry)
    faasr_log("--------------- Before concatenating ---------------")
    faasr_log(min_temp_gdf.head())
    faasr_log(max_temp_gdf.head())

    return min_temp_gdf, max_temp_gdf


def load_all_station_data(
    files: list[str],
    start_date: str,
    end_date: str,
) -> gpd.GeoDataFrame:
    min_temp_gdfs: list[gpd.GeoDataFrame] = []
    max_temp_gdfs: list[gpd.GeoDataFrame] = []

    for file in files:
        min_temp_gdf, max_temp_gdf = load_station_data(file, start_date, end_date)
        min_temp_gdfs.append(min_temp_gdf)
        max_temp_gdfs.append(max_temp_gdf)

    min_temp_gdf = pd.concat(min_temp_gdfs).dropna()
    max_temp_gdf = pd.concat(max_temp_gdfs).dropna()

    min_temp_groups = min_temp_gdf[["STATION", "TMIN"]].groupby("STATION")
    max_temp_groups = max_temp_gdf[["STATION", "TMAX"]].groupby("STATION")

    avg_min_temp_gdf = min_temp_groups.mean().reset_index()
    avg_max_temp_gdf = max_temp_groups.mean().reset_index()

    temp_gdf = pd.concat([min_temp_gdf, max_temp_gdf])
    temp_gdf = temp_gdf[["STATION", "geometry"]].drop_duplicates(subset=["STATION"])
    temp_gdf = temp_gdf.merge(avg_min_temp_gdf, on="STATION", how="left")
    temp_gdf = temp_gdf.merge(avg_max_temp_gdf, on="STATION", how="left")
    temp_gdf["TMIN"] = temp_gdf["TMIN"] / 10
    temp_gdf["TMAX"] = temp_gdf["TMAX"] / 10

    return temp_gdf


def process_ghcnd_data(folder_name: str) -> None:
    try:
        # 1. Load input data
        get_file("stations.geojson", folder_name)
        stations = gpd.read_file("stations.geojson")

        faasr_log(f"Loaded input data from folder {folder_name}")

        # 2. Download station data
        station_ids = stations["Station ID"].tolist()
        files = download_station_data(station_ids)

        faasr_log(f"Downloaded station data for {len(station_ids)} stations")

        # 3. Load and process all station data
        last_week = datetime.now() - timedelta(days=28)
        start_date = last_week - timedelta(days=last_week.weekday())
        end_date = start_date + timedelta(days=6)
        temp_gdf = load_all_station_data(
            files,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        faasr_log(
            f"Loaded {len(temp_gdf)} rows of temperature data for week starting {last_week}"
        )

        # 4. Upload the temperature data
        temp_gdf.to_file("temp_gdf.geojson", driver="GeoJSON")
        put_file("temp_gdf.geojson", folder_name)

        faasr_log(f"Saved temperature data to FaaSr bucket {folder_name}")

    except Exception as e:
        import traceback

        faasr_log(f"Error processing data: {e}")
        faasr_log(f"Traceback: {traceback.format_exc()}")
        raise e
