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


def get_file(file_name: str, folder_name: str) -> None:
    """
    Get a file from the FaaSr bucket.

    Args:
        file_name: The name of the file to get from the FaaSr bucket.
        folder_name: The name of the folder to get the file from.
    """
    faasr_get_file(
        local_file=file_name,
        remote_folder=f"{folder_name}/{faasr_invocation_id()}",
        remote_file=file_name,
    )


def put_file(file_name: str, folder_name: str) -> None:
    """
    Put a file to the FaaSr bucket.

    Args:
        file_name: The name of the file to put to the FaaSr bucket.
        folder_name: The name of the folder to put the file to.
    """
    faasr_put_file(
        local_file=file_name,
        remote_folder=f"{folder_name}/{faasr_invocation_id()}",
        remote_file=file_name,
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


def download_station(url: str, output_name: str) -> int:
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


def download_all_stations(station_ids: list[str]) -> list[str]:
    """
    Download data from the NOAA Global Historical Climatology Network Daily (GHCND)
    dataset for a list of stations and save it to a local file.

    Args:
        station_ids: The IDs of the stations to download the data from.

    Returns:
        A list of the file names of the files downloaded.
    """
    files = []

    for station_id in station_ids:
        num_rows = download_station(build_url(station_id), f"{station_id}.csv")
        files.append(f"{station_id}.csv")
        faasr_log(f"Downloaded {num_rows} rows from {station_id}")

    return files


def get_temperature_data(
    file_name: str,
    start_date: str,
    end_date: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Get the temperature data for a given station and date range.

    Args:
        file_name: The name of the file to get the data from.
        start_date: The start date to get the data from.
        end_date: The end date to get the data to.

    Returns:
        A tuple containing the minimum and maximum temperature data as GeoDataFrames.
        The first element of the tuple is the minimum temperature data and the second
        element is the maximum temperature data.
    """
    # Load the data into a pandas DataFrame
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

    # Filter the data to the date range
    df = df[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]

    # Create a geometry column for the data
    geometry = df.apply(lambda row: Point(row["LONGITUDE"], row["LATITUDE"]), axis=1)

    # Create GeoDataFrames for the minimum and maximum temperature data
    min_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMIN"]], geometry=geometry)
    max_temp_gdf = gpd.GeoDataFrame(df[["STATION", "DATE", "TMAX"]], geometry=geometry)

    return min_temp_gdf, max_temp_gdf


def get_all_temperature_data(
    files: list[str],
    start_date: str,
    end_date: str,
) -> gpd.GeoDataFrame:
    """
    Get the average temperature data for all stations and date range.

    Args:
        files: The list of files to get the data from.
        start_date: The start date to get the data from.
        end_date: The end date to get the data to.

    Returns:
        A GeoDataFrame containing the average temperature data.
    """
    min_temp_gdfs: list[gpd.GeoDataFrame] = []
    max_temp_gdfs: list[gpd.GeoDataFrame] = []

    # Get the temperature data for each station
    for file in files:
        min_temp_gdf, max_temp_gdf = get_temperature_data(file, start_date, end_date)
        min_temp_gdfs.append(min_temp_gdf)
        max_temp_gdfs.append(max_temp_gdf)

    # Concatenate the minimum and maximum temperature data
    min_temp_gdf = pd.concat(min_temp_gdfs).dropna()
    max_temp_gdf = pd.concat(max_temp_gdfs).dropna()

    # Get the average temperature data for each station
    min_temp_groups = min_temp_gdf[["STATION", "TMIN"]].groupby("STATION")
    max_temp_groups = max_temp_gdf[["STATION", "TMAX"]].groupby("STATION")
    avg_min_temp_gdf = min_temp_groups.mean().reset_index()
    avg_max_temp_gdf = max_temp_groups.mean().reset_index()

    # Create a single GeoDataFrame for the average temperature data
    temp_gdf = pd.concat([min_temp_gdf, max_temp_gdf])[["STATION", "geometry"]]
    temp_gdf = temp_gdf.drop_duplicates(subset=["STATION"])
    temp_gdf = temp_gdf.merge(avg_min_temp_gdf, on="STATION", how="left")
    temp_gdf = temp_gdf.merge(avg_max_temp_gdf, on="STATION", how="left")

    # Convert the temperature data to whole degrees Celsius
    temp_gdf["TMIN"] = temp_gdf["TMIN"] / 10
    temp_gdf["TMAX"] = temp_gdf["TMAX"] / 10

    return temp_gdf


def process_ghcnd_data(folder_name: str) -> None:
    """
    Process the GHCND temperature data for the selected stations and upload the
    output data to the FaaSr bucket.
    """
    # 1. Load input data
    get_file("Stations.geojson", folder_name)
    stations = gpd.read_file("Stations.geojson")
    faasr_log(f"Loaded input data from folder {folder_name}")

    # 2. Download station data
    station_ids = stations["Station ID"].tolist()
    files = download_all_stations(station_ids)
    faasr_log(f"Downloaded station data for {len(station_ids)} stations")

    # 3. Process all station data
    now = datetime.strptime(faasr_invocation_id(), "%Y-%m-%d-%H-%M-%S")
    prev_week = now - timedelta(days=28)
    start_date = prev_week - timedelta(days=prev_week.weekday())
    end_date = start_date + timedelta(days=6)
    temp_gdf = get_all_temperature_data(
        files,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    faasr_log(
        f"Loaded {len(temp_gdf)} rows of temperature data for week starting {prev_week}"
    )

    # 4. Upload the temperature data
    temp_gdf.to_file("TemperatureData.geojson", driver="GeoJSON")
    put_file("TemperatureData.geojson", folder_name)

    faasr_log(f"Saved temperature data to FaaSr bucket {folder_name}")
