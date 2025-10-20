import requests
from FaaSr_py.client.py_client_stubs import faasr_log, faasr_put_file


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


def get_ghcnd_data(folder_name: str, output_name: str, station_id: str):
    """
    Download data from the NOAA Global Historical Climatology Network Daily (GHCND)
    dataset for a specific station and upload it to an S3 bucket.

    Args:
        folder_name: The name of the folder to upload the data to.
        output_name: The name of the file to upload the data to.
        station_id: The ID of the station to download the data from.
    """

    # 1. Build the URL
    url = build_url(station_id)
    faasr_log(f"Downloading data from {url}")

    # 2. Download the file to a local file
    num_rows = download_data(url, output_name)
    faasr_log(f"Downloaded {num_rows} rows from {url}")

    # 3. Upload the file to the S3 bucket
    faasr_put_file(
        local_file=output_name,
        remote_folder=folder_name,
        remote_file=output_name,
    )

    faasr_log(f"Uploaded data to {folder_name}/{output_name}")
