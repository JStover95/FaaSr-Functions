import requests
from FaaSr_py.client.py_client_stubs import faasr_log, faasr_put_file


def get_ghcnd_data(folder_name: str, output_name: str, station_id: str):
    """
    Download data from the NOAA Global Historical Climatology Network Daily (GHCND) dataset
    and upload it to an S3 bucket.

    Args:
        folder_name: The name of the folder to upload the data to.
        output_name: The name of the file to upload the data to.
        station_id: The ID of the station to download the data from.
    """

    # 1. Build the URL
    base_url = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
    url = f"{base_url}/{station_id}.csv"

    # 2. Download the file to a local file
    faasr_log(f"Downloading data from {url}")

    try:
        response = requests.get(url, timeout=20)

        with open(output_name, "w") as f:
            f.write(response.text)

        num_rows = len(response.text.split("\n")) - 1  # Subtract 1 for the header row
        faasr_log(f"Downloaded {num_rows} rows from {url}")

    except requests.exceptions.RequestException as e:
        faasr_log(f"Error downloading data from {url}: {e}")
        raise e

    except Exception as e:
        faasr_log(f"Unknown error downloading data from {url}: {e}")
        raise e

    # 3. Upload the file to the S3 bucket
    faasr_log(f"Uploading data to {folder_name}/{output_name}")

    faasr_put_file(
        local_file=output_name,
        remote_folder=folder_name,
        remote_file=output_name,
    )

    faasr_log("Data successfully uploaded")
