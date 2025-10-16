from datetime import datetime, timedelta

import pandas as pd
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_put_file


def compare_to_yearly_average(
    folder_name: str,
    input_name: str,
    output_name: str,
    column_name: str,
    start: str,
    end: str,
):
    """
    Compare the values for this year to the average of the same 30 day period from the previous 10 years.

    This returns two DataFrames with the following columns:
    - DAY: MM-DD
    - COLUMN(s): Daily value(s)
    """
    # Download the input file
    faasr_get_file(
        local_file=input_name,
        remote_folder=folder_name,
        remote_file=input_name,
    )

    # Read the input file
    df = pd.read_csv(input_name)

    # Get the last 30 days of this year
    this_year = df[(df["DATE"] >= start) & (df["DATE"] <= end)].copy()

    # Get the same 30-day period from the previous 10 years
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    # Create a list to store data from previous years
    previous_years_data = []

    # Get data for the same period from the previous 10 years
    for year_offset in range(1, 11):  # 1 to 10 years back
        start_date_prev = start_date - timedelta(days=365 * year_offset)
        end_date_prev = end_date - timedelta(days=365 * year_offset - 30)

        start_date_str = start_date_prev.strftime("%Y-%m-%d")
        end_date_str = end_date_prev.strftime("%Y-%m-%d")

        # Get data for this year
        year_data = df[
            (df["DATE"] >= start_date_str) & (df["DATE"] <= end_date_str)
        ].copy()

        if len(year_data) > 0:
            # Convert date to MM-DD format for comparison
            year_data["DAY"] = year_data["DATE"].apply(lambda x: x[5:])
            previous_years_data.append(year_data[["DAY", column_name]])

    # Create current year data with DAY format
    current_year = this_year.copy()
    current_year["DAY"] = current_year["DATE"].apply(lambda x: x[5:])
    current_year_df = current_year[["DAY", column_name]]

    # Calculate average precipitation for each day across previous years
    if previous_years_data:
        # Combine all previous years data
        all_prev_data = pd.concat(previous_years_data, ignore_index=True)

        # Group by DAY and calculate mean precipitation
        avg_prev_data = all_prev_data.groupby("DAY")[column_name].mean().reset_index()
        avg_prev_data.columns = ["DAY", column_name]
    else:
        # If no previous data, create empty DataFrame with same structure
        avg_prev_data = pd.DataFrame(columns=["DAY", column_name])

    # Save the output files to local files
    current_year_df.to_csv(f"current_year_{output_name}", index=False)
    avg_prev_data.to_csv(f"avg_prev_data_{output_name}", index=False)

    # Drop leap day
    current_year_df = current_year_df[current_year_df["DAY"] != "02-29"]
    avg_prev_data = avg_prev_data[avg_prev_data["DAY"] != "02-29"]

    # Upload the output files
    faasr_put_file(
        local_file=f"current_year_{output_name}",
        remote_folder=folder_name,
        remote_file=f"current_year_{output_name}",
    )

    faasr_put_file(
        local_file=f"avg_prev_data_{output_name}",
        remote_folder=folder_name,
        remote_file=f"avg_prev_data_{output_name}",
    )
