# Weather Visualization Workflow

## Table of Contents

- [Key Topics](#key-topics)
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Understanding our Data](#understanding-our-data)
- [Writing our Functions](#writing-our-functions)
  - [1. Get our Data](#1-get-our-data)
  - [2. Process our Data](#2-process-our-data)
  - [3. Plot our Data](#3-plot-our-data)
- [Building our Workflow](#building-our-workflow)
  - [1. Set Up our Compute Server](#1-set-up-our-compute-server)
  - [2. Set Up our Data Store](#2-set-up-our-data-store)
  - [3. Add our Functions](#3-add-our-functions)
    - [Get Data Function](#get-data-function)
    - [Process Data Functions](#process-data-functions)
    - [Plot Data Function](#plot-data-function)
  - [4. Connect our Functions](#4-connect-our-functions)
  - [5. Finalize our Workflow Configuration](#5-finalize-our-workflow-configuration)
- [Download and Invoke the Workflow](#download-and-invoke-the-workflow)
  - [Download the Workflow](#download-the-workflow)
  - [Register and Invoke the Workflow](#register-and-invoke-the-workflow)
  - [View the Output Data](#view-the-output-data)
- [Create a Conditional Workflow](#create-a-conditional-workflow)
  - [Updating our Function Code](#updating-our-function-code)
  - [Updating our Workflow](#updating-our-workflow)
- [Create a Data Stores Workflow](#create-a-data-stores-workflow)
  - [Updating our Function Code for Data Stores](#updating-our-function-code-for-data-stores)
  - [Updating our Workflow for Data Stores](#updating-our-workflow-for-data-stores)

## Key Topics

- Writing functions
- Invoking multiple functions in parallel
- Duplicating functions
- Adding Python packages
- Workflow Builder GUI
- Creating conditional workflows
- Using custom data stores

## Introduction

The Weather Visualization Workflow is an example of a common FaaSr use case. It pulls data from the NOAA Global Historical Climatology Network Daily (GHCND) dataset, processes the data, and creates a visualization that is uploaded to S3. This tutorial is a comprehensive walkthrough of the process for creating your own workflows, from writing the code to viewing its outputs.

```mermaid
flowchart LR
  01["Get Data"]
  02a["Process Precipitation"]
  02b["Process Temperature Min"]
  02c["Process Temperature Max"]
  03["Plot Data"]

  01 --> 02a
  01 --> 02b
  01 --> 02c
  02a --> 03
  02b --> 03
  02c --> 03
```

This tutorial also includes sections on creating workflow variants:

- **Conditional Workflow** (see [Create a Conditional Workflow](#create-a-conditional-workflow)): Demonstrates how to use conditional invocations to automatically fall back to alternative data sources when primary data is unavailable. In this example, the workflow attempts to use precipitation data from a weather station in Philomath, OR, but falls back to Corvallis data if Philomath data is not available.

- **Data Stores Workflow** (see [Create a Data Stores Workflow](#create-a-data-stores-workflow)): Demonstrates how to use multiple data stores within a single workflow, allowing you to store outputs in different locations. In this example, intermediate data is stored in the default S3 data store, while the final visualization is uploaded to a Backblaze B2 data store.

The conditional workflow structure is shown below:

```mermaid
flowchart LR
  01["Get Data"]
  02["Get Philomath Data"]
  03a["Process Philomath<br/>Precipitation"]
  03b["Process Precipitation<br/>(Fallback)"]
  04a["Process Temperature Min"]
  04b["Process Temperature Max"]
  05["Plot Data"]

  01 --> 02
  01 --> 04a
  01 --> 04b
  02 -->|True| 03a
  02 -->|False| 03b
  03a --> 05
  03b --> 05
  04a --> 05
  04b --> 05
```

Below is an example of the visualization we will be creating:

![Example weather data visualization](../assets/weather-visualization-workflow-example-600px.png)

## Prerequisites

This example function assumes you already completed the FaaSr tutorial ([https://faasr.io/FaaSr-Docs/tutorial/](https://faasr.io/FaaSr-Docs/tutorial/)) and have the necessary repositories and configuration set up. This tutorial will use the `FaaSr/FaaSr-Functions` repo as the function code source repository, but you may use your own repository as you follow along.

## Understanding our Data

For this tutorial, we are working with the [NOAA Global Historical Climatology Network Daily (GHCND) dataset](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily). Full documentation for the dataset can be found here: [https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/GHCND_documentation.pdf](https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/GHCND_documentation.pdf).

This large dataset is segmented into CSV data from weather stations across the world, each of which are accessible from the base URL [https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/](https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/). For example:

```plaintext
Name            Last modified     Size
------------------------------------------
USC00351857.csv 2025-10-19 13:01  1610136
USC00351862.csv 2025-10-19 13:01  13115701
USC00351877.csv 2025-10-19 13:01  6200961
USC00351897.csv 2025-10-19 13:01  7349609
```

Inspecting a CSV file, we see columns with:

1. Metadata such as the station ID and station name.
2. Daily weather observations like `PRCP` (precipitation), `TMIN` (minimum temperature), and `TMAX` (maximum temperature).
3. Metadata (measurement, quality, and source flags) about the daily observations, labeled as `..._ATTRIBUTES`.

> ℹ️ A more sophisticated data analysis would consider the additional observation metadata, however this is out of scope for this tutorial.

For this tutorial, we will be focusing on daily observations of precipitation and temperature to build our data visualization.

For this tutorial we will be using the Corvallis, OR (Oregon State University) station data (`USC00351862`). A complete list of weather stations and their IDs in this dataset can be found here: [https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt).

## Writing our Functions

### 1. Get our Data

The first function in our workflow is tasked with getting our GHCND data. The complete function can be found in [01_get_data.py](./python/01_get_data.py).

First, we will write our imports:

- `requests`: We will use requests to download CSV data from a URL.
- `faasr_log`: This will write log outputs to S3.
- `faasr_put_file`: We will use this function for storing our CSV data on S3.

```python
import requests
from FaaSr_py.client.py_client_stubs import faasr_log, faasr_put_file
```

Next we need a function to build the URL that we will use for downloading the CSV. This will take our Station ID as an argument and return the complete URL.

```python
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
```

We will write a single function for handling the CSV download using the `requests` library. This will receive the URL to download from and the name of the file to save the CSV data to. This will return an integer with the number of rows in the CSV.

> ℹ️ Note that we wrapped the download in a `try`/`except` block. This allows us to use `faasr_log` to record an error, simplifying troubleshooting if the download fails.

```python
def download_data(url: str, output_name: str) -> str:
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
```

Finally, we can put everything together in a single function that:

1. Builds the URL.
2. Downloads the CSV file to a local file.
3. Uploads the file to the S3 bucket using `faasr_put_file`.

This function will be called by FaaSr, so we will configure the `folder_name`, `output_name`, and `station_id` arguments when building our workflow.

```python
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
```

### 2. Process our Data

The second function in our workflow will handle processing the observations (precipitation, minimum temperature, and maximum temperature) that we are interested in. The complete function can be found in [02_process_data.py](./python/02_process_data.py).

First, we will define our imports, which now also include:

- `date`, `datetime`, and `timedelta` objects imported from the Python standard library for working with dates.
- `pandas`, a powerful library for working with tabular data.
- `faasr_get_file`, which we will use to download the CSV file created by our previous function.

```python
from datetime import date, datetime, timedelta

import pandas as pd
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log, faasr_put_file
```

Next we will define a function for getting our input data. This downloads the CSV file and returns it as a pandas DataFrame (See [What kind of data does pandas handle?](https://pandas.pydata.org/docs/getting_started/intro_tutorials/01_table_oriented.html) for more information). This function will receive the input `folder_name` and `input_name` as arguments.

```python
def get_input_data(folder_name: str, input_name: str) -> pd.DataFrame:
    """
    Get the input data from the FaaSr bucket and return it as a pandas DataFrame.

    Args:
        folder_name: The name of the folder to get the input data from.
        input_name: The name of the input file to get the data from.

    Returns:
        A pandas DataFrame containing the input data.
    """
    faasr_get_file(
        local_file=input_name,
        remote_folder=folder_name,
        remote_file=input_name,
    )
    return pd.read_csv(input_name)
```

We will be consistently working with date ranges while processing data, so we need a function that handles slicing a DataFrame by dates. This function will receive a DataFrame and start and end dates that we will use to return rows from the start date to the end data (inclusive).

> ℹ️ This function uses boolean indexing to locate all rows within a certain date range. For more details see [Indexing and selecting data](https://pandas.pydata.org/docs/user_guide/indexing.html).

```python
def slice_data_by_date(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Slice the data by date and return a new DataFrame.

    Args:
        df: A pandas DataFrame containing the data to slice.
        start: The start date to slice the data from.
        end: The end date to slice the data to.

    Returns:
        A pandas DataFrame containing the sliced data.
    """
    return df[(df["DATE"] >= start) & (df["DATE"] <= end)].copy()
```

Now we can start writing our logic for processing observation data. We will start with a function to process the current year's data. This will receive as arguments the DataFrame, name of the column we want to extract, and start and end dates. Since we are interested in the date only for comparison to previous years, we will create a `DAY` column that includes only the month and day of the `DATE` column.

> ℹ️ This function uses `apply` for efficient operations on DataFrames. For more information refer to the [pandas documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.apply.html).

```python
def process_current_year(
    df: pd.DataFrame,
    column_name: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Process the current year data and return a DataFrame with the day of the year and
    the column value.

    Args:
        df: A pandas DataFrame containing the data to process.
        column_name: The name of the column to process.
        start: The start date to process the data from.
        end: The end date to process the data to.

    Returns:
        A pandas DataFrame containing the processed data.
    """
    current_year = slice_data_by_date(df, start, end)

    # Get only the day of the year and the column value
    current_year["DAY"] = current_year["DATE"].apply(lambda x: x[5:])
    return current_year[["DAY", column_name]]
```

The next function requires some added complexity to calculate the average observation values from the previous 10 years. To manage this, we take advantage of the datetime library to move our start and end dates to previous years and use `timedelta` to add 30 days to the tail of the previous years' averages. The `concat` and `groupby` functions in the pandas library allow us to concatenate all data from each year then calculate the mean for each day, which we then return as the final result.

> ℹ️ For more information on the `groupby` function, see [Group by: split-apply-combine](https://pandas.pydata.org/docs/user_guide/groupby.html).

```python
def process_previous_years(
    df: pd.DataFrame,
    column_name: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Process the previous years data and return a DataFrame with the day of the year and
    the column value.

    Args:
        df: A pandas DataFrame containing the data to process.
        column_name: The name of the column to process.
        start: The start date to process the data from.
        end: The end date to process the data to.

    Returns:
        A pandas DataFrame containing the processed data.
    """

    # Get data for the same period + 30 days from the previous 10 years
    previous_years_data = []

    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    for year_offset in range(1, 11):
        # Get data for this year
        prev_start_date = date(
            year=start_date.year - year_offset,
            month=start_date.month,
            day=start_date.day,
        )

        prev_end_date = date(
            year=end_date.year - year_offset,
            month=end_date.month,
            day=end_date.day,
        ) + timedelta(days=30)  # Add 30 days to the end date

        year_data = slice_data_by_date(
            df,
            prev_start_date.strftime("%Y-%m-%d"),
            prev_end_date.strftime("%Y-%m-%d"),
        )

        # Convert date to MM-DD format for comparison
        year_data["DAY"] = year_data["DATE"].apply(lambda x: x[5:])
        previous_years_data.append(year_data[["DAY", column_name]])

    # Calculate the mean value for each day across previous years
    previous_years = pd.concat(previous_years_data, ignore_index=True)
    df = previous_years.groupby("DAY")[column_name].mean().reset_index()
    return df
```

As a final step, we will need to upload our processed data to S3. These two functions upload the current and previous years' averages, taking advantage of `to_csv` to save our DataFrames as CSV files before uploading.

> ℹ️ `to_csv` handles much of the complexity of converting our data to a CSV file. For more information refer to the [pandas documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html).

```python
def upload_current_year_data(
    folder_name: str,
    output_name: str,
    current_year: pd.DataFrame,
) -> None:
    """
    Save the output data to a local file and upload it to the S3 bucket.

    Args:
        folder_name: The name of the folder to save the output data to.
        output_name: The name of the output file to save the data to.
        current_year: A pandas DataFrame containing the current year data.
    """
    current_year.to_csv(f"current_year_{output_name}", index=False)

    faasr_put_file(
        local_file=f"current_year_{output_name}",
        remote_folder=folder_name,
        remote_file=f"current_year_{output_name}",
    )


def upload_previous_years_data(
    folder_name: str,
    output_name: str,
    previous_years: pd.DataFrame,
) -> None:
    """
    Save the output data to a local file and upload it to the S3 bucket.

    Args:
        folder_name: The name of the folder to save the output data to.
        output_name: The name of the output file to save the data to.
        previous_years: A pandas DataFrame containing the previous years data.
    """
    previous_years.to_csv(f"previous_years_{output_name}", index=False)

    faasr_put_file(
        local_file=f"previous_years_{output_name}",
        remote_folder=folder_name,
        remote_file=f"previous_years_{output_name}",
    )
```

Finally, we will write the function to be run by FaaSr that will:

1. Get the input data.
2. Process the current year's data.
3. Process the previous years' averages.
4. Upload the output data.

```python
def compare_to_yearly_average(
    folder_name: str,
    input_name: str,
    output_name: str,
    column_name: str,
    start: str,
    end: str,
):
    """
    Compare the values for this year to the average of the same period + 30 days from
    the previous 10 years.

    Args:
        folder_name: The name of the folder to get the input data from.
        input_name: The name of the input file to get the data from.
        output_name: The name of the output file to save the data to.
        column_name: The name of the column to process.
        start: The start date to process the data from.
        end: The end date to process the data to.
    """
    # 1. Get the input data
    df = get_input_data(folder_name, input_name)
    faasr_log(f"Loaded input data from {folder_name}/{input_name} with {len(df)} rows")

    # 2. Process the current year data
    current_year = process_current_year(df, column_name, start, end)
    faasr_log("Processed current year data")

    # 3. Process the previous years data
    previous_years = process_previous_years(df, column_name, start, end)
    faasr_log("Processed previous years data")

    # 4. Upload the output data
    upload_current_year_data(folder_name, output_name, current_year)
    faasr_log(f"Uploaded data to {folder_name}/current_year_{output_name}")

    upload_previous_years_data(folder_name, output_name, previous_years)
    faasr_log(f"Uploaded data to {folder_name}/previous_years_{output_name}")
```

### 3. Plot our Data

Our third and final function in this workflow will plot the data we processed in previous steps. The complete function can be found in [03_plot_data.py](./python/03_plot_data.py).

First, we will define our imports, adding `matplotlib` as our library for creating the visualization:

```python
import matplotlib.pyplot as plt
import pandas as pd
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log, faasr_put_file
from matplotlib.axes import Axes
```

Similar to our previous function, we will start with defining a function for downloading our input data:

```python
def get_input_data(
    folder_name: str,
    input_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get the input data from the FaaSr bucket and return it as a pandas DataFrame.

    Args:
        folder_name: The name of the folder to get the input data from.
        input_name: The name of the input file to get the data from.

    Returns:
        A tuple containing the current year data and the previous years data.
    """
    faasr_get_file(
        local_file=f"current_year_{input_name}",
        remote_folder=folder_name,
        remote_file=f"current_year_{input_name}",
    )

    faasr_get_file(
        local_file=f"previous_years_{input_name}",
        remote_folder=folder_name,
        remote_file=f"previous_years_{input_name}",
    )

    current_year_data = pd.read_csv(f"current_year_{input_name}")
    previous_years_data = pd.read_csv(f"previous_years_{input_name}")

    return current_year_data, previous_years_data
```

Before plotting our data, we need to perform a few operations to prepare it for visualization. First, we will use `merge` to combine our current and previous years' data. The dataset we are using in this example reports temperature in tenths of a degree Celsius, so we will transform our temperature observation columns to whole degrees Celsius. Finally, we also drop leap days.

> ℹ️ For more information on merging and joining DataFrames, see [Merge, join, concatenate, and compare](https://pandas.pydata.org/docs/user_guide/merging.html).

```python
def prepare_data(
    current_year_precip: pd.DataFrame,
    current_year_min_temp: pd.DataFrame,
    current_year_max_temp: pd.DataFrame,
    prev_years_precip: pd.DataFrame,
    prev_years_min_temp: pd.DataFrame,
    prev_years_max_temp: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare the data for plotting.

    Args:
        current_year_precip: The current year precipitation data.
        current_year_min_temp: The current year minimum temperature data.
        current_year_max_temp: The current year maximum temperature data.
        prev_years_precip: The previous years precipitation data.
        prev_years_min_temp: The previous years minimum temperature data.
        prev_years_max_temp: The previous years maximum temperature data.

    Returns:
        A tuple containing the current year data and the previous years data.
    """

    # Merge the dataframes on the DAY column
    current_year = current_year_precip.merge(current_year_min_temp, on="DAY")
    current_year = current_year.merge(current_year_max_temp, on="DAY")
    prev_years = prev_years_precip.merge(prev_years_min_temp, on="DAY")
    prev_years = prev_years.merge(prev_years_max_temp, on="DAY")

    # Convert temperature from tenths of degrees Celsius to degrees Celsius
    current_year["TMAX"] = current_year["TMAX"] / 10
    current_year["TMIN"] = current_year["TMIN"] / 10
    prev_years["TMAX"] = prev_years["TMAX"] / 10
    prev_years["TMIN"] = prev_years["TMIN"] / 10

    # Drop leap days
    current_year = current_year[current_year["DAY"] != "02-29"]
    prev_years = prev_years[prev_years["DAY"] != "02-29"]

    return current_year, prev_years
```

Since our visualization includes a unique subplot for each visualization, we will write a helper function that creates a subplot for given data. This will take the `Axes` of the subplot we are creating and the `x` and `y` data for current and previous years. We will use the `title` and `ylabel` arguments to distinguish the units and data we are plotting.

To distinguish current and previous years' data, we modify the `alpha` and `linestyle` properties of the previous years' data. As a final step we also modify the x-axis ticks to display every 7th day only.

> ℹ️ To see a robust catalog of operations you can perform on a subplot in matplotlib, see [Subplots, axes and figures](https://matplotlib.org/stable/gallery/subplots_axes_and_figures/index.html).

```python
def plot_subplot(
    ax: Axes,
    x_data: pd.Series,
    y_data: pd.Series,
    prev_years_x_data: pd.Series,
    prev_years_y_data: pd.Series,
    title: str,
    ylabel: str,
) -> None:
    """
    Plot a subplot with the given data. Current year data is plotted with 100% opacity
    and previous years data is plotted with 30% opacity.

    Args:
        ax: The axes to plot the data on.
        x_data: The x-axis data.
        y_data: The y-axis data.
        prev_years_x_data: The x-axis data for the previous years.
        prev_years_y_data: The y-axis data for the previous years.
        title: The title of the subplot.
        ylabel: The label for the y-axis.
    """
    # Create the plot for the current year
    ax.plot(
        x_data,
        y_data,
        alpha=1.0,
        label="This year",
        linewidth=2,
    )

    # Create the plot for the previous years
    ax.plot(
        prev_years_x_data,
        prev_years_y_data,
        alpha=0.3,
        label="Last 10 years",
        linewidth=2,
        linestyle="--",
    )

    # Set the title and y-axis label
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Set the x-axis ticks to every 7th day
    tick_positions = prev_years_x_data[::7]
    ax.set_xticks(tick_positions)
    ax.tick_params(axis="x", rotation=45)
```

Finally, we will write our FaaSr function that:

1. Gets the input data.
2. Prepares the data for plotting.
3. Creates the figure with three subplots.
4. Saves the plot to a file and uploads it to S3.

Here, we use `plt.subplots` to create a figure with three subplots. The axes of these subplots (`ax1`, `ax2`, and `ax3`) are what we pass to the previous function we wrote.

> ℹ️ For more information on working with subplots in matplotlib, see [Create multiple subplots using `plt.subplots`](https://matplotlib.org/stable/gallery/subplots_axes_and_figures/subplots_demo.html).

```python
def plot_weather_comparison(
    folder_name: str,
    input_precip_name: str,
    input_min_temp_name: str,
    input_max_temp_name: str,
    location: str,
    output_name: str,
):
    """
    Create a combined plot with three subplots: precipitation, min temp, and max temp.

    Args:
        folder_name: The name of the folder to get the input data from.
        input_precip_name: The name of the input file to get the precipitation data from.
        input_min_temp_name: The name of the input file to get the minimum temperature data from.
        input_max_temp_name: The name of the input file to get the maximum temperature data from.
        location: The location of the weather data.
        output_name: The name of the output file to save the plot to.
    """

    # 1. Get the input data
    current_year_precip, prev_years_precip = get_input_data(
        folder_name,
        input_precip_name,
    )

    faasr_log(f"Loaded precipitation data from {folder_name}/{input_precip_name}")

    current_year_min_temp, prev_years_min_temp = get_input_data(
        folder_name,
        input_min_temp_name,
    )

    faasr_log(
        f"Loaded minimum temperature data from {folder_name}/{input_min_temp_name}"
    )

    current_year_max_temp, prev_years_max_temp = get_input_data(
        folder_name,
        input_max_temp_name,
    )

    faasr_log(
        f"Loaded maximum temperature data from {folder_name}/{input_max_temp_name}"
    )

    # 2. Prepare the data for plotting
    current_year, prev_years = prepare_data(
        current_year_precip,
        current_year_min_temp,
        current_year_max_temp,
        prev_years_precip,
        prev_years_min_temp,
        prev_years_max_temp,
    )

    faasr_log("Prepared data for plotting")

    # 3. Create the figure with 3 subplots
    _, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))
    plt.suptitle(f"Current Year Weather Data with 10 Year Average for {location}")

    # Precipitation subplot
    plot_subplot(
        ax=ax1,
        x_data=current_year["DAY"],
        y_data=current_year["PRCP"],
        prev_years_x_data=prev_years["DAY"],
        prev_years_y_data=prev_years["PRCP"],
        title="Precipitation",
        ylabel="Precipitation (mm)",
    )

    faasr_log("Plotted precipitation subplot")

    # Maximum temperature subplot
    plot_subplot(
        ax=ax2,
        x_data=current_year["DAY"],
        y_data=current_year["TMAX"],
        prev_years_x_data=prev_years["DAY"],
        prev_years_y_data=prev_years["TMAX"],
        title="Maximum Temperature",
        ylabel="Temperature (°C)",
    )

    faasr_log("Plotted maximum temperature subplot")

    # Minimum temperature subplot
    plot_subplot(
        ax=ax3,
        x_data=current_year["DAY"],
        y_data=current_year["TMIN"],
        prev_years_x_data=prev_years["DAY"],
        prev_years_y_data=prev_years["TMIN"],
        title="Minimum Temperature",
        ylabel="Temperature (°C)",
    )

    faasr_log("Plotted minimum temperature subplot")

    # 4. Save the plot to a file and upload it to the S3 bucket
    plt.tight_layout()
    plt.savefig(output_name)
    plt.close()

    faasr_put_file(
        local_file=output_name,
        remote_folder=folder_name,
        remote_file=output_name,
    )

    faasr_log(f"Uploaded plot to {folder_name}/{output_name}")
```

## Building our Workflow

Now that we wrote our three functions, we are ready to start building our workflow using the FaaSr Workflow Builder: [https://faasr.io/FaaSr-workflow-builder/](https://faasr.io/FaaSr-workflow-builder/).

The final workflow file that we will create can be found in [WeatherVisualization.json](./WeatherVisualization.json). Before getting started, you can visualize this workflow by clicking **Upload** from the Workflow Builder and either uploading the file or importing from its GitHub URL: [https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualization.json](https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualization.json).

> ℹ️ As you make changes to your workflow, you can click the **vertical layout** or **horizontal layout** controls at the top of the right-hand layout view to re-arrange the layout with your changes.

### 1. Set Up our Compute Server

After opening the Workflow Builder, we will first add a compute server. Click **Edit Compute Servers**, then following the FaaSr tutorial (see [Prerequisites](#prerequisites)), enter your GitHub username for **UserName**, `Faasr-workflow` for **ActionRepoName**, and `main` for **Branch**.

> ℹ️ This workflow uses GitHub Actions, but it is possible to bring your own compute server, like AWS Lambda. See the documentation for more details: [https://faasr.io/FaaSr-Docs/advanced/](https://faasr.io/FaaSr-Docs/advanced/).

Your configuration should appear as below:

![Compute server screenshot](../assets/weather-visualization-workflow-compute-server-600px.png)

### 2. Set Up our Data Store

Click **Edit Data Stores**. Then, enter the endpoint, bucket, and region that was used in the tutorial (see [Prerequisites](#prerequisites)). For **Endpoint**, **Bucket**, and **Region** enter `https://play.min.io`, `faasr`, and `us-east-1`.

> ℹ️ This workflow uses MinIO, but it is possible to bring your own S3 data store, like AWS S3. See the documentation for more details [https://faasr.io/FaaSr-Docs/workflows/#data-stores](https://faasr.io/FaaSr-Docs/workflows/#data-stores).

Your data store configuration should appear as below:

![Data store screenshot](../assets/weather-visualization-workflow-data-store-600px.png)

### 3. Add our Functions

#### Get Data Function

Navigate back to **Edit Actions/Functions** and find the field labeled **Start typing to create a new action...**, then enter `GetData` and press Enter.

![Create a new action screenshot](../assets/weather-visualization-workflow-add-function-600px.png)

With the function created, we can begin configuring it. For **Function Name**, enter the name of the function we created in [1. Get our Data](#1-get-our-data): `get_ghcnd_data`. For **Language**, select **Python**, and for **Compute Server**, ensure it is set to the default **GH** that we created in [1. Set Up our Computer Server](#1-set-up-our-compute-server).

> ⚠️ Notice here that `get_ghcnd_data` is the _Function Name_ (the name of the actual Python function that FaaSr will run), while `GetData` is the _Action ID_ (the unique identifier that FaaSr will use for orchestrating the workflow).

Your configuration should appear as below:

![Function configuration screenshot](../assets/weather-visualization-workflow-configure-function-600px.png)

To add arguments to the function, click **Add New Arguments** under the **Arguments** header. In the popup window enter the following argument names and values:

- `folder_name`: WeatherVisualization
- `output_name`: WeatherData.csv
- `station_id`: USC00351862

> ℹ️ `USC00351862` is the ID of the Oregon State University station in Corvallis, OR. For information on finding a different Station ID, see [Understanding our Data](#understanding-our-data).

You should see the arguments entered as below:

![Function arguments](../assets/weather-visualization-workflow-function-arguments-600px.png)

Next, for **Function's Git Repo/Path**, enter the Git repository name and folder that contains the Python files we created in [Writing our Functions](#writing-our-functions). For example: `FaaSr/FaaSr-Functions/WeatherVisualization/python`. Leave **Function's Action Container** blank to use the default container.

> ℹ️ This is the Docker container that will run the FaaSr framework and invoke our functions. It is possible to use your own container here, but is only recommended for very advanced use cases and an in-depth knowledge of Docker.

#### Process Data Functions

Next we will create three functions for processing our data, one for each variable that we are interested in (precipitation, minimum temperature, and maximum temperature).

Create a new function called `ProcessPrecipitation`. For **Function Name** enter the name of the function we created in [2. Process our Data](#2-process-our-data): `compare_to_yearly_average`. Set **Language** and **Compute Server** to `Python` and `GH`.

Next, add our arguments:

- `folder_name`: WeatherVisualization
- `input_name`: WeatherData.csv
- `output_name`: PrecipitationData.csv
- `column_name`: PRCP
- `start`: 2025-01-01
- `end`: 2025-03-01

Now, set **Function's Git Repo/Path** to the repository and folder containing the functions that we wrote in [Writing our Functions](#writing-our-functions), for example `FaaSr/FaaSr-Functions/WeatherVisualization/python`. Leave **Function's Action Container** blank.

Because our function uses the pandas library, we must add it to the function. Under **Python Packages for the Function**, enter `pandas` in the **NewPackageName** field and click **Add Package**.

> ℹ️ FaaSr will always install the latest version of Python packages. It recommended to write your code with the latest available versions to avoid any conflicts with old package versions.

Your Python packages should appear as below:

![Python packages screenshot](../assets/weather-visualization-workflow-add-python-package-600px.png)

With our Process Precipitation function created, we can simplify the creation of the same function for minimum and maximum temperature using the Workflow Builder's duplicate action feature. Scroll to the top of the left-hand menu to find the **Duplicate Action** field, enter `ProcessTemperatureMin`, and click **Duplicate Action**:

![Duplicate action screenshot](../assets/weather-visualization-workflow-duplicate-action-600px.png)

For the duplicated action, the only configuration we must change are the `output_name` and `column_name` arguments. Everything else can remain exactly the same, since FaaSr will run the exact same function:

- `output_name`: TemperatureMinData.csv
- `column_name`: TMIN

Finally, duplicate the action once more, naming the next function `ProcessTemperatureMax` and change the arguments accordingly:

- `output_name`: TemperatureMaxData.csv
- `column_name`: TMAX

#### Plot Data Function

Finally we will create the function for plotting our data. Create a new function called `PlotData`. Use the same configuration as our previous functions with the following differences:

For **Function Name** enter the name of the function we created in [3. Plot our Data](#3-plot-our-data): `plot_weather_comparison`.

Enter the following arguments:

- `folder_name`: WeatherVisualization
- `input_precip_name`: PrecipitationData.csv
- `input_min_temp_name`: TemperatureMinData.csv
- `input_max_temp_name`: TemperatureMaxData.csv
- `location`: Corvallis, OR
  - You may change this location depending on which Station ID you decided to use in the Get Data function.
- `output_name`: WeatherComparison.png

For Python packages, enter both `pandas` and `matplotlib`.

### 4. Connect our Functions

Our workflow's functions are configured, so our next step is to define its invocation paths. Navigate to our Get Data function, either by clicking it in the right-hand layout view or selecting it from the dropdown at the top of the left-hand menu.

Scroll to **Next Actions to Invoke**, click **Add New InvokeNext**, and use the popup menu to connect the Get Data function with our Process Data functions. With the leftmost dropdown menu, select `ProcessPrecipitation`, leave the remaining options unchanged, and click **Add New InvokeNext**.

> ℹ️ This popup menu also allows us to define rank (parallel execution) and conditional invocation (depending on whether a function returns `true` or `false`). For an example of conditional invocations, see [Create a Conditional Workflow](#create-a-conditional-workflow). For more information, refer to the documentation: [https://faasr.io/FaaSr-Docs/conditional/](https://faasr.io/FaaSr-Docs/conditional/).

Repeat this for our other Process Data functions `ProcessTemperatureMin` and `ProcessTemperatureMax`.

> ℹ️ Here we are marking the same function `PlotData` as the next invocation of multiple functions. By doing this, `PlotData` will run only once after each invoking function completes.

Our next invocations should now appear as below:

![Next actions to invoke screenshot](../assets/weather-visualization-workflow-invoke-next-600px.png)

Now, repeat this process to connect each of our Process Data functions to our Plot Data function. From each, click **Add New InvokeNext**, and from the dropdown menu select `PlotData`.

### 5. Finalize our Workflow Configuration

Our final step is to finalize our workflow configuration. Click **Workflow Settings**, then for **Workflow Name** enter `WeatherVisualization` and for **Entry Point** select `GetData`. Leave the remaining configuration as default.

> ℹ️ **Entry Point** is the first function we want to invoke in or workflow. \
> ℹ️ Refer to the documentation for more details on the other configuration options in this menu: [https://faasr.io/FaaSr-Docs/workflows/#workflow-settings](https://faasr.io/FaaSr-Docs/workflows/#workflow-settings).

Your workflow settings should appear as below:

![Workflow settings](../assets/weather-visualization-workflow-settings-600px.png)

## Download and Invoke the Workflow

With our workflow complete, click the **vertical layout** control at the top of the right-hand layout view to see our changes. The complete workflow should appear as below:

![Workflow layout screenshot](../assets/weather-visualization-workflow-layout-600px.png)

### Download the Workflow

Click on **Download** and click the **Download WeatherVisualization.json** button in the popup menu.

> ℹ️ It is possible to also download a particular Workflow Builder layout, in case it is necessary to share the particular layout with others on your team. See the documentation for more information: [https://faasr.io/FaaSr-Docs/workflows/#working-with-layout-files](https://faasr.io/FaaSr-Docs/workflows/#working-with-layout-files).

### Register and Invoke the Workflow

Navigate to your **FaaSr-workflow** repository (see [Prerequisites](#prerequisites) for more information) and upload the workflow file, then follow the necessary steps to register and invoke your workflow:

1. Navigate to your repo's **Actions** tab and from the left-hand menu select the **(FAASR REGISTER)** workflow.
2. Click **Run workflow**, enter the name of the JSON file `WeatherVisualization.json`, and click **Run workflow**.
   - Wait for the FaaSr Register workflow to complete, and you should see the five functions appear in the left-hand menu, prefixed with the workflow name (e.g., **WeatherVisualization-GetData**).
3. Repeat steps 1 and 2 with the **(FAASR INVOKE)** workflow.
   - You can monitor the  workflow's process by clicking on each function in the left-hand menu to view their workflow runs.

### View the Output Data

After successful invocation, your S3 bucket should contain the following outputs:

> ℹ️ How you view these outputs depends on the S3 provider you used for this tutorial.

```plaintext
your-bucket/
├── FaaSrLog/
└── WeatherVisualization/
    ├── CurrentYearPrecipitationData.csv
    ├── CurrentYearTemperatureMaxData.csv
    ├── CurrentYearTemperatureMinData.csv
    ├── PreviousYearsPrecipitationData.csv
    ├── PreviousYearsTemperatureMaxData.csv
    ├── PreviousYearsTemperatureMinData.csv
    ├── WeatherComparison.png
    └── WeatherData.csv
```

**FaaSrLog** includes the workflow's log outputs, which can be useful for troubleshooting any issues. See the documentation for more details: [https://faasr.io/FaaSr-Docs/logs/](https://faasr.io/FaaSr-Docs/logs/). **WeatherVisualization** includes all our workflow outputs, including the intermediate CSV outputs generated by our Get Data and Process Data functions.

The final generated graph is **WeatherComparison.png**.

## Create a Conditional Workflow

This section demonstrates how to modify the workflow to use conditional invocations. In this real-life scenario, we need to plot weather data for Corvallis, but we are required to use precipitation data from a weather station in Philomath. However, if the data from Philomath is not available, we fall back to the original precipitation data from Corvallis.

The conditional variant of our workflow includes:

- [`get_ghcnd_data_conditional`](./python/01_get_data.py): A function that conditionally downloads data and returns `True` or `False` based on data availability

The final conditional workflow file can be found in [WeatherVisualizationConditional.json](./WeatherVisualizationConditional.json). You can visualize this workflow by clicking **Upload** from the Workflow Builder and either uploading the file or importing from its GitHub URL: [https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualizationConditional.json](https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualizationConditional.json).

### Updating our Function Code

#### `01_get_data.py`

In `get_ghcnd_data_conditional`, we will modify the function to check if data is available and return a boolean value that determines which path the workflow should take.

First, we need to import `faasr_return` from `FaaSr_py.client.py_client_stubs`:

```python
from FaaSr_py.client.py_client_stubs import (
    faasr_log,
    faasr_put_file,
    faasr_return,
)
```

The conditional function checks if the data file exists (in this example, we use a placeholder function `file_exists()`). If the file exists, it downloads the data and returns `True`. Otherwise, it returns `False`:

```python
def get_ghcnd_data_conditional(folder_name: str, output_name: str, station_id: str):
    """
    Conditionally download data from the NOAA Global Historical Climatology Network Daily (GHCND)
    dataset for a specific station and upload it to an S3 bucket.

    This function mimics a conditional step in a workflow, where if the file exists, the function
    returns True. Otherwise, if the file does not exist, the function returns False.

    Args:
        folder_name: The name of the folder to upload the data to.
        output_name: The name of the file to upload the data to.
        station_id: The ID of the station to download the data from.
    """

    if file_exists():
        faasr_log(f"File exists, downloading data from {station_id}")
        get_ghcnd_data(folder_name, output_name, station_id)
        faasr_return(True)
    else:
        faasr_log("File does not exist, returning False")
        faasr_return(False)
```

> ℹ️ In a real implementation, `file_exists()` would check if the data file exists in your S3 bucket or data store. This example uses a placeholder function for demonstration purposes.\
> ⚠️ When using `faasr_return`, every execution path must end with `faasr_return`. If your workflow has a conditional invocation and the function does not end with `faasr_return`, an error will be thrown.

The key difference from the original `get_ghcnd_data` function is the use of `faasr_return()` to return a boolean value. This return value is what FaaSr uses to determine which path in the conditional invocation to follow.

### Updating our Workflow

Now that we've updated our function code to support conditional invocations, we need to modify our workflow configuration in the FaaSr Workflow Builder. We'll start with an existing workflow (either the one we built earlier or by uploading [WeatherVisualization.json](./WeatherVisualization.json)) and make the following changes.

#### 1. Add Get Philomath Data Function

Create a new function called `GetPhilomathData`. For **Function Name**, enter `get_ghcnd_data_conditional`. Set **Language** and **Compute Server** to `Python` and `GH`.

Add the following arguments:

- `folder_name`: WeatherVisualization
- `output_name`: PhilomathData.csv
- `station_id`: US1ORBN0028

> ℹ️ `US1ORBN0028` is the station ID for Philomath, OR. You can find other station IDs in the GHCND stations list: [https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt).

Set **Function's Git Repo/Path** to the repository and folder containing the conditional function. For example: `FaaSr/FaaSr-Functions/WeatherVisualization/python` (or your own repository if you've created the conditional function there).

#### 2. Add Process Philomath Precipitation Function

Create a new function called `ProcessPhilomathPrecipitation`. This function will process precipitation data from Philomath when it's available.

For **Function Name**, enter `compare_to_yearly_average`. Set **Language** and **Compute Server** to `Python` and `GH`.

Add the following arguments:

- `folder_name`: WeatherVisualization
- `input_name`: PhilomathData.csv
- `output_name`: PrecipitationData.csv
- `column_name`: PRCP
- `start`: 2025-01-01
- `end`: 2025-03-01

Set **Function's Git Repo/Path** to the repository and folder containing the processing functions, for example `FaaSr/FaaSr-Functions/WeatherVisualization/python`.

Add `pandas` to the function's Python packages.

Finally, configure this function to invoke `PlotData` by scrolling to **Next Actions to Invoke**, clicking **Add New InvokeNext**, and selecting `PlotData` from the dropdown.

#### 3. Update Get Data Function

Navigate to the `GetData` function. We need to update its **Next Actions to Invoke** to include `GetPhilomathData` instead of `ProcessPrecipitation`.

Scroll to **Next Actions to Invoke**. You should see three invocations:

- `ProcessPrecipitation` (which we'll remove or keep as a fallback)
- `ProcessTemperatureMin`
- `ProcessTemperatureMax`

Remove the `ProcessPrecipitation` invocation (click the **Delete** button for that row), then click **Add New InvokeNext** and select `GetPhilomathData` from the dropdown. This ensures that `GetPhilomathData` runs after `GetData` downloaded our fallback data from Corvallis.

> ℹ️ The `ProcessPrecipitation` function will still be invoked conditionally by `GetPhilomathData` if the Philomath data is not available, so we don't need it as a direct invocation from `GetData`.

#### 4. Configure Conditional InvokeNext

Navigate to the `GetPhilomathData` function in the Workflow Builder. Scroll to **Next Actions to Invoke** and click **Add New InvokeNext**.

In the popup menu, select **ProcessPhilomathData** from the dropdown menu. Then click on the dropdown menu with **Unconditional** and change it to **True**. This tells FaaSr what to invoke in the True invocation path.

![True conditional invocation screenshot](../assets/weather-visualization-workflow-conditional-true-600px.png)

Repeat the same steps to add **ProcessData** as our fallback, and set the conditional invocation to **False**.

![False conditional invocation screenshot](../assets/weather-visualization-workflow-conditional-false-600px.png)

The UI will show two rows in the **Next Actions to Invoke** section:

- One row with a dropdown showing `ProcessPhilomathPrecipitation` and a boolean dropdown set to `True`
- Another row with a dropdown showing `ProcessPrecipitation` and a boolean dropdown set to `False`

The workflow layout should now look like the following. Notice that the green and red branches from `GetPhilomathData` represent the True and False invocation paths.

![Conditional workflow layout screenshot](../assets/weather-visualization-workflow-conditional-layout-600px.png)

#### 5. Update Workflow Settings

Click **Workflow Settings** and set the **Workflow Name** to `WeatherVisualizationConditional`. The **Entry Point** should remain `GetData`.

All other settings remain the same as the original workflow.

### How the Conditional Workflow Works

With these changes complete, your workflow is configured for conditional processing. When you invoke this workflow, FaaSr will:

1. Run `GetData` to download Corvallis weather data
2. Run `GetPhilomathData` in parallel with temperature processing functions
3. `GetPhilomathData` checks if Philomath data is available:
   - If `True`: Downloads Philomath data and invokes `ProcessPhilomathPrecipitation`
   - If `False`: Skips downloading and invokes `ProcessPrecipitation` (which uses Corvallis data)
4. The selected precipitation processing function runs and then invokes `PlotData`
5. `PlotData` creates the final visualization using either Philomath or Corvallis precipitation data, along with Corvallis temperature data

This conditional workflow allows you to automatically fall back to Corvallis precipitation data when Philomath data is unavailable, ensuring your workflow completes successfully regardless of data availability.

## Create a Data Stores Workflow

This section demonstrates how to modify the workflow to use multiple data stores. In this scenario, we want to upload the final visualization to a different data store than the default one. This is useful when you need to store outputs in a separate location, such as a different S3-compatible service like Backblaze B2, while keeping intermediate data in your primary data store.

The data stores variant of our workflow includes:

- [`plot_weather_comparison_data_stores`](./python/03_plot_data.py): A function that uploads the final visualization to a specific data store using the `server_name` parameter

The final data stores workflow file can be found in [WeatherVisualizationDataStores.json](./WeatherVisualizationDataStores.json). You can visualize this workflow by clicking **Upload** from the Workflow Builder and either uploading the file or importing from its GitHub URL: [https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualizationDataStores.json](https://github.com/FaaSr/FaaSr-Functions/blob/main/WeatherVisualization/WeatherVisualizationDataStores.json).

### Creating a Bucket on Backblaze

#### 1. Create an Account

To get started with Backblaze, navigate to [https://www.backblaze.com/](https://www.backblaze.com/) to create an account. No credit card is required for the first 10 GB of S3 storage.

#### 2. Create a Bucket

After creating an account and logging in, click on **Create Bucket**. In the popup menu, enter a unique name for the bucket. For this tutorial, you can leave the remaining settings as their defaults. Then, click **Create a Bucket** to create the bucket.

After creating your bucket, note the bucket's **Endpoint**. Depending on your account's region, it may appear as `s3.us-east-005.backblazeb2.com`.

#### 3. Create your Bucket's Access Credentials

Next, in the left-hand menu click **Application Keys**. Under **Your Application Keys**, click **Add a New Application Key**. In the popup, enter a descriptive name for your key. In the **Allow access to Buckets** dropdown, select the bucket you just created. This is optional, but recommended. For this tutorial, leave all other options as their default values. Finally, click **Create New Key**.

Next, note the **keyId** and **applicationKey** values that Backblaze generated. You will need these for the next step.

#### 4. Store your Bucket's Access Credentials

Next, navigate to your FaaSr-workflow GitHub repo to store your credentials. CLick on **Settings**, then in the left-hand menu click **Secrets and variables** and then **Actions**.

Click on **New repository secret** and enter the name as `BACKBLAZE_ACCESSKEY` and enter the **keyId** from the previous step as the value. Then, create a second secret named `BACKBLAZE_SECRETKEY` with the **applicationKey** value from the previous step.

> ℹ️ The prefix to any access key/secret key pair that you add to your Github repository must match the name of your custom datastore (case-insensitive). For example, a custom data store named `PersonalS3` would require secrets `PERSONALS3_ACCESSKEY` and `PERSONALS3_SECRETKEY`.

Your bucket and repository are now ready to use your custom Backblaze data store.

### Updating our Function Code for Data Stores

#### `03_plot_data.py`

In `plot_weather_comparison_data_stores`, we will modify the function to specify which data store to use when uploading the final visualization.

The key change is in the `faasr_put_file` call. Instead of using the default data store, we specify the `server_name` parameter to upload to a specific data store:

```python
faasr_put_file(
    local_file=output_name,
    remote_folder=folder_name,
    remote_file=output_name,
    server_name="Backblaze",
)
```

The `server_name` parameter must match the name of a data store defined in your workflow's **DataStores** configuration. In this example, we use `"Backblaze"` to upload to the Backblaze data store, while all other functions continue to use the default S3 data store.

> ℹ️ The `server_name` parameter is optional. If omitted, `faasr_put_file` will use the workflow's default data store. You can also use `server_name` with `faasr_get_file` to read from a specific data store.

The rest of the function remains identical to `plot_weather_comparison`. The only difference is the addition of the `server_name` parameter in the final `faasr_put_file` call.

### Updating our Workflow for Data Stores

Now that we've updated our function code to support multiple data stores, we need to modify our workflow configuration in the FaaSr Workflow Builder. We'll start with an existing workflow (either the one we built earlier or by uploading [WeatherVisualization.json](./WeatherVisualization.json)) and make the following changes.

#### 1. Add Additional Data Store

Click **Edit Data Stores** in the Workflow Builder. You should see your existing data store (e.g., `S3`). To add a second data store, click **Add New Data Store** or a similar button in the data stores configuration panel.

In the popup menu, enter `Backblaze` for the data store name and click **Create New Data Store**.

> ℹ️ You can add multiple data stores to your workflow. Each data store must have a unique name that matches the `server_name` parameter used in your function code.

Your data stores configuration should now show both `S3` and `Backblaze` data stores.

![New datastore screenshot](../assets/weather-visualization-workflow-new-datastore-600px.png)

Next, click on the **Backblaze** data store that you just created to edit its configuration, then enter your bucket's endpoint and name and the region of your account. Your config should look similar to the following:

![Custom datastore config screenshot](../assets/weather-visualization-workflow-datastore-config-600px.png)

#### 2. Update Plot Data Function

Navigate to the `PlotData` function in the Workflow Builder. We need to update the function to use the data stores variant.

Update the **Function Name** from `plot_weather_comparison` to `plot_weather_comparison_data_stores`. This tells FaaSr to use the function that includes the `server_name` parameter in its `faasr_put_file` call.

All other configuration (arguments, Python packages, etc.) remains the same as the original `PlotData` function.

#### 3. Update Workflow Settings

Click **Workflow Settings** and set the **Workflow Name** to `WeatherVisualizationDataStores`. The **Entry Point** should remain `GetData`.

The **Default Data Store** should remain set to `S3` (or your primary data store). This ensures that all functions without an explicit `server_name` parameter will continue to use the default data store.

All other settings remain the same as the original workflow.

### How the Data Stores Workflow Works

With these changes complete, your workflow is configured to use multiple data stores. When you invoke this workflow, FaaSr will:

1. All functions (`GetData`, `ProcessPrecipitation`, `ProcessTemperatureMin`, `ProcessTemperatureMax`) continue to read from and write to the default S3 data store
2. `PlotData` reads input data from the default S3 data store (since `faasr_get_file` doesn't specify a `server_name`)
3. `PlotData` creates the final visualization and uploads it to the Backblaze data store (because `faasr_put_file` specifies `server_name="Backblaze"`)

This data stores workflow allows you to:
- Keep intermediate processing data in your primary data store (S3)
- Store final outputs in a separate data store (Backblaze) for distribution, backup, or compliance reasons
- Use different data stores for different purposes within the same workflow

> ℹ️ You can use `server_name` with both `faasr_get_file` and `faasr_put_file` to read from or write to any data store defined in your workflow, giving you fine-grained control over where data is stored and retrieved.
