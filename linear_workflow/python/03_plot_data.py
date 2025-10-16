import matplotlib.pyplot as plt
import pandas as pd
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_put_file


def plot_weather_comparison(
    folder_name: str,
    input_precip_name: str,
    input_min_temp_name: str,
    input_max_temp_name: str,
    output_name: str,
):
    """
    Create a combined plot with three subplots: precipitation, min temp, and max temp.
    Each subplot has one unique color, with current year data at 100% opacity
    and last 10 years data at 50% opacity.
    """
    # Get precipitation data
    faasr_get_file(
        local_file="current_year_precipitation-data.csv",
        remote_folder=folder_name,
        remote_file=input_precip_name,
    )
    faasr_get_file(
        local_file="avg_prev_data_precipitation-data.csv",
        remote_folder=folder_name,
        remote_file=input_precip_name,
    )

    current_year_precip = pd.read_csv("current_year_precipitation-data.csv")
    avg_prev_data_precip = pd.read_csv("avg_prev_data_precipitation-data.csv")

    # Get min temperature data
    faasr_get_file(
        local_file="current_year_temperature-min-data.csv",
        remote_folder=folder_name,
        remote_file=input_min_temp_name,
    )
    faasr_get_file(
        local_file="avg_prev_data_temperature-min-data.csv",
        remote_folder=folder_name,
        remote_file=input_min_temp_name,
    )

    current_year_min_temp = pd.read_csv("current_year_temperature-min-data.csv")
    avg_prev_data_min_temp = pd.read_csv("avg_prev_data_temperature-min-data.csv")

    # Get max temperature data
    faasr_get_file(
        local_file="current_year_temperature-max-data.csv",
        remote_folder=folder_name,
        remote_file=input_max_temp_name,
    )
    faasr_get_file(
        local_file="avg_prev_data_temperature-max-data.csv",
        remote_folder=folder_name,
        remote_file=input_max_temp_name,
    )

    current_year_max_temp = pd.read_csv("current_year_temperature-max-data.csv")
    avg_prev_data_max_temp = pd.read_csv("avg_prev_data_temperature-max-data.csv")

    # Merge the dataframes on the DAY column
    current_year = current_year_precip.merge(current_year_min_temp, on="DAY")
    current_year = current_year.merge(current_year_max_temp, on="DAY")
    last_10_years = avg_prev_data_precip.merge(avg_prev_data_min_temp, on="DAY")
    last_10_years = last_10_years.merge(avg_prev_data_max_temp, on="DAY")

    # Create figure with 3 subplots
    _, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))

    current_year["TMAX"] = current_year["TMAX"] / 10
    current_year["TMIN"] = current_year["TMIN"] / 10
    last_10_years["TMAX"] = last_10_years["TMAX"] / 10
    last_10_years["TMIN"] = last_10_years["TMIN"] / 10

    # Define colors for each subplot
    colors = ["blue", "red", "green"]

    # Precipitation subplot
    ax1.plot(
        current_year["DAY"],
        current_year["PRCP"],
        color=colors[0],
        alpha=1.0,
        label="This year",
        linewidth=2,
    )
    ax1.plot(
        last_10_years["DAY"],
        last_10_years["PRCP"],
        color=colors[0],
        alpha=0.5,
        label="Last 10 years",
        linewidth=2,
        linestyle="--",
    )
    ax1.set_title("Precipitation")
    ax1.set_ylabel("Precipitation (mm)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Maximum temperature subplot
    ax2.plot(
        current_year["DAY"],
        current_year["TMAX"],
        color=colors[2],
        alpha=1.0,
        label="This year",
        linewidth=2,
    )
    ax2.plot(
        last_10_years["DAY"],
        last_10_years["TMAX"],
        color=colors[2],
        alpha=0.5,
        label="Last 10 years",
        linewidth=2,
        linestyle="--",
    )
    ax2.set_title("Maximum Temperature")
    ax2.set_ylabel("Temperature (°C)")
    ax2.set_xlabel("Date (MM-DD)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Minimum temperature subplot
    ax3.plot(
        current_year["DAY"],
        current_year["TMIN"],
        color=colors[1],
        alpha=1.0,
        label="This year",
        linewidth=2,
    )
    ax3.plot(
        last_10_years["DAY"],
        last_10_years["TMIN"],
        color=colors[1],
        alpha=0.5,
        label="Last 10 years",
        linewidth=2,
        linestyle="--",
    )
    ax3.set_title("Minimum Temperature")
    ax3.set_ylabel("Temperature (°C)")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # Set x-axis ticks for all subplots
    if len(last_10_years["DAY"]) > 0:
        tick_positions = last_10_years["DAY"][::7]
        for ax in [ax1, ax2, ax3]:
            ax.set_xticks(tick_positions)
            ax.tick_params(axis="x", rotation=45)

    # Adjust layout to prevent overlap
    plt.title("Current Year Weather Data with 10 Year Average")
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(output_name)
    plt.close()

    # Upload the plot to the S3 bucket
    faasr_put_file(
        local_file=output_name,
        remote_folder=folder_name,
        remote_file=output_name,
    )
