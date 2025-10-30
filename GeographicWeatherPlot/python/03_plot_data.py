import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log, faasr_put_file
from matplotlib.axes import Axes
from scipy.interpolate import griddata


def load_input_data(folder_name: str, file_name: str) -> gpd.GeoDataFrame:
    faasr_get_file(
        local_file=file_name,
        remote_folder=folder_name,
        remote_file=file_name,
    )
    return gpd.read_file(file_name)


def create_grid(gdf: gpd.GeoDataFrame) -> tuple[np.ndarray, np.ndarray]:
    region_bounds = gdf.bounds
    minx, miny, maxx, maxy = region_bounds.iloc[0]
    grid_resolution = 100
    x_grid = np.linspace(minx, maxx, grid_resolution)
    y_grid = np.linspace(miny, maxy, grid_resolution)
    X_grid, Y_grid = np.meshgrid(x_grid, y_grid)

    return X_grid, Y_grid


def create_heatmap(
    ax: Axes,
    values: np.ndarray,
    points: np.ndarray,
    X_grid: np.ndarray,
    Y_grid: np.ndarray,
    title: str,
    cmap: str,
):
    interpolation = griddata(
        points,
        values,
        (X_grid, Y_grid),
        method="cubic",
        fill_value=np.nan,
    )

    # Plot minimum temperature heatmap
    im1 = ax.contourf(
        X_grid,
        Y_grid,
        interpolation,
        levels=20,
        cmap=cmap,
        alpha=0.8,
    )
    ax.scatter(
        points[:, 0],
        points[:, 1],
        c=values,
        s=50,
        cmap=cmap,
        edgecolors="black",
        linewidth=0.5,
    )
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.colorbar(im1, ax=ax, label="Temperature (°C)")


def plot_county_weekly_temperature(folder_name: str):
    # 1. Load input data
    outer_gdf = load_input_data(folder_name, "outer_boundary.geojson")
    temp_gdf = load_input_data(folder_name, "temp_gdf.geojson")
    state_gdf = load_input_data(folder_name, "state.geojson")
    county_gdf = load_input_data(folder_name, "county.geojson")

    # Create a grid for interpolation
    X_grid, Y_grid = create_grid(outer_gdf)

    # Extract coordinates and temperature values
    points = np.column_stack([temp_gdf.geometry.x, temp_gdf.geometry.y])

    # Create separate heatmaps for min and max temperatures
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # # Minimum temperature heatmap
    # min_temp_values = temp_gdf["TMIN"].values
    # min_temp_interp = griddata(
    #     points,
    #     min_temp_values,
    #     (X_grid, Y_grid),
    #     method="cubic",
    #     fill_value=np.nan,
    # )

    # # Plot minimum temperature heatmap
    # im1 = ax1.contourf(
    #     X_grid,
    #     Y_grid,
    #     min_temp_interp,
    #     levels=20,
    #     cmap="Blues_r",
    #     alpha=0.8,
    # )
    # ax1.scatter(
    #     temp_gdf.geometry.x,
    #     temp_gdf.geometry.y,
    #     c=temp_gdf["TMIN"],
    #     s=50,
    #     cmap="Blues_r",
    #     edgecolors="black",
    #     linewidth=0.5,
    # )
    # ax1.set_title("Minimum Temperature Heatmap (°C)")
    # ax1.set_xlabel("Longitude")
    # ax1.set_ylabel("Latitude")
    # plt.colorbar(im1, ax=ax1, label="Temperature (°C)")

    # # Maximum temperature heatmap
    # max_temp_values = temp_gdf["TMAX"].values
    # max_temp_interp = griddata(
    #     points,
    #     max_temp_values,
    #     (X_grid, Y_grid),
    #     method="cubic",
    #     fill_value=np.nan,
    # )

    # # Plot maximum temperature heatmap
    # im2 = ax2.contourf(
    #     X_grid,
    #     Y_grid,
    #     max_temp_interp,
    #     levels=20,
    #     cmap="Reds",
    #     alpha=0.8,
    # )
    # ax2.scatter(
    #     temp_gdf.geometry.x,
    #     temp_gdf.geometry.y,
    #     c=temp_gdf["TMAX"],
    #     s=50,
    #     cmap="Reds",
    #     edgecolors="black",
    #     linewidth=0.5,
    # )
    # ax2.set_title("Maximum Temperature Heatmap (°C)")
    # ax2.set_xlabel("Longitude")
    # ax2.set_ylabel("Latitude")
    # plt.colorbar(im2, ax=ax2, label="Temperature (°C)")
    create_heatmap(
        ax1,
        temp_gdf["TMIN"],
        points,
        X_grid,
        Y_grid,
        "Minimum Temperature Heatmap (°C)",
        "Blues_r",
    )
    create_heatmap(
        ax2,
        temp_gdf["TMAX"],
        points,
        X_grid,
        Y_grid,
        "Maximum Temperature Heatmap (°C)",
        "Reds",
    )

    # Add geographic boundaries to both subplots
    region_bounds = outer_gdf.bounds
    minx, miny, maxx, maxy = region_bounds.iloc[0]
    for ax in [ax1, ax2]:
        state_gdf.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)
        county_gdf.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)
        outer_gdf.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    # Set aspect ration to original resolution
    county_bbox = county_gdf.bounds
    county_min_x = county_bbox["minx"].iloc[0]
    county_min_y = county_bbox["miny"].iloc[0]
    county_max_x = county_bbox["maxx"].iloc[0]
    county_max_y = county_bbox["maxy"].iloc[0]
    original_aspect_ratio = (county_max_x - county_min_x) / (
        county_max_y - county_min_y
    )
    ax1.set_aspect(original_aspect_ratio)
    ax2.set_aspect(original_aspect_ratio)

    # Set ticks to every .5 degrees
    ax1.set_xticks(np.arange(minx + 0.5 - minx % 0.5, maxx, 0.5))
    ax1.set_yticks(np.arange(miny + 0.5 - miny % 0.5, maxy, 0.5))
    ax2.set_xticks(np.arange(minx + 0.5 - minx % 0.5, maxx, 0.5))
    ax2.set_yticks(np.arange(miny + 0.5 - miny % 0.5, maxy, 0.5))

    plt.tight_layout()
    plt.savefig("temperature_heatmap.png")

    faasr_put_file(
        local_file="temperature_heatmap.png",
        remote_folder=folder_name,
        remote_file="temperature_heatmap.png",
    )
    faasr_log(f"Uploaded temperature heatmap to {folder_name}/temperature_heatmap.png")
