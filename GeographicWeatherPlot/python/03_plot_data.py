import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log, faasr_put_file
from matplotlib.axes import Axes
from scipy.interpolate import griddata


def load_input_data(folder_name: str, file_name: str) -> gpd.GeoDataFrame:
    """
    Load the input data from the FaaSr bucket and return it as a geopandas GeoDataFrame.

    Args:
        folder_name: The name of the folder to get the input data from.
        file_name: The name of the input file to get the data from.

    Returns:
        A geopandas GeoDataFrame containing the input data.
    """
    faasr_get_file(
        local_file=file_name,
        remote_folder=folder_name,
        remote_file=file_name,
    )
    return gpd.read_file(file_name)


def get_bounds(gdf: gpd.GeoDataFrame) -> tuple[float, float, float, float]:
    """
    Get the outer bounds of a geopandas GeoDataFrame.

    Args:
        gdf: The geopandas GeoDataFrame to get the bounds of.

    Returns:
        A tuple containing the minimum and maximum x and y coordinates.
    """
    region_bounds = gdf.bounds
    minx, miny, maxx, maxy = region_bounds.iloc[0]
    return minx, miny, maxx, maxy


def create_grid(gdf: gpd.GeoDataFrame) -> tuple[np.ndarray, np.ndarray]:
    """
    Create a grid for the heatmap interpolation.

    Args:
        gdf: The geopandas GeoDataFrame to create the grid for.

    Returns:
        A tuple containing the x and y grids.
    """
    minx, miny, maxx, maxy = get_bounds(gdf)
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
) -> None:
    """
    Create a heatmap for the given values.

    Args:
        ax: The axes to plot the heatmap on.
        values: The values to plot the heatmap for.
        points: The points to plot the heatmap for.
        X_grid: The x grid to plot the heatmap on.
        Y_grid: The y grid to plot the heatmap on.
        title: The title of the heatmap.
        cmap: The colormap to use for the heatmap.
    """
    # Interpolate the values on the grid
    interpolation = griddata(
        points,
        values,
        (X_grid, Y_grid),
        method="cubic",
        fill_value=np.nan,
    )

    # Plot the heatmap
    im1 = ax.contourf(
        X_grid,
        Y_grid,
        interpolation,
        levels=20,
        cmap=cmap,
        alpha=0.8,
    )

    # Plot the stations as scatter points
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


def add_boundaries(ax: Axes, gdf: gpd.GeoDataFrame) -> None:
    """
    Add geographic boundaries to a plot.

    Args:
        ax: The axes to plot the boundaries on.
        gdf: The GeoDataFrame to plot the boundaries of.
    """
    gdf.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)


def set_limits(ax: Axes, gdf: gpd.GeoDataFrame) -> None:
    """
    Set the limits of a plot.

    Args:
        ax: The axes to set the limits of.
        gdf: The GeoDataFrame to use for the outer bounds.
    """
    minx, miny, maxx, maxy = get_bounds(gdf)
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)


def set_aspect_ratio(ax: Axes, gdf: gpd.GeoDataFrame) -> None:
    """
    Set the aspect ratio of a plot.

    Args:
        ax: The axes to set the aspect ratio of.
        gdf: The GeoDataFrame to use for the aspect ratio.
    """
    minx, miny, maxx, maxy = get_bounds(gdf)
    original_aspect_ratio = (maxx - minx) / (maxy - miny)
    ax.set_aspect(original_aspect_ratio)


def set_ticks(ax: Axes, gdf: gpd.GeoDataFrame) -> None:
    """
    Set the ticks of a plot.

    Args:
        ax: The axes to set the ticks of.
        gdf: The GeoDataFrame to use for the outer bounds.
    """
    minx, miny, maxx, maxy = get_bounds(gdf)
    ax.set_xticks(np.arange(minx + 0.5 - minx % 0.5, maxx, 0.5))
    ax.set_yticks(np.arange(miny + 0.5 - miny % 0.5, maxy, 0.5))


def plot_county_weekly_temperature(folder_name: str, county_name: str):
    """
    Plot the weekly temperature for a given county, save the plot to a file, and upload
    it to the S3 bucket.

    Args:
        folder_name: The name of the folder to get the input data from.
    """
    # 1. Load input data
    outer_gdf = load_input_data(folder_name, "outer_boundary.geojson")
    temp_gdf = load_input_data(folder_name, "temp_gdf.geojson")
    state_gdf = load_input_data(folder_name, "state.geojson")
    county_gdf = load_input_data(folder_name, "county.geojson")

    # 2. Prepare the grid and points for heatmap interpolation
    X_grid, Y_grid = create_grid(outer_gdf)
    points = np.column_stack([temp_gdf.geometry.x, temp_gdf.geometry.y])

    # 3. Plot the heatmaps
    _, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
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

    # 4. Add geographic boundaries to both subplots
    add_boundaries(ax1, state_gdf)
    add_boundaries(ax1, county_gdf)
    add_boundaries(ax2, state_gdf)
    add_boundaries(ax2, county_gdf)

    # 5. Set each plot's limits and aspect ratio
    set_limits(ax1, outer_gdf)
    set_limits(ax2, outer_gdf)
    set_aspect_ratio(ax1, county_gdf)
    set_aspect_ratio(ax2, county_gdf)

    # 6. Set ticks to every .5 degrees
    set_ticks(ax1, outer_gdf)
    set_ticks(ax2, outer_gdf)

    # 7. Save the plot to a file and upload it to the S3 bucket
    plt.tight_layout()
    plt.title(f"Temperature Heatmap for {county_name} County")
    plt.savefig("temperature_heatmap.png")

    faasr_put_file(
        local_file="temperature_heatmap.png",
        remote_folder=folder_name,
        remote_file="temperature_heatmap.png",
    )
    faasr_log(f"Uploaded temperature heatmap to {folder_name}/temperature_heatmap.png")
