from scipy.interpolate import griddata
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime
import rioxarray as rio
import geopandas as gpd
from shapely.geometry import mapping

# Functions
def create_interpolated_tsm(tsm_data, lat_data, lon_data, ref_lats, ref_lons, method="linear"):
    """
    Interpolates TSM values onto a regular reference lat/lon grid.

    Parameters:
    - tsm_data, lat_data, lon_data: raw input arrays (2D)
    - ref_lats, ref_lons: 1D reference arrays defining grid
    - method: interpolation method ('linear', 'nearest', 'cubic')

    Returns:
    - xr.DataArray with dims ('lat', 'lon')
    """
    # Flatten and mask NaNs
    mask = ~np.isnan(tsm_data)
    points = np.column_stack((lon_data[mask], lat_data[mask]))
    values = tsm_data[mask]

    # Target grid
    lon_grid, lat_grid = np.meshgrid(ref_lons, ref_lats)

    # Interpolate onto grid
    interpolated = griddata(points, values, (lon_grid, lat_grid), method=method)

    return xr.DataArray(
        interpolated,
        coords={"lat": ref_lats, "lon": ref_lons},
        dims=["lat", "lon"]
    )

# Prep path and storage
base_path = Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/envisat/test_data")
all_dataarrays = []

# Load Geom
roi = gpd.read_file("/Users/lopezama/Documents/Blackwood/MERIS/ROI/west_us_poly_ll/west_us_poly_ll.shp")
roi = roi.to_crs("EPSG:4326")
roi_geom = [mapping(roi.geometry.unary_union)]

# Build reference grid once
lat_min, lat_max = 30, 50
lon_min, lon_max = -130, -110
res_deg = 0.0027
ref_lats = np.arange(lat_min, lat_max, res_deg)
ref_lons = np.arange(lon_min, lon_max, res_deg)

# Loop through all time directories
for time_folder in base_path.iterdir():
    if not time_folder.is_dir():
        continue

    try:
        # Parse timestamp from folder name
        time = datetime.strptime(time_folder.name[:15], "%Y%m%dT%H%M%S")

        # Load NetCDFs
        tsm_ds = xr.open_dataset(time_folder / "tsm_nn.nc")
        geo_ds = xr.open_dataset(time_folder / "geo_coordinates.nc")

        tsm_data = tsm_ds['TSM_NN'].values
        lat_data = geo_ds['latitude'].values
        lon_data = geo_ds['longitude'].values

        # Create gridded TSM
        tsm_gridded = create_interpolated_tsm(tsm_data, lat_data, lon_data, ref_lats, ref_lons)

        # Add time dimension
        tsm_gridded = tsm_gridded.expand_dims(time=[time])
        all_dataarrays.append(tsm_gridded)

    except Exception as e:
        print(f"Skipping {time_folder.name}: {e}")

# Stack all data frames
tsm_stack = xr.concat(all_dataarrays, dim="time")
tsm_stack.name = "TSM"

# Create dataset with proper CRS
tsm_ds = xr.Dataset({"TSM": tsm_stack})
tsm_ds.rio.write_crs("EPSG:4326", inplace=True)

# Clip Shapefile
# Ensure spatial dims and CRS are set
tsm = tsm_ds["TSM"].rio.write_crs("EPSG:4326")
tsm = tsm.rio.set_spatial_dims(x_dim="lon", y_dim="lat")

# Clip using the ROI
tsm_clipped = tsm.rio.clip(roi_geom, crs="EPSG:4326", drop=True)

# Save it
tsm_ds.to_netcdf("TSM_stack.nc")
tsm_clipped.to_netcdf("TSM_stack_clipped.nc")
