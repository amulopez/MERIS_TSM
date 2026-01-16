from pathlib import Path
import xarray as xr
import rioxarray
import geopandas as gpd
import numpy as np
from datetime import datetime


def extract_datetime_from_filename(filepath):
    """
    Extract the start datetime from a filename like:
    TSM_EN1_MDSI_MER_FRS_2P_20100401T185204_20100401T190059_....tif
    """
    stem = filepath.stem
    try:
        for part in stem.split("_"):
            if len(part) == 15 and part.startswith("20"):
                return datetime.strptime(part, "%Y%m%dT%H%M%S")
    except ValueError:
        pass
    print(f"❌ Could not extract timestamp from {filepath.name}")
    return None


def clip_stack_to_shapefile(stacked, shapefile_path):
    """Clip stacked xarray to a shapefile using rioxarray."""
    shp = gpd.read_file(shapefile_path)
    shp = shp.to_crs(stacked.rio.crs)
    return stacked.rio.clip(shp.geometry.values, shp.crs, drop=True)


def build_target_grid(bbox, res_deg=0.0027):
    """Create a target grid dataset using bounding box and resolution."""
    lon_min, lat_min, lon_max, lat_max = bbox
    new_lon = np.arange(lon_min, lon_max, res_deg)
    new_lat = np.arange(lat_min, lat_max, res_deg)
    return xr.Dataset({
        "x": ("x", new_lon),
        "y": ("y", new_lat),
    })


def regrid_to_target_grid(da, target_grid):
    """Regrid a single DataArray to the provided target grid."""
    return da.interp_like(target_grid, method="nearest")


def stack_tsms_from_geotiffs(geotiff_dir, shapefile=None, bbox=None, res_deg=0.0027):
    geotiff_dir = Path(geotiff_dir)
    files = sorted(geotiff_dir.glob("TSM_*.tif"))
    if not files:
        raise FileNotFoundError("No matching TSM_*.tif files found.")

    if not bbox:
        raise ValueError("bbox must be provided for consistent regridding.")

    target_grid = build_target_grid(bbox, res_deg=res_deg)

    dataarrays = []
    for f in files:
        print(f"🗂️ Processing: {f.name}")
        timestamp = extract_datetime_from_filename(f)
        if timestamp is None:
            continue
        try:
            da = rioxarray.open_rasterio(f).squeeze()
            da = regrid_to_target_grid(da, target_grid)
            da = da.expand_dims(time=[timestamp])
            dataarrays.append(da)
        except Exception as e:
            print(f"⚠️ Skipping {f.name} due to error: {e}")

    stacked = xr.concat(dataarrays, dim="time")
    stacked.name = "TSM"

    if shapefile:
        stacked = clip_stack_to_shapefile(stacked, shapefile)

    return stacked
