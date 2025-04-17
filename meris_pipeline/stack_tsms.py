import xarray as xr
import numpy as np
import re
from pathlib import Path
import datetime
import time
from pyresample import geometry, kd_tree
import rioxarray


def stack_regridded_tsms(processed_root, lat_bounds=(30.0, 50.0), lon_bounds=(-130.0, -110.0), res_deg=0.0027, clip_to=None):
    """
    Stack MERIS TSM NetCDFs from processed folders using pyresample interpolation.

    Args:
        processed_root (str or Path): Path to root folder with processed/<granule>/ folders
        lat_bounds (tuple): Min/max latitude
        lon_bounds (tuple): Min/max longitude
        res_deg (float): Resolution in degrees (default: 300m ~ 0.0027°)
        clip_to (str or Path, optional): Path to shapefile for optional clipping

    Returns:
        xarray.DataArray: TSM(time, lat, lon)
    """
    processed_root = Path(processed_root)
    granule_dirs = list((processed_root / "processed").iterdir())

    ref_lats = np.arange(lat_bounds[0], lat_bounds[1], res_deg)
    ref_lons = np.arange(lon_bounds[0], lon_bounds[1], res_deg)
    ref_lon_grid, ref_lat_grid = np.meshgrid(ref_lons, ref_lats)

    target_grid = geometry.AreaDefinition(
        area_id="regular_grid",
        description="Regular lat/lon grid",
        proj_id="latlon",
        projection={"proj": "latlong"},
        width=ref_lon_grid.shape[1],
        height=ref_lat_grid.shape[0],
        area_extent=(lon_bounds[0], lat_bounds[0], lon_bounds[1], lat_bounds[1])
    )

    stacked = []
    times = []

    for folder in granule_dirs:
        print(f"\n🧽 Processing granule: {folder.name}")
        start = time.time()

        tsm_path = folder / "tsm_nn.nc"
        geo_path = folder / "geo_coordinates.nc"
        if not tsm_path.exists() or not geo_path.exists():
            print(f"⚠️ Skipping {folder.name}: missing required .nc files")
            continue

        try:
            tsm_ds = xr.open_dataset(tsm_path)
            geo_ds = xr.open_dataset(geo_path)

            tsm = tsm_ds["TSM_NN"].values
            lat = geo_ds["latitude"].values
            lon = geo_ds["longitude"].values

            swath_def = geometry.SwathDefinition(lons=lon, lats=lat)
            regridded = kd_tree.resample_nearest(
                swath_def,
                tsm,
                target_grid,
                radius_of_influence=3000,
                fill_value=np.nan,
                reduce_data=True
            )

            match = re.search(r'(\d{8}T\d{6})', folder.name)
            timestamp = np.datetime64(datetime.datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")) if match else np.datetime64("NaT")

            da = xr.DataArray(
                regridded[np.newaxis, :, :],
                coords={"time": [timestamp], "lat": ref_lats, "lon": ref_lons},
                dims=["time", "lat", "lon"],
                name="TSM"
            )
            stacked.append(da)
            times.append(timestamp)

            # Save GeoTIFF
            geotiff_folder = processed_root / "geotiffs"
            geotiff_folder.mkdir(parents=True, exist_ok=True)
            output_tif_path = geotiff_folder / f"TSM_{timestamp}.tif"

            # Fix spatial dims for rioxarray
            da_for_raster = da.rename({"lat": "y", "lon": "x"})
            da_for_raster.rio.write_crs("EPSG:4326", inplace=True)
            da_for_raster = da_for_raster.rio.set_spatial_dims(x_dim="x", y_dim="y")

            # Save to GeoTIFF
            da_for_raster.squeeze("time").rio.to_raster(output_tif_path)

            print(f"💾 Saved GeoTIFF: {output_tif_path.name}")

            end = time.time()
            print(f"✅ Finished {folder.name} in {end - start:.2f} seconds")

        except Exception as e:
            print(f"❌ Error processing {folder.name}: {e}")
            continue

    if not stacked:
        raise ValueError("No valid TSM files found for stacking")

    stacked_da = xr.concat(stacked, dim="time").sortby("time")

    if clip_to:
        import geopandas as gpd
        import rioxarray
        stacked_da = stacked_da.rio.write_crs("EPSG:4326", inplace=False)
        gdf = gpd.read_file(clip_to)
        stacked_da = stacked_da.rio.clip(gdf.geometry.values, gdf.crs, drop=True)

    return stacked_da


def load_stacked_tsms_from_geotiffs(geotiff_folder):
    from pathlib import Path
    import xarray as xr
    import datetime
    import numpy as np

    geotiff_folder = Path(geotiff_folder)

    datasets = []

    for tif in sorted(geotiff_folder.glob("*.tif")):
        da = xr.open_dataarray(tif, engine="rasterio")

        # Expand dims manually
        da = da.expand_dims("time")

        # Extract timestamp from filename
        timestamp_str = tif.stem.split("_")[1]
        timestamp = np.datetime64(timestamp_str)

        da = da.assign_coords(time=("time", [timestamp]))
        datasets.append(da)

    if not datasets:
        raise ValueError(f"No GeoTIFFs found in {geotiff_folder}")

    stacked = xr.concat(datasets, dim="time").sortby("time")
    return stacked

def extract_time_from_filename(filename):
    """
    Extract timestamp from filename like 'TSM_20100401T185204.tif'.

    Args:
        filename (str): GeoTIFF filename

    Returns:
        np.datetime64: extracted timestamp
    """
    match = re.search(r'(\d{8}T\d{6})', filename)
    if match:
        return np.datetime64(datetime.datetime.strptime(match.group(1), "%Y%m%dT%H%M%S"))
    else:
        return np.datetime64("NaT")
