from pathlib import Path
import zipfile
import shutil
import xarray as xr
import numpy as np
from scipy.interpolate import griddata
import re
import datetime


def postprocess_granule(zip_path, output_root):
    """
    Unzips a MERIS granule ZIP, filters NetCDFs, regrids TSM to a standard lat/lon grid,
    and adds time metadata from the filename.
    """
    try:
        zip_path = Path(zip_path)
        extract_dir = zip_path.with_suffix("")  # Strip .zip extension for folder name

        # Unzip to temporary folder
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_dir)

        # Find relevant NetCDFs
        tsm_path = next(extract_dir.glob("*TSM*.nc"), None)
        geo_path = next(extract_dir.glob("*geo_coordinates*.nc"), None)
        if not tsm_path or not geo_path:
            shutil.rmtree(extract_dir, ignore_errors=True)
            raise FileNotFoundError("Required TSM or geo_coordinates NetCDF missing")

        # Load data
        tsm_ds = xr.open_dataset(tsm_path)
        geo_ds = xr.open_dataset(geo_path)

        tsm_data = tsm_ds['TSM_NN'].values
        lat_data = geo_ds['latitude'].values
        lon_data = geo_ds['longitude'].values

        # Reference grid
        lat_min, lat_max = 30.0, 50.0
        lon_min, lon_max = -130.0, -110.0
        res_deg = 0.0027
        ref_lats = np.arange(lat_min, lat_max, res_deg)
        ref_lons = np.arange(lon_min, lon_max, res_deg)
        ref_lon_grid, ref_lat_grid = np.meshgrid(ref_lons, ref_lats)

        # Interpolate
        mask = ~np.isnan(tsm_data)
        points = np.column_stack((lon_data[mask], lat_data[mask]))
        values = tsm_data[mask]
        regridded = griddata(points, values, (ref_lon_grid, ref_lat_grid), method='linear')

        # Extract time from filename
        match = re.search(r'(\d{8}T\d{6})', zip_path.name)
        if match:
            timestamp = np.datetime64(datetime.datetime.strptime(match.group(1), "%Y%m%dT%H%M%S"))
        else:
            timestamp = np.datetime64('NaT')

        # Create DataArray
        regridded_da = xr.DataArray(
            regridded[np.newaxis, :, :],  # add time dimension
            coords={"time": [timestamp], "lat": ref_lats, "lon": ref_lons},
            dims=["time", "lat", "lon"],
            name="TSM"
        )

        # Save to NetCDF
        output_nc = Path(output_root) / "regridded" / f"{zip_path.stem}.nc"
        output_nc.parent.mkdir(parents=True, exist_ok=True)
        regridded_da.to_netcdf(output_nc)

        return str(output_nc)

    except Exception as e:
        print(f"Postprocessing failed for {zip_path}: {e}")
        return None
