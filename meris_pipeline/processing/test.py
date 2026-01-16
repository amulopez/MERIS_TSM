
"""
import numpy as np
from pyresample import geometry as geom
from pyresample import kd_tree as kdt
from osgeo import gdal, gdal_array, osr
import warnings
from pathlib import Path
import xarray as xr
import geopandas as gpd
import rioxarray
from datetime import datetime
warnings.filterwarnings("ignore")
from pathlib import Path
from datetime import datetime
import xarray as xr
import rioxarray
import matplotlib.pyplot as plt
import cartopy.crs as ccrs



#-------------#
# Load Files
#-------------#
tsm_path = Path("../../MERIS_downloads/processed/EN1_MDSI_MER_FRS_2P_20100401T190059_20100401T190326_042280_0142_20180216T183621_0100/tsm_nn.nc")
geo_path = Path("../../MERIS_downloads/processed/EN1_MDSI_MER_FRS_2P_20100401T190059_20100401T190326_042280_0142_20180216T183621_0100/geo_coordinates.nc")
output_path = Path("../../MERIS_downloads/test_output.tif")

tsm_nc_path = Path(tsm_path)
geo_nc_path = Path(geo_path)
output_path = Path(output_path)

#-------------#
# GeoTiff Test
#-------------#
def create_geotiff_from_swath(tsm_nc_path, geo_nc_path, output_path, res_deg=0.0027):

    # Load datasets
    tsm_ds = xr.open_dataset(tsm_nc_path)
    geo_ds = xr.open_dataset(geo_nc_path)

    tsm = tsm_ds["TSM_NN"].values.squeeze()
    lat = geo_ds["latitude"].values
    lon = geo_ds["longitude"].values

    # Mask invalid values
    mask = np.isfinite(tsm) & np.isfinite(lat) & np.isfinite(lon)
    if np.sum(mask) == 0:
        print("❌ No valid TSM pixels found.")
        return None

    # Define Swath and Area
    swath_def = geom.SwathDefinition(lons=lon, lats=lat)
    lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)
    lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)

    ref_lats = np.arange(lat_min, lat_max, res_deg)
    ref_lons = np.arange(lon_min, lon_max, res_deg)
    cols, rows = len(ref_lons), len(ref_lats)

    area_extent = (lon_min, lat_min, lon_max, lat_max)
    area_def = geom.AreaDefinition(
        "area_id", "MERIS Grid", "latlon",
        {
            'proj': 'longlat',
            'datum': 'WGS84'
        },
        cols, rows, area_extent
    )

    index, outdex, index_array, dist_array = kdt.get_neighbour_info(
        swath_def, area_def, radius_of_influence=5000, neighbours=1
    )

    # Resample with nearest-neighbor
    grid = kdt.get_sample_from_neighbour_info(
        'nn', area_def.shape, tsm, index, outdex, index_array, fill_value=np.nan
    )

    # Save using GDAL
    driver = gdal.GetDriverByName("GTiff")
    output_path = str(output_path)
    dataset = driver.Create(
        output_path,
        cols,
        rows,
        1,
        gdal_array.NumericTypeCodeToGDALTypeCode(grid.dtype)
    )

    pixel_size_x = (lon_max - lon_min) / cols
    pixel_size_y = (lat_max - lat_min) / rows
    transform = [lon_min, pixel_size_x, 0, lat_max, 0, -pixel_size_y]
    dataset.SetGeoTransform(transform)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset.SetProjection(srs.ExportToWkt())

    band = dataset.GetRasterBand(1)
    band.WriteArray(grid)
    band.SetNoDataValue(np.nan)
    band.FlushCache()

    dataset = None
    print(f"💾 Saved properly geolocated GeoTIFF: {output_path}")

create_geotiff_from_swath(tsm_nc_path, geo_nc_path, output_path)

#-------------#
# Stack Test
#-------------#

def extract_datetime_from_filename(filepath):

    #Extract the start datetime from a filename like:
    #TSM_EN1_MDSI_MER_FRS_2P_20100401T185204_20100401T190059_....tif
    
    stem = filepath.stem
    try:
        # Split by underscores and find the first string that looks like a timestamp
        for part in stem.split("_"):
            if len(part) == 15 and part.startswith("20"):
                return datetime.strptime(part, "%Y%m%dT%H%M%S")
    except ValueError:
        pass
    print(f"❌ Could not extract timestamp from {filepath.name}")
    return None

def clip_stack_to_shapefile(stacked, shapefile_path):
    #Clip stacked xarray to a shapefile using rioxarray

    shp = gpd.read_file(shapefile_path)
    shp = shp.to_crs(stacked.rio.crs)  # Ensure CRS match
    return stacked.rio.clip(shp.geometry.values, shp.crs, drop=True)


geotiff_dir = Path("../MERIS_downloads/geotiffs")
output_nc_path = Path("../MERIS_downloads/stacked_tsm_bare.nc")

# Find files
files = sorted(geotiff_dir.glob("TSM_*.tif"))
if not files:
    raise FileNotFoundError("No matching TSM_*.tif files found.")

dataarrays = []
for f in files:
    print(f"🗂️ Processing: {f.name}")
    timestamp = extract_datetime_from_filename(f)
    if timestamp is None:
        continue

    try:
        da = rioxarray.open_rasterio(f).squeeze()  # (y, x)
        da = da.expand_dims(time=[timestamp])      # add time dimension
        dataarrays.append(da)
    except Exception as e:
        print(f"⚠️ Skipping {f.name} due to error: {e}")

# Stack along time axis
stacked = xr.concat(dataarrays, dim="time")
stacked.name = "TSM"

# Clip Stacked to ROI
roi_shape = '../data/west_us_poly_ll/west_us_poly_ll.shp'
stack_clip = clip_stack_to_shapefile(stacked, roi_shape)

# Save out
stack_clip.to_netcdf(output_nc_path)

# Stack Daily
stacked.coords["date"] = ("time", stacked["time"].dt.floor("D"))
daily_mean = stacked.groupby("date").mean(dim="time", skipna=True)
daily_mean.name = "TSM_daily"
daily_mean.to_netcdf("TSM_daily.nc")


#-------------#
# Visualize Test
#-------------#

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def plot_tsm_slice(stacked, time_index=0, title=None):
    #Plot a single time slice from stacked TSM data.
    slice_da = stacked.isel(date=time_index)

    fig, ax = plt.subplots(figsize=(10, 6), subplot_kw={'projection': ccrs.PlateCarree()})
    pc = ax.pcolormesh(
        slice_da.x, slice_da.y, slice_da,
        cmap="viridis", shading="auto", transform=ccrs.PlateCarree()
    )
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.set_title(title or str(slice_da.time.values))
    fig.colorbar(pc, ax=ax, label='TSM')
    plt.show()
"""

#---------#
# Check Stack
#---------#
import os
os.environ["MPLBACKEND"] = "TkAgg"  # Weird stuff kelly needs to do for her computer and Pycharm
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import xarray as xr
xpath = "..//MERIS_downloads/stacked_tsm.nc"
stacked = xr.open_dataset(xpath)

fig, ax = plt.subplots(1,2, figsize=(10, 8), subplot_kw={'projection': ccrs.PlateCarree()})
ax = ax.flatten()

# Plot data
stacked.TSM.isel(time=0).plot(ax=ax[0], transform=ccrs.PlateCarree(), cmap='Spectral_r', cbar_kwargs={'label': 'TSM'})
stacked.TSM.isel(time=1).plot(ax=ax[1], transform=ccrs.PlateCarree(), cmap='Spectral_r', cbar_kwargs={'label': 'TSM'})

# Add coastlines and features
for axt in ax:
    axt.coastlines(resolution='10m', color='black')
    axt.add_feature(cfeature.BORDERS, linestyle=':')
    axt.add_feature(cfeature.LAND, facecolor='lightgray')

plt.tight_layout()
fig.figure.savefig("tsm_2days_gridded.png", dpi=300)
print("Saved preview as tsm_day1.png")




