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

"""
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


tsm_path = Path("../../MERIS_downloads/processed/EN1_MDSI_MER_FRS_2P_20100401T190059_20100401T190326_042280_0142_20180216T183621_0100/tsm_nn.nc")
geo_path = Path("../../MERIS_downloads/processed/EN1_MDSI_MER_FRS_2P_20100401T190059_20100401T190326_042280_0142_20180216T183621_0100/geo_coordinates.nc")
output_path = Path("../../MERIS_downloads/test_output.tif")

tsm_nc_path = Path(tsm_path)
geo_nc_path = Path(geo_path)
output_path = Path(output_path)
#-------------#
# GeoTiff Test
#-------------#

create_geotiff_from_swath(tsm_nc_path, geo_nc_path, output_path)

"""

# Stacking Test
def extract_datetime_from_filename(filename):
    """Extract datetime from filename like TSM_20100401T185204.tif"""
    parts = filename.stem.split("_")
    if len(parts) >= 2:
        try:
            return datetime.strptime(parts[1], "%Y%m%dT%H%M%S")
        except ValueError:
            pass
    return None


def load_and_clip_geotiff(geotiff_path, shapefile_path=None):
    """Load single GeoTIFF with rioxarray and clip to shapefile if provided."""
    da = rioxarray.open_rasterio(geotiff_path, chunks="auto").squeeze()  # shape: (y, x)

    if shapefile_path:
        shp = gpd.read_file(shapefile_path)
        shp = shp.to_crs(da.rio.crs)  # ensure CRS match
        da = da.rio.clip(shp.geometry.values, shp.crs, drop=True)

    return da


# Shapefile
geotiff_dir="../MERIS_downloads/geotiffs",
output_nc_path="../MERIS_downloads/stacked_tsm.nc",
shapefile_path="../data/west_us_poly_ll/west_us_poly_ll.shp"

geotiff_dir = Path(geotiff_dir)
files = sorted(geotiff_dir.glob("TSM_*.tif"))

dataarrays = []
times = []

for f in files:
    time = extract_datetime_from_filename(f)
    if time is None:
        print(f"⚠️ Skipping invalid filename: {f.name}")
        continue

    da = rioxarray.open_rasterio(f, chunks="auto").squeeze()  # shape: (y, x)

    da = da.expand_dims(time=[time])
    dataarrays.append(da)
    times.append(time)

stacked = xr.concat(dataarrays, dim="time")
stacked.name = "TSM"

print(f"📦 Saving to {output_nc_path} ...")
stacked.to_netcdf(output_nc_path)
print("✅ Done!")

