from pathlib import Path
import numpy as np
import xarray as xr
from pyresample import geometry as geom
from pyresample import kd_tree as kdt
from osgeo import gdal, gdal_array, osr

def create_geotiff_from_swath(tsm_nc_path, geo_nc_path, output_path, res_deg=0.0027):
    """
    Converts a MERIS swath to a gridded GeoTIFF using nearest-neighbor resampling.
    """
    tsm_nc_path = Path(tsm_nc_path)
    geo_nc_path = Path(geo_nc_path)
    output_path = Path(output_path)

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
        area_id="area_id",
        description="MERIS Grid",
        proj_id="latlon",
        projection={
            'proj': 'longlat',
            'datum': 'WGS84'
        },
        width=cols,
        height=rows,
        area_extent=area_extent
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
    dataset = driver.Create(
        str(output_path),
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
