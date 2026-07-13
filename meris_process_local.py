#!/usr/bin/env python3
"""
MERIS TSM WORKFLOW: FROM RAW DATA TO DAILY MOSAICS
==============================================================================

Standalone script version of the confirmed MERIS TSM notebook workflow.
Same logic as the notebook version — reorganized into functions with a
main() entry point and optional command-line overrides, so it can be run
directly with:

    python meris_tsm_workflow.py
    python meris_tsm_workflow.py --base-directory /path/to/data --masking-strategy cloud_only

Differences from the Sentinel-3 version this was adapted from:
  1. Raw archives are delivered as ".ZIP" (uppercase) instead of ".zip".
     The unzip step matches the extension case-insensitively so both
     ".zip" and ".ZIP" (and any mixed case) are picked up.
  2. MERIS quality flags are NOT a single WQSF bitmask like Sentinel-3.
     They are split across TWO netCDFs:
       - common_flags.nc  -> variables ES, CC, CO
       - wqsf.nc          -> variables WP_QS, WP_PC
     Step 3 builds the combined quality mask from these two files using
     explicit bit-extraction, rather than eumartools.flag_mask() (which
     assumes a single WQSF flag word and is no longer applicable here).

WORKFLOW OVERVIEW:
  Step 1: Unzip raw .ZIP/.zip files and delete originals
  Step 2: Clean up netCDF files (keep only needed variables)
  Step 3: Build MERIS quality mask (ES/CC/CO + WP_QS/WP_PC) and apply to TSM_NN
  Step 4: Convert masked netCDF swath data to georeferenced GeoTIFF rasters
  Step 5: Clip rasters to Region of Interest (ROI) using shapefile
  Step 6: Create daily mosaic rasters (merge multiple passes per day if they exist)

ASSUMPTIONS CARRIED OVER FROM THE S3 SCRIPT (please verify against your data):
  - TSM_NN is assumed to be stored as packed integer DNs with
    scale_factor/add_offset attributes encoding log10(g/m³), decoded as:
      (1) linear:  log10_val = DN * scale_factor + add_offset
      (2) exp:     physical  = 10 ^ log10_val  ->  g/m³
    Confirmed against S3IPF PDS 004_3, Table 7-6 (scale_factor=0.01811835,
    add_offset=-2). Read dynamically from each file's own attributes, with
    a runtime sanity check against these reference values (Step 3).
  - The SEN3-style folder suffix is unchanged. If your MERIS product
    folders instead end in ".SAFE" (or something else), pass
    --safe-folder-suffix .SAFE.
  - TSM_NN_FAIL (WP_PC bit 3) is included in the default custom flag list.
    Each user must modify the CUSTOM_FLAGS below as relevant to their analyses.
==============================================================================
"""

import os
import re
import glob
import zipfile
import argparse
from pathlib import Path
import numpy as np
import xarray as xr
from pyresample import geometry as geom
from pyresample import kd_tree as kdt
from osgeo import gdal, osr
from datetime import datetime
import warnings
import geopandas as gpd
import rioxarray
import rasterio
from rasterio.merge import merge
from rasterio.warp import reproject, Resampling

warnings.filterwarnings('ignore')

# ==============================================================================
# DEFAULT CONFIGURATION — overridable via command-line arguments (see main())
# ==============================================================================

DEFAULT_BASE_DIRECTORY = "/Users/lopezama/Documents/Blackwood/MERIS/meris_workflow_tests/local/data"
DEFAULT_ROI_SHAPE       = "/Users/lopezama/Documents/Blackwood/shapefiles/huc10_coastline_buffer/huc10_coastline_buffer.shp"
DEFAULT_MASKING_STRATEGY   = 'custom'   # options: 'recommended', 'cloud_only', 'custom'
DEFAULT_SAFE_FOLDER_SUFFIX = ".SEN3"    # change to ".SAFE" if your MERIS product folders use that suffix

# MERIS quality flag names available (see get_meris_flag_components() below):
#   LAND_MAP, LAND_RADIOMETRIC, CLOUD, CLOUD_AMBIGUOUS, INVALID, COSMETIC,
#   SUSPECT, HISOLZEN, SATURATED, HIGHGLINT, SEA_ICE, TSM_NN_FAIL
CUSTOM_FLAGS = [
    'LAND_MAP', 'LAND_RADIOMETRIC', 'CLOUD', 'CLOUD_AMBIGUOUS', 'INVALID',
    'COSMETIC', 'SUSPECT', 'HISOLZEN', 'SATURATED', 'HIGHGLINT', 'SEA_ICE',
    'TSM_NN_FAIL',   # <- remove this line if flagging OCNN/TSM_NN failures is not wanted
]

NODATA_VALUE = -9999.0   # numeric sentinel used throughout for GeoTIFF nodata

# Reference TSM_NN packing values from S3IPF PDS 004_3 ("Product Data Format
# Specification - OLCI Level 2 Marine"), Table 7-6. Used only as a sanity
# check in Step 3 — the actual decode always uses each file's own attrs.
REFERENCE_TSM_SCALE_FACTOR = 0.01811835
REFERENCE_TSM_ADD_OFFSET   = -2.0
REFERENCE_TOLERANCE        = 1e-4  # abs difference allowed before warning

FILES_TO_KEEP = [
    "cloud.nc", "common_flags.nc", "cqsf.nc", "geo_coordinates.nc",
    "iop_nn.nc", "par.nc", "tie_geo_coordinates.nc", "tie_geometries.nc",
    "time_coordinates.nc", "tsm_nn.nc", "trsp.nc", "wqsf.nc",
    "Oa01_reflectance.nc", "Oa02_reflectance.nc", "Oa03_reflectance.nc",
    "Oa04_reflectance.nc", "Oa05_reflectance.nc", "Oa06_reflectance.nc",
    "Oa07_reflectance.nc", "Oa08_reflectance.nc", "Oa09_reflectance.nc",
    "Oa010_reflectance.nc", "Oa011_reflectance.nc", "Oa012_reflectance.nc",
    "Oa016_reflectance.nc", "Oa017_reflectance.nc", "Oa018_reflectance.nc",
    "Oa019_reflectance.nc", "Oa021_reflectance.nc"
]


# ==============================================================================
# STEP 1: UNZIP RAW DATA FILES
# ==============================================================================

def unzip_and_delete(directory):
    """Unzips all .zip/.ZIP (any case) files in directory and deletes the originals."""
    zip_count = 0
    for filename in os.listdir(directory):
        if filename.lower().endswith(".zip"):
            file_path = os.path.join(directory, filename)
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                os.remove(file_path)
                zip_count += 1
                print(f"  Unzipped and deleted: {filename}")
            except zipfile.BadZipFile:
                print(f"  Skipping invalid zip file: {filename}")
            except Exception as e:
                print(f"  Error processing {filename}: {e}")

    print(f"\n{'='*60}")
    print(f"STEP 1 COMPLETE: Unzipped {zip_count} files")
    print(f"{'='*60}\n")


def run_step1(base_directory):
    print("\n" + "="*60)
    print("STEP 1: UNZIPPING RAW DATA FILES")
    print("="*60)
    unzip_and_delete(base_directory)


# ==============================================================================
# STEP 2: CLEAN UP NETCDF FILES
# ==============================================================================

def run_step2(base_directory, safe_folder_suffix):
    print("\n" + "="*60)
    print("STEP 2: CLEANING UP NETCDF FILES")
    print("="*60)

    deleted_count = 0
    kept_count    = 0

    for folder_name in os.listdir(base_directory):
        folder_path = os.path.join(base_directory, folder_name)
        if os.path.isdir(folder_path) and folder_name.endswith(safe_folder_suffix):
            print(f"\n📂 Processing folder: {folder_name}")
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                if os.path.isfile(file_path) and file not in FILES_TO_KEEP:
                    os.remove(file_path)
                    deleted_count += 1
                    print(f"  ✗ Deleted: {file}")
                elif os.path.isfile(file_path):
                    kept_count += 1
                    print(f"  ✓ Kept: {file}")

    print(f"\n{'='*60}")
    print(f"STEP 2 COMPLETE: Deleted {deleted_count} files, kept {kept_count} files")
    print(f"{'='*60}\n")


# ==============================================================================
# STEP 3: BUILD MERIS QUALITY MASK AND APPLY TO TSM DATA
# ==============================================================================
#
# MERIS quality flags live in two files:
#   common_flags.nc -> ES (bits: 0=LAND_MAP, 1=LAND_RADIOMETRIC)
#                       CC (bits: 0=CLOUD, 1=CLOUD_AMBIGUOUS)
#                       CO (bits: 0=INVALID, 1=COSMETIC, 4=SUSPECT,
#                                 6=HISOLZEN, 12-26=SATURATED)
#   wqsf.nc          -> WP_QS (bit: 0=SEA_ICE, 2=HIGHGLINT)
#                       WP_PC (bit: 3=TSM_NN_FAIL)
#
# TSM_NN encoding (confirmed against S3IPF PDS 004_3, Table 7-6):
#   - Stored as packed integer DNs with scale_factor/add_offset attrs
#   - Step A: log10_val  = DN * scale_factor + add_offset   -> log10(g/m³)
#   - Step B: physical   = 10 ^ log10_val                   -> g/m³
#   Negative log10 values are physically valid (e.g. -2.0 = 0.01 g/m³).
#   The intermediate masked netCDF is saved as float32 in physical g/m³
#   units directly (mirrors the S3 script's Step 3/4 split, collapsed here
#   since the decode itself doesn't depend on the flag source).
# ==============================================================================

def extract_bit(arr: np.ndarray, bit: int) -> np.ndarray:
    """Extracts a single bit from an integer flag array as a boolean mask."""
    return (arr.astype(np.uint64) & (np.uint64(1) << np.uint64(bit))) != 0


def extract_saturated(CO: np.ndarray) -> np.ndarray:
    """SATURATED is a multi-bit flag spanning CO bits 12-26 (any bit set = saturated)."""
    sat_mask = np.uint32(0)
    for bit in range(12, 27):
        sat_mask |= np.uint32(1 << bit)
    return (CO.astype(np.uint32) & sat_mask) != 0


def get_meris_flag_components(common_flags_path, wqsf_path):
    """
    Reads common_flags.nc (ES, CC, CO) and wqsf.nc (WP_QS, WP_PC) and returns
    a dict of {flag_name: boolean_mask_array}, one entry per quality flag.
    """
    cf_ds   = xr.open_dataset(common_flags_path)
    wqsf_ds = xr.open_dataset(wqsf_path)

    ES    = cf_ds["ES"].values.astype(np.uint32)
    CC    = cf_ds["CC"].values.astype(np.uint32)
    CO    = cf_ds["CO"].values.astype(np.uint32)
    WP_QS = wqsf_ds["WP_QS"].values.astype(np.uint64)
    WP_PC = wqsf_ds["WP_PC"].values.astype(np.uint32)

    cf_ds.close()
    wqsf_ds.close()

    flag_components = {
        'LAND_MAP':         extract_bit(ES, 0),
        'LAND_RADIOMETRIC': extract_bit(ES, 1),
        'CLOUD':            extract_bit(CC, 0),
        'CLOUD_AMBIGUOUS':  extract_bit(CC, 1),
        'INVALID':          extract_bit(CO, 0),
        'COSMETIC':         extract_bit(CO, 1),
        'SUSPECT':          extract_bit(CO, 4),
        'HISOLZEN':         extract_bit(CO, 6),
        'SATURATED':        extract_saturated(CO),
        'HIGHGLINT':        extract_bit(WP_QS, 2),
        'SEA_ICE':          extract_bit(WP_QS, 0),
        'TSM_NN_FAIL':      extract_bit(WP_PC, 3),
    }
    return flag_components


def get_flag_list(strategy):
    all_flags = ['LAND_MAP', 'LAND_RADIOMETRIC', 'CLOUD', 'CLOUD_AMBIGUOUS',
                 'INVALID', 'COSMETIC', 'SUSPECT', 'HISOLZEN', 'SATURATED',
                 'HIGHGLINT', 'SEA_ICE', 'TSM_NN_FAIL']
    strategies = {
        'recommended': all_flags,
        'cloud_only':  ['CLOUD', 'CLOUD_AMBIGUOUS'],
        'custom':      CUSTOM_FLAGS
    }
    return strategies.get(strategy, strategies['recommended'])


def build_quality_mask(flag_components, flag_list):
    """Combines the selected named flags into a single boolean mask via OR."""
    mask = None
    for name in flag_list:
        if name not in flag_components:
            print(f"   WARNING: unknown flag name '{name}' — skipping")
            continue
        mask = flag_components[name] if mask is None else (mask | flag_components[name])
    if mask is None:
        raise ValueError("No valid flags selected — quality mask would be empty.")
    return mask


def apply_tsm_mask(tsm_nc_path, common_flags_path, wqsf_path, output_path, flag_list):
    """
    Reads raw packed TSM_NN DNs, applies scale/offset to get log10(g/m³) then
    10^x to get physical g/m³, builds the combined MERIS quality mask from
    common_flags.nc + wqsf.nc, applies it, and saves a clean float32 netCDF
    in physical g/m³ units (no scale_factor/add_offset attrs).
    """
    try:
        # Open raw — no auto-decode so we control every step
        tsm_ds_raw = xr.open_dataset(tsm_nc_path, mask_and_scale=False)
        tsm_raw    = tsm_ds_raw["TSM_NN"]

        print(f"   Raw TSM_NN attributes:")
        for k, v in tsm_raw.attrs.items():
            print(f"     {k}: {v}")
        print(f"   Raw dtype: {tsm_raw.dtype}")
        print(f"   Raw DN range: {tsm_raw.values.min()} – {tsm_raw.values.max()}")

        # Extract packing parameters
        scale_factor = float(tsm_raw.attrs.get('scale_factor', 1.0))
        add_offset   = float(tsm_raw.attrs.get('add_offset',   0.0))

        # Sanity check against the documented reference values
        # (S3IPF PDS 004_3, Table 7-6: scale_factor=0.01811835, add_offset=-2)
        if abs(scale_factor - REFERENCE_TSM_SCALE_FACTOR) > REFERENCE_TOLERANCE:
            print(f"   WARNING: scale_factor {scale_factor} differs from "
                  f"documented reference {REFERENCE_TSM_SCALE_FACTOR} — "
                  f"double-check this file's packing.")
        if abs(add_offset - REFERENCE_TSM_ADD_OFFSET) > REFERENCE_TOLERANCE:
            print(f"   WARNING: add_offset {add_offset} differs from "
                  f"documented reference {REFERENCE_TSM_ADD_OFFSET} — "
                  f"double-check this file's packing.")

        fill_value   = tsm_raw.attrs.get('_FillValue', None)
        valid_min    = tsm_raw.attrs.get('valid_min',  None)
        valid_max    = tsm_raw.attrs.get('valid_max',  None)

        dn = tsm_raw.values.astype(np.float64)

        if fill_value is not None:
            dn = np.where(dn == float(fill_value), np.nan, dn)
        if valid_min is not None:
            dn = np.where(dn < float(valid_min), np.nan, dn)
        if valid_max is not None:
            dn = np.where(dn > float(valid_max), np.nan, dn)

        # Step A: linear decode -> log10(g/m³)
        tsm_log10 = dn * scale_factor + add_offset
        # Step B: exponentiate -> physical g/m³
        tsm_physical = np.power(10.0, tsm_log10)

        valid_log  = tsm_log10[np.isfinite(tsm_log10)]
        valid_phys = tsm_physical[np.isfinite(tsm_physical)]
        if valid_phys.size > 0:
            print(f"   log10 TSM range:    {valid_log.min():.4f} – {valid_log.max():.4f} lg(g/m³)")
            print(f"   Physical TSM range: {valid_phys.min():.4f} – {valid_phys.max():.4f} g/m³")
        else:
            print(f"   WARNING: No finite physical values after decode")

        # Build the MERIS quality mask from the two flag files
        flag_components = get_meris_flag_components(common_flags_path, wqsf_path)
        quality_mask     = build_quality_mask(flag_components, flag_list)

        if quality_mask.shape != tsm_physical.shape:
            raise ValueError(
                f"Flag mask shape {quality_mask.shape} does not match "
                f"TSM_NN shape {tsm_physical.shape} — check that common_flags.nc, "
                f"wqsf.nc, and tsm_nn.nc are on the same grid."
            )

        tsm_physical[quality_mask] = np.nan

        # Statistics
        valid_before  = int(np.sum(np.isfinite(tsm_physical) | quality_mask))
        valid_after   = int(np.sum(np.isfinite(tsm_physical)))
        masked_pixels = valid_before - valid_after

        stats = {
            'total_pixels':   tsm_physical.size,
            'valid_before':   valid_before,
            'valid_after':    valid_after,
            'masked_pixels':  masked_pixels,
            'masked_percent': (masked_pixels / valid_before * 100) if valid_before > 0 else 0
        }

        # Per-flag pixel counts (helpful diagnostic)
        total_px = quality_mask.size
        print(f"   Flag pixel counts (n_total = {total_px:,}):")
        for name in flag_list:
            if name in flag_components:
                n = int(np.sum(flag_components[name]))
                print(f"     {name:<18}: {n:>8,}  ({n / total_px * 100:.1f} %)")

        # Build clean output dataset in physical g/m³, no packing attributes
        tsm_ds_raw.close()
        tsm_ds_template = xr.open_dataset(tsm_nc_path, mask_and_scale=False)

        masked_da = xr.DataArray(
            tsm_physical.astype(np.float32),
            dims=tsm_ds_template["TSM_NN"].dims,
            coords=tsm_ds_template["TSM_NN"].coords,
            attrs={
                'units':                 'g m-3',
                'long_name':             'Total Suspended Matter — linear g/m³ (decoded from log10 storage)',
                'quality_flags_applied': ', '.join(flag_list),
                'masking_date':          datetime.now().isoformat(),
                'scale_applied':         f'log10_val = DN * {scale_factor} + {add_offset}; physical = 10^log10_val',
            }
        )

        masked_ds = xr.Dataset({'TSM_NN': masked_da}, attrs=tsm_ds_template.attrs)

        encoding = {'TSM_NN': {'dtype': 'float32', '_FillValue': NODATA_VALUE}}
        masked_ds.to_netcdf(output_path, encoding=encoding)

        masked_ds.close()
        tsm_ds_template.close()

        return stats

    except Exception as e:
        import traceback
        print(f"  ✗ Error applying mask: {e}")
        traceback.print_exc()
        return None


def run_step3(base_dir, safe_folder_suffix, masking_strategy):
    masked_dir = base_dir / "tsm_masked"
    masked_dir.mkdir(exist_ok=True)

    print("\n" + "="*60)
    print("STEP 3: APPLYING MERIS QUALITY FLAG MASKS TO TSM DATA")
    print("="*60)

    flag_list = get_flag_list(masking_strategy)
    print(f"Masking strategy: {masking_strategy}")
    print(f"Flags applied:    {', '.join(flag_list)}")
    print(f"Output directory: {masked_dir}\n")

    total_processed  = 0
    total_masked_pix = 0
    total_valid_bef  = 0
    total_valid_aft  = 0

    for subfolder in base_dir.iterdir():
        if subfolder.is_dir() and subfolder.name.endswith(safe_folder_suffix):
            tsm_path           = subfolder / "tsm_nn.nc"
            common_flags_path  = subfolder / "common_flags.nc"
            wqsf_path          = subfolder / "wqsf.nc"

            if tsm_path.exists() and common_flags_path.exists() and wqsf_path.exists():
                output_path = masked_dir / f"{subfolder.name}_tsm_masked.nc"
                print(f" Processing: {subfolder.name}")
                stats = apply_tsm_mask(tsm_path, common_flags_path, wqsf_path, output_path, flag_list)

                if stats:
                    total_processed  += 1
                    total_masked_pix += stats['masked_pixels']
                    total_valid_bef  += stats['valid_before']
                    total_valid_aft  += stats['valid_after']
                    print(f"   Valid pixels: {stats['valid_before']:,} → {stats['valid_after']:,}")
                    print(f"   Masked: {stats['masked_pixels']:,} px ({stats['masked_percent']:.1f}%)")
            else:
                missing = []
                if not tsm_path.exists():          missing.append("tsm_nn.nc")
                if not common_flags_path.exists(): missing.append("common_flags.nc")
                if not wqsf_path.exists():          missing.append("wqsf.nc")
                print(f" Skipping {subfolder.name}: missing {', '.join(missing)}")

    overall_pct = (total_masked_pix / total_valid_bef * 100) if total_valid_bef > 0 else 0
    print(f"\n{'='*60}")
    print(f"STEP 3 COMPLETE: Processed {total_processed} files")
    print(f"Total valid before: {total_valid_bef:,} | after: {total_valid_aft:,}")
    print(f"Total masked: {total_masked_pix:,} ({overall_pct:.1f}%)")
    print(f"{'='*60}\n")

    return masked_dir, flag_list


# ==============================================================================
# STEP 4: CREATE GEOTIFFS FROM MASKED SWATH DATA
# ==============================================================================
#
# Reads the clean float32 g/m³ netCDF from Step 3.
# Opens with mask_and_scale=False since the file is already in physical units
# with no packing attributes — values are ready to write directly to GeoTIFF.
# ==============================================================================

def create_geotiff_from_masked_swath(masked_tsm_path, geo_nc_path, output_path,
                                     res_deg=0.0027, nodata=NODATA_VALUE):
    """
    Resamples masked TSM swath (g/m³) onto a regular lat/lon grid and
    writes a float32 GeoTIFF (EPSG:4326).
    """
    tsm_ds = xr.open_dataset(masked_tsm_path, mask_and_scale=False)
    geo_ds = xr.open_dataset(geo_nc_path,     mask_and_scale=True)

    tsm = tsm_ds["TSM_NN"].values.squeeze().astype(np.float32)
    lat = geo_ds["latitude"].values
    lon = geo_ds["longitude"].values

    tsm = np.where(tsm == nodata, np.nan, tsm)

    valid_in = tsm[np.isfinite(tsm)]
    if valid_in.size == 0:
        print(f"   No valid TSM pixels — skipping")
        tsm_ds.close()
        geo_ds.close()
        return False
    print(f"   Input TSM range:     {valid_in.min():.4f} – {valid_in.max():.4f} g/m³")

    swath_def = geom.SwathDefinition(lons=lon, lats=lat)
    lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)
    lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)

    ref_lats = np.arange(lat_min, lat_max, res_deg)
    ref_lons = np.arange(lon_min, lon_max, res_deg)
    cols, rows = len(ref_lons), len(ref_lats)

    area_def = geom.AreaDefinition(
        "area_id", "MERIS Grid", "latlon",
        {'proj': 'longlat', 'datum': 'WGS84'},
        cols, rows,
        (lon_min, lat_min, lon_max, lat_max)
    )

    index, outdex, index_array, dist_array = kdt.get_neighbour_info(
        swath_def, area_def, radius_of_influence=5000, neighbours=1
    )
    grid = kdt.get_sample_from_neighbour_info(
        'nn', area_def.shape, tsm, index, outdex, index_array, fill_value=np.nan
    ).astype(np.float32)

    valid_out = grid[np.isfinite(grid)]
    if valid_out.size > 0:
        print(f"   Resampled TSM range: {valid_out.min():.4f} – {valid_out.max():.4f} g/m³")

    grid_out = np.where(np.isnan(grid), nodata, grid).astype(np.float32)

    driver  = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(str(output_path), cols, rows, 1, gdal.GDT_Float32)

    pixel_size_x = (lon_max - lon_min) / cols
    pixel_size_y = (lat_max - lat_min) / rows
    dataset.SetGeoTransform([lon_min, pixel_size_x, 0, lat_max, 0, -pixel_size_y])

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset.SetProjection(srs.ExportToWkt())

    band = dataset.GetRasterBand(1)
    band.WriteArray(grid_out)
    band.SetNoDataValue(nodata)
    band.SetMetadataItem('UNITS', 'g m-3')

    if 'quality_flags_applied' in tsm_ds["TSM_NN"].attrs:
        band.SetMetadataItem('QUALITY_FLAGS', tsm_ds["TSM_NN"].attrs['quality_flags_applied'])
    if 'scale_applied' in tsm_ds["TSM_NN"].attrs:
        band.SetMetadataItem('SCALE_APPLIED', tsm_ds["TSM_NN"].attrs['scale_applied'])

    band.FlushCache()
    dataset = None
    tsm_ds.close()
    geo_ds.close()

    print(f"   Saved GeoTIFF: {output_path.name}")
    return True


def run_step4(base_dir, masked_dir):
    output_dir = base_dir / "geotiff"
    output_dir.mkdir(exist_ok=True)

    print("\n" + "="*60)
    print("STEP 4: CREATING GEOTIFFS FROM MASKED NETCDF FILES")
    print("="*60)
    print(f"Input directory:  {masked_dir}")
    print(f"Output directory: {output_dir}\n")

    processed_count = 0
    skipped_count   = 0

    for masked_file in masked_dir.glob("*.nc"):
        original_folder_name = masked_file.name.replace("_tsm_masked.nc", "")
        original_folder      = base_dir / original_folder_name
        geo_path             = original_folder / "geo_coordinates.nc"

        if geo_path.exists():
            output_path = output_dir / f"TSM_{original_folder_name}.tif"
            print(f"📂 Processing: {original_folder_name}")
            if create_geotiff_from_masked_swath(masked_file, geo_path, output_path):
                processed_count += 1
            else:
                skipped_count += 1
        else:
            print(f"⏩ Skipping: {original_folder_name} (missing geo_coordinates.nc)")
            skipped_count += 1

    print(f"\n{'='*60}")
    print(f"STEP 4 COMPLETE: Created {processed_count} GeoTIFFs, skipped {skipped_count}")
    print(f"{'='*60}\n")

    return output_dir


# ==============================================================================
# STEP 5: CLIP TO REGION OF INTEREST
# ==============================================================================

def clip_geotiff_with_shapefile(geotiff_path, shapefile_path, output_path):
    """Clips a GeoTIFF to a shapefile boundary using rioxarray."""
    try:
        roi    = gpd.read_file(shapefile_path)
        raster = rioxarray.open_rasterio(geotiff_path, masked=True)

        if raster.rio.crs != roi.crs:
            roi = roi.to_crs(raster.rio.crs)

        clipped = raster.rio.clip(roi.geometry.values, roi.crs, drop=True, invert=False)
        clipped.rio.to_raster(output_path, compress='lzw')
        print(f"  ✓ Clipped: {output_path.name}")
        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def run_step5(base_dir, output_dir, roi_shape):
    clipped_dir = base_dir / "geotiff_clipped"
    clipped_dir.mkdir(exist_ok=True)

    print("\n" + "="*60)
    print("STEP 5: CLIPPING TO REGION OF INTEREST")
    print("="*60)

    if not os.path.exists(roi_shape):
        print(f" ERROR: Shapefile not found at {roi_shape} — skipping clipping step")
        return clipped_dir

    print(f"Shapefile:        {roi_shape}")
    print(f"Input directory:  {output_dir}")
    print(f"Output directory: {clipped_dir}\n")

    total_clips      = 0
    successful_clips = 0

    for geotiff_file in output_dir.glob("*.tif"):
        total_clips += 1
        clipped_path = clipped_dir / geotiff_file.name
        print(f"[{total_clips}] {geotiff_file.name}")

        if clipped_path.exists():
            print(f"  ⊙ Already exists — skipping")
            successful_clips += 1
            continue

        if clip_geotiff_with_shapefile(geotiff_file, roi_shape, clipped_path):
            successful_clips += 1

    print(f"\n{'='*60}")
    print(f"STEP 5 COMPLETE: Clipped {successful_clips}/{total_clips} files")
    print(f"{'='*60}\n")

    return clipped_dir


# ==============================================================================
# STEP 6: CREATE DAILY MOSAIC RASTERS
# ==============================================================================

def merge_and_average(files, nodata=NODATA_VALUE):
    """
    Merges multiple same-day rasters and averages overlapping pixels.
    Nodata sentinel is excluded from averaging via NaN promotion.
    """
    srcs = [rasterio.open(f) for f in files]
    merged_array, merged_transform = merge(srcs, method='first')

    stack = []
    for src in srcs:
        data = src.read(1).astype(np.float32)
        data = np.where(data == nodata, np.nan, data)

        reprojected = np.full(merged_array[0].shape, np.nan, dtype=np.float32)
        reproject(
            source=data,
            destination=reprojected,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=merged_transform,
            dst_crs=src.crs,
            resampling=Resampling.nearest,
            src_nodata=np.nan,
            dst_nodata=np.nan
        )
        stack.append(reprojected)

    stack    = np.stack(stack, axis=0)
    averaged = np.nanmean(stack, axis=0)

    averaged_out = np.where(np.isnan(averaged), nodata, averaged).astype(np.float32)

    meta = srcs[0].meta.copy()
    for src in srcs:
        src.close()

    return averaged_out, merged_transform, meta


def run_step6(clipped_dir, flag_list, masking_strategy):
    input_folder  = str(clipped_dir)
    output_folder = os.path.join(input_folder, "daily_mosaics")
    os.makedirs(output_folder, exist_ok=True)

    print("\n" + "="*60)
    print("STEP 6: CREATING DAILY MOSAIC RASTERS")
    print("="*60)
    print(f"Input directory:  {input_folder}")
    print(f"Output directory: {output_folder}\n")

    all_files = glob.glob(os.path.join(input_folder, "*.tif"))

    date_pattern  = re.compile(r"(\d{8})")
    files_by_date = {}
    for f in all_files:
        match = date_pattern.search(os.path.basename(f))
        if match:
            files_by_date.setdefault(match.group(1), []).append(f)

    print(f"Found {len(all_files)} files covering {len(files_by_date)} unique dates\n")

    mosaic_count = 0
    for date, files in sorted(files_by_date.items()):
        print(f" Processing {date} ({len(files)} file(s))...")

        merged_array, merged_transform, meta = merge_and_average(files)

        meta.update({
            "height":    merged_array.shape[0],
            "width":     merged_array.shape[1],
            "transform": merged_transform,
            "dtype":     'float32',
            "count":     1,
            "nodata":    NODATA_VALUE
        })

        out_path = os.path.join(output_folder, f"TSM_daily_{date}.tif")
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(merged_array, 1)

        mosaic_count += 1
        print(f"   Saved: TSM_daily_{date}.tif")

    print(f"\n{'='*60}")
    print(f"STEP 6 COMPLETE: Created {mosaic_count} daily mosaics")
    print(f"{'='*60}\n")

    print("\n" + "="*60)
    print("WORKFLOW COMPLETE!")
    print("="*60)
    print(f"Masking strategy: {masking_strategy}")
    print(f"Flags applied:    {', '.join(flag_list)}")
    print(f"Final output:     {mosaic_count} daily mosaic GeoTIFFs")
    print(f"Location:         {output_folder}")
    print("="*60)


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="MERIS TSM workflow: raw SAFE/SEN3 data -> daily mosaic GeoTIFFs."
    )
    parser.add_argument("--base-directory", default=DEFAULT_BASE_DIRECTORY,
                         help="Directory containing raw MERIS .zip/.ZIP archives and/or extracted product folders.")
    parser.add_argument("--roi-shape", default=DEFAULT_ROI_SHAPE,
                         help="Path to the ROI shapefile used for clipping.")
    parser.add_argument("--masking-strategy", default=DEFAULT_MASKING_STRATEGY,
                         choices=["recommended", "cloud_only", "custom"],
                         help="Which quality flag set to apply.")
    parser.add_argument("--safe-folder-suffix", default=DEFAULT_SAFE_FOLDER_SUFFIX,
                         help="Suffix identifying MERIS product folders (e.g. .SEN3 or .SAFE).")
    parser.add_argument("--skip-unzip", action="store_true",
                         help="Skip Step 1 (unzip) — use if data is already extracted.")
    return parser.parse_args()


def main():
    args = parse_args()
    base_dir = Path(args.base_directory)

    if not args.skip_unzip:
        run_step1(args.base_directory)
    else:
        print("\nSTEP 1 SKIPPED (--skip-unzip)\n")

    run_step2(args.base_directory, args.safe_folder_suffix)
    masked_dir, flag_list = run_step3(base_dir, args.safe_folder_suffix, args.masking_strategy)
    output_dir = run_step4(base_dir, masked_dir)
    clipped_dir = run_step5(base_dir, output_dir, args.roi_shape)
    run_step6(clipped_dir, flag_list, args.masking_strategy)


if __name__ == "__main__":
    main()
