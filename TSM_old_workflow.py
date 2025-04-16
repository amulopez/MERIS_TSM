#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### WHAT THIS CODE DOES ###

# 1) Unzips MERIS data .zip file, extracts .SEN3 folder with netCDFs, and deletes the .zip folder.
# 2) Renames the .SEN3 folder to be the date-time of the scene acquisition (extracts this info from characters in the original folder name)
# 3) Deletes unwanted .nc files and keeps specified .nc files. Prints out a list of what was deleted/kept. 
#    Resets the new working directory to be the folder containing the .nc files.  
# 4) Add lat/lon as spatial dimensions to TSM netcdf from geo_coordinates netcdf
# 5) Convert georeferenced TSM netcdf to GeoTIFF - yields a geotiff with incorrect transformation and bounding box
# 6) Fix the bounding box and transformation of the TSM geotiff
# 7) Plot the corrected TSM geotiff
# 8) Clips TSM geotiff to western US coast ROI using shapefile
# 9) Plot clipped TSM geotiff

###  EDIT BEFORE RUNNING  ###
# Edit the base_directory in steps 1-2 and parent_directory in step 3 (should all be the same directory)
#       This is the directory where the .zip folder is downloaded, extracted, deleted, and renamed.
#       The final renamed directory has the .nc files that are modified for analysis 
# In (8) edit path to ROI shapefile used for clipping

# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov

# Load packages
import os
import zipfile
import shutil
import glob
import netCDF4 as nc
from netCDF4 import Dataset
import rasterio
from rasterio.transform import from_origin
import numpy as np
from osgeo import gdal, osr
import xarray as xr
import dask
import rioxarray as rio
import matplotlib
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import geopandas as gpd
import pandas as pd
import pyproj
import shapefile
from shapely.geometry import shape
import rasterio.mask
import fiona


# In[ ]:


# 1 unzips all .ZIP files in a directory then deletes the original .ZIP file

def unzip_and_delete(directory):
    """Unzips all zip files in a directory and then deletes the original zip files.

    Args:
        directory: The path to the directory containing the zip files.
    """
    for filename in os.listdir(directory):
        if filename.endswith(".ZIP"):
            file_path = os.path.join(directory, filename)
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                os.remove(file_path)
                print(f"Unzipped and deleted: {filename}")
            except zipfile.BadZipFile:
                print(f"Skipping invalid zip file: {filename}")
            except Exception as e:
                 print(f"An error occurred processing {filename}: {e}")
                

# Example usage:
base_directory = "/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test"  # Path to directory with .ZIP files
unzip_and_delete(base_directory)


# In[ ]:


# 2 Renames all folders ending in .SEN3 in a directory
# Renames the folder to the acquisiton start date-time extracted from the long original file name

# Define the base directory containing the extracted .SEN3 subfolder
base_directory = r"/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test"

# Loop through all items in the base directory
for item in os.listdir(base_directory):
    item_path = os.path.join(base_directory, item)
    
    # Process only directories that end with ".SEN3" (case-insensitive)
    if os.path.isdir(item_path) and item.upper().endswith('.SEN3'):
        # Create the new folder name using characters of the current name
        new_folder_name = item[16:31] + "_use"
        new_folder_path = os.path.join(base_directory, new_folder_name)
        
        # Check to ensure the new folder name doesn't already exist
        if not os.path.exists(new_folder_path):
            os.rename(item_path, new_folder_path)
            print(f"Renamed folder: {item} -> {new_folder_name}")

            # Update base_directory to the newly renamed folder
            base_directory = new_folder_path
            print(f"New working directory: {base_directory}")
        else:
            print(f"Skipping {item}: {new_folder_name} already exists.")


# In[ ]:


# 3 Deletes netCDFs that are not needed
# Prints list of deleted and kept files

# Define the base or "parent" directory containing the extracted subfolder that has the .nc files
# Same as "base directory" in steps 1-2
parent_directory = r"/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test"

# List of filenames to keep (modify as needed)
files_to_keep = [
    "cloud.nc", "common_flags.nc", "cqsf.nc", "geo_coordinates.nc", 
    "iop_nn.nc", "par.nc", "tie_geo_coordinates.nc", "tie_geometries.nc", 
    "time_coordinates.nc", "tsm_nn.nc", "wqsf.nc"
]

# Loop through all items in the parent directory
for folder_name in os.listdir(parent_directory):
    folder_path = os.path.join(parent_directory, folder_name)

    # Process only directories that end with "_use"
    if os.path.isdir(folder_path) and folder_name.endswith("_use"):
        print(f"Processing folder: {folder_name}")

        # Set this folder as the new base directory
        base_directory = folder_path

        # Loop through all files in the folder
        for file in os.listdir(base_directory):
            file_path = os.path.join(base_directory, file)

            # Delete the file if it's not in the keep list
            if os.path.isfile(file_path) and file not in files_to_keep:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            else:
                print(f"Kept: {file_path}")

        # Change the working directory to the newly set base directory
        os.chdir(base_directory)
        print(f"New working directory: {os.getcwd()}")

print("Cleanup process complete.")


# In[ ]:


# 4 Add lat/lon as spatial dimensions to TSM netcdf from geo_coordinates netcdf

# **Step 1: Open the NetCDF files**
# NetCDF with data but missing lat/lon dimensions
tsm_nc = os.path.join(os.getcwd(), "tsm_nn.nc")  # Absolute path
#tsm_nc = "tsm_nn.nc"  # Relative path to file

# NetCDF containing lat/lon dimensions
geocoord_nc = os.path.join(os.getcwd(), "geo_coordinates.nc")  # Absolute path
#geocoord_nc = "geo_coordinates.nc"   # Relative path to file

ds_tsm = xr.open_dataset(tsm_nc)
ds_geocoord = xr.open_dataset(geocoord_nc)

# **Step 2: Extract latitude & longitude**
lat = ds_geocoord["latitude"].values  # Extract lat as NumPy array
lon = ds_geocoord["longitude"].values  # Extract lon as NumPy array

# **Step 3: Ensure lat/lon are correctly formatted**
if lat.ndim == 2:  # If 2D, extract unique values along the correct axis
    lat = lat[:, 0]  # Take the first column (assuming lat is constant across rows)
if lon.ndim == 2:
    lon = lon[0, :]  # Take the first row (assuming lon is constant across columns)

print(f"Latitude shape after fix: {lat.shape}")  # Should be (4289,)
print(f"Longitude shape after fix: {lon.shape}")  # Should be (4481,)

# **Step 4: Extract Data & Ensure Correct Shape**
var_name = list(ds_tsm.data_vars.keys())[0]  # Get first variable name
data_values = ds_tsm[var_name].values  # Extract data

# If data is 3D (e.g., time, lat, lon), select the first time step
if data_values.ndim == 3:
    data_values = data_values[0, :, :]

# Ensure the data shape matches (lat, lon)
if data_values.shape != (len(lat), len(lon)):
    raise ValueError(
        f"Mismatch: data shape {data_values.shape} vs expected ({len(lat)}, {len(lon)})"
    )

print(f"Final Data shape: {data_values.shape}")  # Should match (4289, 4481)

# **Step 5: Create a new NetCDF dataset with correct dimensions**
ds_new = xr.Dataset(
    {
        var_name: (["latitude", "longitude"], data_values)
    },
    coords={
        "latitude": ("latitude", lat),  # Assign dimensions explicitly
        "longitude": ("longitude", lon)
    }
)

# **Step 6: Save the updated dataset**
tsm_nc_spdm = os.path.join(os.getcwd(), "tsm_nn_spdm.nc")  # Absolute path
#tsm_nc_spdm = "tsm_nn_spdm.nc"  # Relative path to file
ds_new.to_netcdf(tsm_nc_spdm)


# In[ ]:


# 5 Convert TSM netcdf to geotiff

# Define path to input netCDF
tsm_nc_spdm = os.path.join(os.getcwd(), "tsm_nn_spdm.nc")  # Absolute path

# Open netcdf
tsm_nc_spdm = xr.open_dataset(tsm_nc_spdm)

# Extract variable
tsm = tsm_nc_spdm['TSM_NN']

# Define spatial dimentions
tsm = tsm.rio.set_spatial_dims(x_dim='longitude', y_dim='latitude')

# Assign CRS
tsm.rio.write_crs("epsg:4326", inplace=True)

# Define name of output GeoTIFF raster in the working directory
output_file = "tsm_nn_bad.tif" 

# Export netcdf as geotiff raster 
tsm.rio.to_raster(output_file)


# In[ ]:


# 6 Fix bounding box and transform in geotiff

def fix_geotiff(file_path, output_path):
    """
    Fixes the GeoTIFF file by manually setting the transform and CRS.
    
    :param file_path: Path to the GeoTIFF file
    :param output_path: Path to save the fixed GeoTIFF
    """
    with rasterio.open(file_path) as src:
        # Read the data
        data = src.read(1).astype(np.float32)

        # Define the bounding box (min_lon, max_lon, min_lat, max_lat)
        min_lon = -126.8  # Set your correct min longitude
        max_lon = -113.5   # Set your correct max longitude
        min_lat = 21.3   # Set your correct min latitude
        max_lat = 34.5    # Set your correct max latitude

        # Define the transform (top-left corner, pixel size)
        # from_origin(top_left_x, top_left_y, pixel_width, pixel_height)
        transform = from_origin(min_lon, max_lat, (max_lon - min_lon) / data.shape[1], (max_lat - min_lat) / data.shape[0])

        # Set the CRS (assuming WGS84 - EPSG:4326)
        crs = "EPSG:4326"

        # Write the fixed GeoTIFF
        with rasterio.open(output_path, 'w', driver='GTiff', 
                           count=1, dtype=data.dtype, 
                           crs=crs, transform=transform, 
                           width=data.shape[1], height=data.shape[0]) as dst:
            dst.write(data, 1)

# Example Usage
tsm_tif_bad = os.path.join(os.getcwd(), "tsm_nn_bad.tif")  # Absolute path
tsm_tif_fix = os.path.join(os.getcwd(), "tsm_nn_fix.tif")  # Absolute path

#tsm_tif_bad = "/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_bad.tif"
#tsm_tif_fix = "/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_fix.tif"

fix_geotiff(tsm_tif_bad, tsm_tif_fix)


# In[ ]:


# 7 Plot TSM fixed geotiff

def view_geotiff(file_path):
    """
    Opens and displays a GeoTIFF file, handling NaN/Inf values properly.

    :param file_path: Path to the GeoTIFF file
    """
    with rasterio.open(file_path) as src:
        data = src.read(1).astype(np.float32)  # Read the first band and ensure float32

        # Handle NoData values
        if src.nodata is not None:
            data[data == src.nodata] = np.nan  # Convert NoData to NaN

        # Check if all values are NaN
        if np.all(np.isnan(data)):
            raise ValueError(f"Error: The dataset {file_path} contains only NaN values and cannot be plotted.")

        # Ensure valid extent
        extent = [src.bounds.left, src.bounds.right, src.bounds.bottom, src.bounds.top]
        
        # Check if extent contains NaN or Inf
        if any(np.isnan(extent)) or any(np.isinf(extent)):
            raise ValueError(f"Error: The spatial extent contains NaN/Inf values: {extent}")

        # Mask invalid values for plotting
        data = np.ma.masked_invalid(data)

        # Plot the data
        plt.figure(figsize=(10, 6))
        plt.imshow(data, cmap="viridis", extent=extent, origin="upper")
        plt.colorbar(label="Value")
        plt.title(f"GeoTIFF Visualization: {file_path}")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.show()

# Example Usage
if __name__ == "__main__":
    tsm_tif_fix = os.path.join(os.getcwd(), "tsm_nn_fix.tif")  # Absolute path
    #tsm_tif_fix = "/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_fix.tif"  
    view_geotiff(tsm_tif_fix)


# In[ ]:


# 8 Clip fixed TSM GeoTIFF using shapefile - EDIT shapefile path before running

# Define input paths for the raster and shapefile
tsm_tif_fix = os.path.join(os.getcwd(), "tsm_nn_fix.tif")  # Absolute path
tsm_tif_fix_clip = os.path.join(os.getcwd(), "tsm_nn_fix_clip.tif")  # Absolute path
#tsm_tif_fix = '/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_fix.tif'
#tsm_tif_fix_clip = '/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_fix_clip.tif'

# MODIFY THIS WHEN RUNNING ON UC MERCED SYSTEM
shapefile_path = '/Users/lopezama/Documents/Blackwood/MERIS/ROI/west_us_poly/west_us_poly.shp'


# Open the shapefile and extract geometries
with fiona.open(shapefile_path, "r") as shapefile:
    shapes = [feature["geometry"] for feature in shapefile]

# Open the raster and clip it using the shapefile's geometries
with rasterio.open(tsm_tif_fix) as src:
    out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
    out_meta = src.meta

# Update metadata for the clipped raster
out_meta.update({
    "driver": "GTiff",
    "height": out_image.shape[1],
    "width": out_image.shape[2],
    "transform": out_transform
})

# Write the clipped raster to a new file
with rasterio.open(tsm_tif_fix_clip, "w", **out_meta) as dest:
    dest.write(out_image)


# In[ ]:


# 9 Plot clipped TSM fixed geotiff

def view_geotiff(file_path):
    """
    Opens and displays a GeoTIFF file, handling NaN/Inf values properly.

    :param file_path: Path to the GeoTIFF file
    """
    with rasterio.open(file_path) as src:
        data = src.read(1).astype(np.float32)  # Read the first band and ensure float32

        # Handle NoData values
        if src.nodata is not None:
            data[data == src.nodata] = np.nan  # Convert NoData to NaN

        # Check if all values are NaN
        if np.all(np.isnan(data)):
            raise ValueError(f"Error: The dataset {file_path} contains only NaN values and cannot be plotted.")

        # Ensure valid extent
        extent = [src.bounds.left, src.bounds.right, src.bounds.bottom, src.bounds.top]
        
        # Check if extent contains NaN or Inf
        if any(np.isnan(extent)) or any(np.isinf(extent)):
            raise ValueError(f"Error: The spatial extent contains NaN/Inf values: {extent}")

        # Mask invalid values for plotting
        data = np.ma.masked_invalid(data)

        # Plot the data
        plt.figure(figsize=(10, 6))
        plt.imshow(data, cmap="viridis", extent=extent, origin="upper")
        plt.colorbar(label="Value")
        plt.title(f"GeoTIFF Visualization: {file_path}")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.show()

# Example Usage
if __name__ == "__main__":
    tsm_tif_fix_clip = os.path.join(os.getcwd(), "tsm_nn_fix_clip.tif")  # Absolute path
    #tsm_tif_fix_clip = '/Users/lopezama/Documents/Blackwood/MERIS/test_data/workflow_test/20120407T182444/tsm_nn_fix_clip.tif'  
    view_geotiff(tsm_tif_fix_clip)

