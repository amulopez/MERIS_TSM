# MERIS TSM Processing Pipeline

This pipeline automates the end-to-end processing of MERIS Level-2 Full Resolution (FRS) Total Suspended Matter (TSM) data from NASA's LAADS DAAC, transforming swath granules into georeferenced GeoTIFFs suitable for coastal monitoring and large-scale analysis.
## Features

- Search and download MERIS granules using earthaccess
- Unzip and extract relevant NetCDFs (tsm_nn.nc and geo_coordinates.nc)
- Convert swath data to regular latitude-longitude grids (~300 m resolution)
- Save CRS-aware GeoTIFFs for analysis and visualization in GIS platforms
- Easily configurable via config.yaml

## Project Structure

```text
meris_pipeline/
├── main.py                   # Pipeline entry point
├── config.yaml               # Config file with bbox, date range, output path
├── query/                    # Earthdata search & download logic
│   └── query.py
├── processing/               # Postprocessing & swath-to-grid tools
│   └── postprocess.py
│   └── create_geotiff_from_swath.py
├── utils/                    # Utility functions
│   └── config.py
├── outputs/                  # Placeholder for processed outputs
├── requirements.txt          # Dependencies

```

## Usage
Run the pipeline using your custom config file:

```bash
python meris_pipeline/main.py --config meris_pipeline/config.yaml
```

You can optionally skip querying (e.g., to reprocess downloaded data):
```bash
python meris_pipeline/main.py --config meris_pipeline/config.yaml --skip_query
```

## Output
- CRS-aware GeoTIFFs saved to: MERIS_downloads/geotiffs/TSM_<granule_id>.tif
- Gridded using nearest-neighbor resampling (no interpolation)
- Standard output grid:
  - Latitude: based on image bounds (step ≈ 0.0027°)
  - Longitude: based on image bounds (step ≈ 0.0027°)
  - CRS: EPSG:4326 (WGS84)
- Variable: TSM (log-scaled total suspended matter)

## Coming Soon
- Time-stacked NetCDF/Zarr for multitemporal analysis
- Shapefile/ROI masking for regional subsets
- Parallelized batch processing for high-throughput servers
- Merging Sentinel 3 OLCI TSM results

## Dependencies
Install required packages with:

```bash
pip install -r requirements.txt
```

Includes:
- xarray, rioxarray, numpy, earthaccess 
- pyresample, GDAL, pyproj