# MERIS TSM Processing Pipeline

This pipeline automates the querying, downloading, and preprocessing of MERIS Level-2 Full Resolution (FRS) Total Suspended Matter (TSM) data from NASA LAADS DAAC.

## Features

- Search and download MERIS granules using `earthaccess`
- Unzip and extract relevant NetCDFs (`TSM` and `geo_coordinates`)
- Interpolate swath data to a consistent lat/lon grid (~300m resolution)
- Save standardized NetCDFs for stacking or analysis

## Project Structure

```text
meris_pipeline/
├── main.py                   # Pipeline entry point
├── config.yaml               # Config file with bbox, date range, output path
├── query/                    # Handles Earthdata queries and downloads
│   └── query.py
├── processing/               # Contains postprocessing and regridding logic
│   └── postprocess.py
├── utils/                    # Utility functions (e.g. config loader)
│   └── config.py
├── outputs/                  # Directory for processed NetCDF/Zarr outputs
├── requirements.txt          # Project dependencies
```

## Usage

```bash
python meris_pipeline/main.py --config meris_pipeline/config.yaml
```

## Output
- Each .zip granule is unzipped, filtered, and interpolated.
- A regridded NetCDF is saved to: MERIS_downloads/regridded/<granule_id>.nc
- Output is standardized to:
  - Latitude: 30° to 50° (step = 0.0027)
  - Longitude: -130° to -110° (step = 0.0027)
- Variable: TSM(lat, lon)

## Coming Soon
- Time dimension stacking for seasonal/annual analysis
- Zarr output for scalable, chunked analysis
- Support for masking by shapefile/ROI