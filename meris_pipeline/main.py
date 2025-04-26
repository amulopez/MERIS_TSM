from pathlib import Path
from utils.config import load_config
from query.query import query_and_download_meris
from processing.stack_tsms_from_geotiffs import stack_tsms_from_geotiffs
import dask
from dask.diagnostics import ProgressBar

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="MERIS TSM Pipeline")
    parser.add_argument("--config", type=str, required=True, help="Path to config.yaml")
    parser.add_argument("--skip_query", action="store_true", help="Skip query and download")
    parser.add_argument("--shapefile", type=str, help="Optional shapefile for clipping")
    return parser.parse_args()

def main():
    args = parse_args()
    config = load_config(args.config)

    bbox = config["bbox"]
    start = config["start"]
    end = config["end"]
    output = Path(config.get("out", "./MERIS_downloads"))

    if not args.skip_query:
        print("🔎 Starting query and download...")
        query_and_download_meris(bbox, start, end, output)

    print("\U0001F4DA Starting stacking...")
    geotiff_folder = output / "geotiffs"

    with dask.config.set(scheduler="threads"):
        with ProgressBar():
            stacked = stack_tsms_from_geotiffs(
                geotiff_folder,
                shapefile=args.shapefile,
                bbox=bbox
            )

    out_stacked = output / "stacked_tsm.nc"
    stacked.to_netcdf(out_stacked)
    print(f"✅ Saved stacked TSM to {out_stacked}")

if __name__ == "__main__":
    main()
