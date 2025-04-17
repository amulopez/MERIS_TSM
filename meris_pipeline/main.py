import argparse
from pathlib import Path
from utils.config import load_config
from query.query import query_and_download_meris
from stack_tsms import stack_regridded_tsms, load_stacked_tsms_from_geotiffs
from processing.postprocess_and_geotiff import postprocess_and_geotiff_granule

def parse_args():
    parser = argparse.ArgumentParser(description="Download, process, and stack MERIS TSM L2 data")
    parser.add_argument("--config", type=str, required=True, help="Path to YAML config file")
    parser.add_argument("--skip_query", action="store_true", help="Skip query and download (process existing ZIPs)")
    parser.add_argument("--shapefile", type=str, help="Optional shapefile to crop final stack")
    return parser.parse_args()

def main():
    args = parse_args()
    config = load_config(args.config)

    bbox = config["bbox"]
    start = config["start"]
    end = config["end"]
    output = Path(config.get("out", "./MERIS_downloads"))

    if not args.skip_query:
        print("\n🔎 Starting query and download...")
        query_and_download_meris(bbox, start, end, output)
    else:
        print("\n⏩ Skipping query step, proceeding with postprocessing...")

    print("\n🛠️ Postprocessing downloaded ZIP files...")
    zip_files = list((output).glob("*.ZIP"))

    if not zip_files:
        print(f"⚠️ No ZIP files found in {output}. Check your setup.")
    else:
        for zip_path in zip_files:
            postprocess_and_geotiff_granule(zip_path, output)

    print("\n📚 Starting stacking...")
    geotiff_folder = output / "geotiffs"

    if geotiff_folder.exists() and any(geotiff_folder.glob("*.tif")):
        print(f"⚡ Loading stack directly from saved GeoTIFFs in {geotiff_folder}...")
        stacked = load_stacked_tsms_from_geotiffs(geotiff_folder)
    else:
        print(f"❗ No GeoTIFFs found, attempting direct regridding from processed folders...")
        stacked = stack_regridded_tsms(output, clip_to=args.shapefile)

    out_path = output / "stacked_tsm.nc"
    stacked.to_netcdf(out_path)
    print(f"\n✅ Final stacked dataset saved to: {out_path}")

if __name__ == "__main__":
    main()
