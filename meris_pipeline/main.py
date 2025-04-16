import argparse
from utils.config import load_config
from query.query import query_and_download_meris
from stack_tsms import stack_regridded_tsms


def parse_args():
    parser = argparse.ArgumentParser(description="Query and download MERIS TSM (L2) data via EarthAccess")
    parser.add_argument("--config", type=str, required=True, help="Path to YAML config file")
    parser.add_argument("--shapefile", type=str, help="Optional shapefile path to crop the final stacked output")
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    bbox = config["bbox"]
    start = config["start"]
    end = config["end"]
    output = config.get("out", "./MERIS_downloads")

    query_and_download_meris(bbox, start, end, output)

    # After all postprocessing is complete
    print("Stacking regridded TSM data...")
    stacked = stack_regridded_tsms(output, clip_to=args.shapefile)

    output_name = "stacked_tsm_clipped.nc" if args.shapefile else "stacked_tsm.nc"
    stacked.to_netcdf(f"{output}/{output_name}")
    print(f"Saved stacked TSM to {output}/{output_name}")


if __name__ == "__main__":
    main()
