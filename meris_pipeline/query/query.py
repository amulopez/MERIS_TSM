from pathlib import Path
import earthaccess
from processing.postprocess import postprocess_granule
from processing.create_geotiff_from_swath import create_geotiff_from_swath

def query_and_download_meris(bbox, start, end, output_root):
    """
    Query and download MERIS TSM granules, postprocess, and create GeoTIFFs.
    """
    print("🔑 Logging into Earthdata...")
    auth = earthaccess.login(strategy="netrc", persist=True)

    if not auth.authenticated:
        raise RuntimeError("Earthdata login failed!")

    print(f"🌍 Searching MERIS L2 Full Resolution TSM (bbox={bbox}, start={start}, end={end})...")

    lon_min, lat_min, lon_max, lat_max = bbox

    granules = earthaccess.search_data(
        short_name="EN1_MDSI_MER_FRS_2P",
        bounding_box=(lon_min, lat_min, lon_max, lat_max),
        temporal=(start, end),
        cloud_hosted=True,
    )

    if not granules:
        print("⚠️ No MERIS TSM granules found matching query parameters.")
        return

    print(f"📦 Found {len(granules)} granules. Starting download and postprocessing...")

    output_root = Path(output_root)
    download_folder = output_root / "raw"
    geotiff_folder = output_root / "geotiffs"
    download_folder.mkdir(parents=True, exist_ok=True)
    geotiff_folder.mkdir(parents=True, exist_ok=True)

    downloaded_files = earthaccess.download(granules, download_folder)

    # Postprocess
    for file_path in downloaded_files:
        try:
            print(f"🧹 Postprocessing {file_path}...")

            output_folder = postprocess_granule(file_path, output_root)

            if output_folder is not None:
                create_geotiff_from_swath(output_folder, geotiff_folder)

        except Exception as e:
            print(f"❌ Failed to process {file_path}: {e}")

    print("🎉 Downloading, cleaning, and GeoTIFF creation complete!")
