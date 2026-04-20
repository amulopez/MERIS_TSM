from pathlib import Path
import earthaccess
from processing.postprocess import postprocess_granule

def query_and_download_meris(bbox, start, end, output_folder):
    """Query and download MERIS TSM granules using Earthaccess."""
    print("🔎 Starting query and download...")
    print("🔑 Logging into Earthdata...")
    earthaccess.login(strategy="interactive", persist=True)

    west, south, east, north = bbox  # Unpack bounding box

    print(f"Searching MERIS L2 Full Resolution TSM (bbox={bbox}, start={start}, end={end})...")
    granules = earthaccess.search_data(
        short_name="EN1_MDSI_MER_FRS_2P",
        bounding_box=(west, south, east, north),
        temporal=(start, end),
        cloud_hosted=True,
    )

    print(f"Found {len(granules)} granules. Starting download and postprocessing...")

    download_folder = Path(output_folder) / "downloads"
    download_folder.mkdir(parents=True, exist_ok=True)

    downloaded_files = earthaccess.download(granules)

    for file_path in downloaded_files:
        try:
            print(f"Postprocessing {Path(file_path).name}...")
            postprocess_granule(Path(file_path), output_folder)
        except Exception as e:
            print(f"Failed to process {file_path}: {e}")
