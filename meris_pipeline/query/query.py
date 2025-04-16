from earthaccess import Auth, search_data, download
from processing.postprocess import postprocess_granule
from pathlib import Path
import zipfile
import shutil


def query_and_download_meris(bbox, start, end, output):
    # Authenticate using earthaccess
    auth = Auth().login()

    # Search for granules in time and space range
    print("Searching for MERIS L2 Full Resolution granules...")
    granules = search_data(
        short_name="EN1_MDSI_MER_FRS_2P",
        cloud_hosted=False,
        bounding_box=tuple(bbox),
        temporal=(start, end)
    )

    if not granules:
        print("No granules found.")
        return

    print(f"Found {len(granules)} granules. Downloading sequentially (local mode)...")
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    processed = []
    for granule in granules:
        try:
            result = download([granule], local_path=output_path)
            if result:
                proc = postprocess_granule(result[0], output_path)
                if proc:
                    processed.append(proc)
        except Exception as e:
            print(f"Failed to process granule: {e}")

    print(f"Successfully processed {len(processed)} granules.")
