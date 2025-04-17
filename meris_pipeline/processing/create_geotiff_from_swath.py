import os
import zipfile
import shutil
import tempfile
from pathlib import Path
from processing.create_geotiff_from_swath import create_geotiff_from_swath

def postprocess_and_geotiff_granule(zip_path, output_root):
    """
    Fully postprocess a MERIS granule:
    - Unzip
    - Filter needed NetCDFs
    - Create CRS-aware GeoTIFF
    - Clean temp space
    - Delete original ZIP if successful
    """

    zip_path = Path(zip_path)
    output_root = Path(output_root)

    if zip_path.suffix.upper() != ".ZIP":
        print(f"⚠️ Skipping non-ZIP file: {zip_path.name}")
        return None

    zip_stem = zip_path.stem
    keep_files = ["tsm_nn.nc", "geo_coordinates.nc"]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                print(f"📦 Unzipped: {zip_path.name}")
        except zipfile.BadZipFile:
            print(f"❌ Bad ZIP file: {zip_path.name}")
            return None

        # Find required NetCDFs
        all_nc = list(temp_dir.rglob("*.nc"))
        needed_nc = {f.name.lower(): f for f in all_nc if f.name.lower() in keep_files}

        if len(needed_nc) < 2:
            print(f"❌ Missing required NetCDFs in {zip_path.name}")
            return None

        # Make output folders
        processed_folder = output_root / "processed" / zip_stem
        processed_folder.mkdir(parents=True, exist_ok=True)

        for name, path in needed_nc.items():
            shutil.copy2(path, processed_folder / name)
            print(f"✅ Copied {name}")

        # Now create GeoTIFF
        geotiff_folder = output_root / "geotiffs"
        geotiff_folder.mkdir(parents=True, exist_ok=True)

        create_geotiff_from_swath(
            tsm_nc_path=processed_folder / "tsm_nn.nc",
            geo_nc_path=processed_folder / "geo_coordinates.nc",
            output_tif_path=geotiff_folder / f"{zip_stem}.tif"
        )

        # Clean original zip if everything worked
        os.remove(zip_path)
        print(f"🗑️ Deleted ZIP: {zip_path.name}")

        return str(processed_folder)
