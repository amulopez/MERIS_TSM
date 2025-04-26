import zipfile
import tempfile
from pathlib import Path
from processing.create_geotiff_from_swath import create_geotiff_from_swath


def postprocess_granule(zip_path, output_root):
    """
    Unzips a MERIS granule ZIP, extracts required NetCDFs,
    and writes a CRS-aware GeoTIFF using nearest-neighbor swath-to-grid conversion.
    """
    zip_path = Path(zip_path)
    if zip_path.suffix.upper() != ".ZIP":
        print(f"⚠️ Skipping non-ZIP file: {zip_path.name}")
        return None

    zip_stem = zip_path.stem

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)

            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    print(f"🗜️ Unzipped {zip_path.name}")
            except zipfile.BadZipFile:
                print(f"❌ Bad ZIP: {zip_path.name}")
                return None

            # Look for required files
            tsm_path = next(temp_dir.rglob("tsm_nn.nc"), None)
            geo_path = next(temp_dir.rglob("geo_coordinates.nc"), None)

            if not tsm_path or not geo_path:
                print(f"❌ Missing required NetCDFs in {zip_path.name}")
                return None

            # Create output folder for GeoTIFFs
            geotiff_folder = Path(output_root) / "geotiffs"
            geotiff_folder.mkdir(parents=True, exist_ok=True)

            output_geotiff = geotiff_folder / f"TSM_{zip_stem}.tif"
            create_geotiff_from_swath(tsm_path, geo_path, output_geotiff)

            return str(output_geotiff)

    except Exception as e:
        print(f"❌ Postprocessing failed for {zip_path}: {e}")
        return None
