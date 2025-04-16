from pathlib import Path
import zipfile
import shutil
import os
import tempfile


def postprocess_granule(zip_path, output_root):
    """
    Unzips a MERIS granule ZIP to a temp dir, retains only necessary NetCDFs,
    and copies them to a permanent processed folder.
    """
    try:
        zip_path = Path(zip_path)

        # Skip non-ZIP files
        if zip_path.suffix.upper() != ".ZIP":
            print(f"Skipping non-ZIP file: {zip_path.name}")
            return None

        zip_stem = zip_path.stem
        keep_files = ["tsm_nn.nc", "geo_coordinates.nc"]

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)

            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    print(f"Unzipped {zip_path.name} to {temp_dir}")
            except zipfile.BadZipFile:
                print(f"Skipping {zip_path.name}: not a valid ZIP file.")
                return None

            # Recursively find .nc files
            all_nc = list(temp_dir.rglob("*.nc"))

            # Filter files to keep
            to_keep = [f for f in all_nc if f.name.lower() in keep_files]
            if len(to_keep) < 2:
                print(f"Missing required NetCDFs in {zip_path.name}")
                return None

            # Create processed output folder
            output_folder = Path(output_root) / "processed" / zip_stem
            output_folder.mkdir(parents=True, exist_ok=True)

            for f in to_keep:
                shutil.copy2(f, output_folder / f.name)
                print(f"Copied: {f.name} to {output_folder}")

            return str(output_folder)

    except Exception as e:
        print(f"Postprocessing failed for {zip_path}: {e}")
        return None
