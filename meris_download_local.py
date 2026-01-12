#!/usr/bin/env python
# coding: utf-8

# In[ ]:

"""
# MERIS Level 2 Data Downloader - Local Computer Version
# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov
#
# Queries and downloads MERIS Level 2 Full Resolution Full Swath Geophysical Product for Ocean, Land and Atmosphere from NASA EarthData Search
# Recommend batching downloads by year to avoid hitting data download limit errors if working with multi-year datasets
# 
# -------------
# BEFORE USING
# -------------
# Users must have .netrc for authentication set up before using this script, see resources below for assistance with this--
#   https://nsidc.org/data/user-resources/help-center/creating-netrc-file-earthdata-login
# 
# Users must have .txt file lists specifying which data files are to be downloaded
#   .txt file will all available MERIS files from NASA EarthData Search for southern California ROI as of September 2025 is provided 
#   Users must use the NASA EarthData Search interface to query data for their respective ROIs / time periods to generate the necessary .txt file list(s)
#   A tutorial for making .txt file lists using the NASA EarthData Search interface is provided in supplemental materials 
# 
# -----------------
# USERS MUST EDIT 
# -----------------
# file_lists starting ~line 43
# base_download_dir ~line 49
# base_log_dir ~line 50
# 
"""

# Packages
import os
from pathlib import Path
import csv
from datetime import datetime
import earthaccess

# -------------------
# USER SETTINGS
# -------------------
file_lists = [
    Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/workflow_tests/pleiades3/test_list1.txt"),
    Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/workflow_tests/pleiades3/test_list2.txt"),
    Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/workflow_tests/pleiades3/test_list3.txt"),
]

base_download_dir = Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/workflow_tests/pleiades3/data")
base_log_dir = Path("/Users/lopezama/Documents/Blackwood/MERIS/scripts/workflow_tests/pleiades3/logs")

# Ensure directories exist
base_download_dir.mkdir(parents=True, exist_ok=True)
base_log_dir.mkdir(parents=True, exist_ok=True)

# Master summary log
master_log_csv = base_log_dir / "master_download_log.csv"

# -------------------
# Authenticate using .netrc
# -------------------
earthaccess.login(strategy="netrc")


# -------------------
# Helper: process one batch
# -------------------
def process_batch(file_list: Path):
    batch_name = file_list.stem  # e.g., "test_list1"
    download_dir = base_download_dir / batch_name
    log_csv_path = base_log_dir / f"{batch_name}_download_log.csv"

    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n Starting batch: {batch_name}")
    print(f"  Download dir: {download_dir}")
    print(f"  Log file: {log_csv_path}")

    log_exists = log_csv_path.exists()
    with open(log_csv_path, "a", newline="") as log_file, \
         open(master_log_csv, "a", newline="") as master_file:

        writer = csv.writer(log_file)
        master_writer = csv.writer(master_file)

        # Write headers if needed
        if not log_exists:
            writer.writerow(["timestamp", "batch", "url", "filename", "status"])
        if not master_log_csv.exists():
            master_writer.writerow(["timestamp", "batch", "url", "filename", "status"])

        # Loop through each URL
        with open(file_list, "r") as f:
            for url in f:
                url = url.strip()
                if not url:
                    continue

                filename = Path(url).name
                local_file = download_dir / filename

                # Skip if already downloaded
                if local_file.exists() and local_file.stat().st_size > 0:
                    print(f"⏩ Skipping already downloaded file: {filename}")
                    status = "skipped (already exists)"
                    entry = [datetime.now().isoformat(), batch_name, url, filename, status]
                    writer.writerow(entry)
                    master_writer.writerow(entry)
                    continue

                print(f"\n Starting download: {filename}")

                try:
                    downloaded_paths = earthaccess.download(
                        url,
                        local_path=str(download_dir)
                    )

                    if downloaded_paths and Path(downloaded_paths[0]).exists():
                        print(f"✅ Download complete: {filename}")
                        status = "success"
                    else:
                        print(f"❌ Download failed: {filename}")
                        status = "failed"

                except Exception as e:
                    print(f"❌ Error: {e}")
                    status = f"error: {str(e)}"

                # Build log entry
                entry = [datetime.now().isoformat(), batch_name, url, filename, status]

                # Write to both logs
                writer.writerow(entry)
                master_writer.writerow(entry)

    print(f"Finished batch: {batch_name}")


# -------------------
# Run all batches
# -------------------
for fl in file_lists:
    if fl.exists():
        process_batch(fl)
    else:
        print(f"⚠️ Skipping missing file list: {fl}")

print("\n All batches processed!")

