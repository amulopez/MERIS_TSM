#!/usr/bin/env python
# coding: utf-8

# In[ ]:

"""
# MERIS Level 2 Data Downloader - HPC version
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
# file_lists starting ~line 54
# base_download_dir ~line 69
# base_log_dir ~line 70
#
# -----------------------------------------------------
# BATCH OPTIONS (specify in corresponding shell script) 
# -----------------------------------------------------
# Run all batches: python download.py --all
# Run specific batches: meris_download_hpc.py --file_list 1 3
# Resume failed downloads for specific batches: meris_download_hpc.py --file_list 1 --resume
# Resume failed downloads for all batches: meris_download_hpc.py --all --resume
# 
"""

# Packages
import os
from pathlib import Path
import csv
from datetime import datetime
import argparse
import earthaccess

# -------------------
# USER SETTINGS
# -------------------

# User edits file lists as needed
file_lists = {
    "1": Path("/nobackup/amulcan/scripts/meris_mml/filelist/test_list1.txt"),
    "2": Path("/nobackup/amulcan/scripts/meris_mml/filelist/test_list2.txt"),
    "3": Path("/nobackup/amulcan/scripts/meris_mml/filelist/2003_list.txt")
}

base_download_dir = Path("/nobackup/amulcan/data/meris/downloads")
base_log_dir = Path("/nobackup/amulcan/data/meris/logs")

# Ensure directories exist
base_download_dir.mkdir(parents=True, exist_ok=True)
base_log_dir.mkdir(parents=True, exist_ok=True)

# Master summary log
master_log_csv = base_log_dir / "master_download_log.csv"

# -------------------
# Authenticate using .netrc
# -------------------
earthaccess.login(strategy="netrc")


def process_urls(batch_name, urls, download_dir, log_csv_path, mode="normal"):
    """Process a list of URLs for a batch (normal run or resume)."""
    log_exists = log_csv_path.exists()
    with open(log_csv_path, "a", newline="") as log_file, \
         open(master_log_csv, "a", newline="") as master_file:

        writer = csv.writer(log_file)
        master_writer = csv.writer(master_file)

        if not log_exists:
            writer.writerow(["timestamp", "batch", "url", "filename", "status"])
        if not master_log_csv.exists():
            master_writer.writerow(["timestamp", "batch", "url", "filename", "status"])

        for url in urls:
            url = url.strip()
            if not url:
                continue

            filename = Path(url).name
            local_file = download_dir / filename

            # Skip already downloaded
            if local_file.exists() and local_file.stat().st_size > 0:
                print(f" Skipping already downloaded file: {filename}")
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

            entry = [datetime.now().isoformat(), batch_name, url, filename, status]
            writer.writerow(entry)
            master_writer.writerow(entry)


def process_batch(batch_name: str, file_list: Path, resume=False):
    """Run a full batch download, or resume failed ones."""
    download_dir = base_download_dir / batch_name
    log_csv_path = base_log_dir / f"{batch_name}_download_log.csv"
    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n Starting batch: {batch_name}")
    print(f"  Download dir: {download_dir}")
    print(f"  Log file: {log_csv_path}")

    urls = []
    if resume and log_csv_path.exists():
        # Resume mode: only retry failed or error entries
        with open(log_csv_path, "r") as log_file:
            reader = csv.DictReader(log_file)
            for row in reader:
                if "failed" in row["status"] or "error" in row["status"]:
                    urls.append(row["url"])
        if not urls:
            print(f"✅ No failed downloads to retry for {batch_name}.")
            return
        print(f"🔄 Resuming {len(urls)} failed downloads...")
    else:
        # Normal mode: read full file list
        with open(file_list, "r") as f:
            urls = [line.strip() for line in f if line.strip()]

    process_urls(batch_name, urls, download_dir, log_csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download MERIS data batches.")
    parser.add_argument("--file_list", nargs="+", help="Specify batch numbers (e.g., 1 2 3)")
    parser.add_argument("--all", action="store_true", help="Run all batches")
    parser.add_argument("--resume", action="store_true", help="Retry only failed downloads")
    args = parser.parse_args()

    if args.all:
        batches_to_run = file_lists.keys()
    elif args.file_list:
        batches_to_run = args.file_list
    else:
        parser.error("You must specify --all or --file_list with one or more batch numbers.")

    for batch in batches_to_run:
        if batch in file_lists and file_lists[batch].exists():
            process_batch(f"file_list{batch}", file_lists[batch], resume=args.resume)
        else:
            print(f" Skipping: file_list{batch} (file not found)")

