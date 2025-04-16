#!/bin/bash

# Set paths
WGET_SCRIPT="/Users/lopezama/Documents/Blackwood/MERIS/download_test/scripts/wget_test.sh"  # Modify with the actual path to your shell script
PYTHON_SCRIPT="/Users/lopezama/Documents/Blackwood/MERIS/download_test/scripts/TSM.py"  # Modify with the actual path to your Python script
DOWNLOAD_DIR="/Users/lopezama/Documents/Blackwood/MERIS/download_test/data"  # Modify with the actual folder where files are saved
TEXT_FILE_LIST="/Users/lopezama/Documents/Blackwood/MERIS/download_test/test_ed_list.txt"  # Modify with the text file that contains the download URLs

# Read the list of files to download
while IFS= read -r url; do
    echo "Starting download for: $url"

    # Run wget to download the first file in the list (simulate the shell script's operation)
    wget --user="INSERT_YOURS" --password="INSERT_YOUR_PASSWORD" --continue --directory-prefix="$DOWNLOAD_DIR" "$url"


    # Extract the filename from the URL
    file_name=$(basename "$url")

    # Ensure the file has finished downloading before proceeding
    while [ ! -f "$DOWNLOAD_DIR/$file_name" ]; do
        sleep 2  # Wait and check again
    done

    echo "Download complete: $file_name"

    # Run the Python script to process the downloaded file
    echo "Running Python script to process: $file_name"
    python3 "$PYTHON_SCRIPT" "$DOWNLOAD_DIR/$file_name"

    echo "Processing complete for: $file_name"
    echo "Resuming next download..."
    
done < "$TEXT_FILE_LIST"

echo "All downloads and processing complete!"
