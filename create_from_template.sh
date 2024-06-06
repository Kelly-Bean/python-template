#!/bin/bash

# Usage: ./script.sh <source_directory> <target_directory>
# Eg: bash create_from_template.sh . ../your-new-repo-name

# Assign command line arguments to variables
source_directory="$1"
target_directory="$2"

# Extract the target directory name without the path
target_dir_name=$(basename "$target_directory")

# Format target directory name for replacement in file contents and names
target_dir_name_underscore="${target_dir_name//-/_}"  # Replace dash with underscore

# Check if the source directory exists
if [ ! -d "$source_directory" ]; then
    echo "Source directory does not exist: $source_directory"
    exit 1
fi

# Check if the target directory exists, create it if it doesn't
if [ ! -d "$target_directory" ]; then
    echo "Target directory does not exist, creating: $target_directory"
    mkdir -p "$target_directory"
fi

# Copy all non-hidden files and directories from source to target
cp -r $source_directory/* $target_directory/

# Replace 'python-template' with the target directory name in file contents
find $target_directory -type f -exec perl -pi -e "s/python-template/$target_dir_name/g" {} \;

# Replace 'python_template' with the target directory name formatted with underscores in file contents
find $target_directory -type f -exec perl -pi -e "s/python_template/$target_dir_name_underscore/g" {} \;

# Rename files: replace 'python-template' with the target directory name
find $target_directory -depth -name '*python-template*' -exec sh -c '
    for file; do
        mv "$file" "$(dirname "$file")/$(basename "$file" | sed "s/python-template/$target_dir_name/")"
    done
' sh {} +

# Rename files: replace 'python_template' with the target directory name formatted with underscores
find $target_directory -depth -name '*python_template*' -exec sh -c '
    for file; do
        mv "$file" "$(dirname "$file")/$(basename "$file" | sed "s/python_template/$target_dir_name_underscore/")"
    done
' sh {} +

# Rename the specific directory 'python_template' if it exists
if [ -d "$target_directory/src/python_template" ]; then
    mv "$target_directory/src/python_template" "$target_directory/src/$target_dir_name_underscore"
fi

echo "Files copied and modified successfully from $source_directory to $target_directory"

