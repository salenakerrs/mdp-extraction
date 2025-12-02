#!/bin/bash

pos_dt="$1"
source_extension="$2"
source_path="$3"

# Validate input
if [ -z "$pos_dt" ]; then
    echo "Error: Pos_dt is Null!"
    exit 1
fi

if [ -z "$source_extension" ]; then
    echo "Error: Source Extension is Null!"
    exit 1
fi

if [ -z "$source_path" ]; then
    echo "Error: Source File not found!"
    exit 1
fi

base_file_name=$(basename ${source_path})
path_only=$(dirname ${source_path})
DATE_OF_SOURCE_DATA=$(date -d "$pos_dt" +"%Y%m%d")
DATE_OF_GENERATED_DATA=$(date +"%Y%m%d")  # Current system date
source_extension=${source_extension,,}

# Define file pattern for searching
FILE_PATTERN="${base_file_name}_${DATE_OF_SOURCE_DATA}*.${source_extension}"

# Find matching files safely
FILES=$(find "$path_only" -type f -name "${FILE_PATTERN}" 2>/dev/null)

if [ -z "$FILES" ]; then
    echo "Error: No files found matching pattern '$FILE_PATTERN'"
    exit 1
fi

# Initialize an associative array to hold data for each date
declare -A records_by_date
declare -A files_by_date

# Process each file
for INPUT_FILE in $FILES; do
    if [ ! -f "$INPUT_FILE" ]; then
        continue
    fi

    # Count and check the number of date patterns in the file names are greater than 2 or not
    count=$(echo "$INPUT_FILE" | grep -o '[0-9]\{8\}' | wc -l)
    result=$( [ "$count" -ge 2 ] && echo "yes" || echo "no" )

    # Extract DATE_OF_DATA from the filename (assuming format kplus_stream_test_YYYYMMDD*)
    # case YYYYMMDD >= 2 -> YYYYMMDD_YYYYMMDD
    if [ "$result" == "yes" ]; then
        DATE_OF_DATA=$(echo "$INPUT_FILE" | sed -nE 's/.*([0-9]{8})_[0-9]+_([0-9]{8}).*/\1_\2/p')
    else
    # case YYYYMMDD < 2
        DATE_OF_DATA=$(echo "$INPUT_FILE" | sed -nE 's/.*([0-9]{8}).*/\1/p')
    fi

    # Count data rows (excluding header)
    RECORDS=$(($(wc -l < "$INPUT_FILE") - 1))

    # Add the number of records to the corresponding date
    records_by_date["$DATE_OF_DATA"]=$((records_by_date["$DATE_OF_DATA"] + RECORDS))

    # Add the file to the list for that date
    files_by_date["$DATE_OF_DATA"]+=$(basename "$INPUT_FILE")", "
done

# Generate control files for each date
for DATE_OF_DATA in "${!records_by_date[@]}"; do
    # Remove the trailing comma from the file list
    FILE_LIST=$(echo "${files_by_date["$DATE_OF_DATA"]}" | tr ',' '\n' | sort | tr '\n' ',' | sed 's/,$//')

    # Define the dynamic output .ctl file name in the same directory
    OUTPUT_CTL="${path_only}/${base_file_name}_${DATE_OF_DATA}.ctl"

    # Create CTL file with metadata
    cat <<EOF > "$OUTPUT_CTL"
"date_of_data"|"date_of_generated_data"|"number_of_records"|"source_file_name"
"$DATE_OF_DATA"|"$DATE_OF_GENERATED_DATA"|"${records_by_date[$DATE_OF_DATA]}"|"${FILE_LIST}"
EOF

    echo "Control file '$OUTPUT_CTL' has been created successfully in '$path_only'."
done
