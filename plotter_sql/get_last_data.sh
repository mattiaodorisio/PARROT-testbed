#!/usr/bin/env bash
set -euo pipefail

# Script to get the most recent benchmark files and concatenate them
# File format: benchmark_<dataset>_<YYYYMMDD_HHMMSS>.txt

RESULTS_DIRS=("../results_synth_32_deli" "../results_synth_32_nondeli")
OUTPUT_FILE="combined_bench.txt"

# Check if results directories exist
for dir in "${RESULTS_DIRS[@]}"; do
    if [[ ! -d "$dir" ]]; then
        echo "Error: Results directory $dir not found" >&2
        exit 1
    fi
done

# Clear output file if it exists
> "$OUTPUT_FILE"

echo "Finding most recent benchmark files..."

# Get all benchmark files and group by dataset name
declare -A latest_files

# Process all benchmark files across all directories
for RESULTS_DIR in "${RESULTS_DIRS[@]}"; do
for file in "$RESULTS_DIR"/benchmark_*.txt; do
    if [[ ! -f "$file" ]]; then
        echo "No benchmark files found in $RESULTS_DIR" >&2
        continue
    fi
    
    # Extract dataset name and timestamp from filename
    # Pattern: benchmark_<dataset>_<YYYYMMDD_HHMMSS>.txt
    basename_file=$(basename "$file")
    
    # Skip files with "_ignore" in the filename
    if [[ $basename_file == *_ignore* ]]; then
        echo "Skipping ignored file: $basename_file" >&2
        continue
    fi

    if [[ $basename_file =~ ^benchmark_(.+)_([0-9]{8}_[0-9]{6})\.txt$ ]]; then
        dataset="${BASH_REMATCH[1]}"
        timestamp="${BASH_REMATCH[2]}"
        key="$RESULTS_DIR||$dataset"

        # Keep the most recent file per (directory, dataset) pair
        if [[ -z "${latest_files[$key]:-}" ]] || [[ "$timestamp" > "${latest_files[$key]}" ]]; then
            latest_files[$key]="$timestamp:$file"
        fi
    else
        echo "Warning: Skipping file with unexpected format: $basename_file" >&2
    fi
done
done

# Check if we found any files
if [[ ${#latest_files[@]} -eq 0 ]]; then
    echo "Error: No valid benchmark files found" >&2
    exit 1
fi

# Process files and add dataset_name to RESULT lines
for key in "${!latest_files[@]}"; do
    dataset="${key#*||}"
    timestamp_file="${latest_files[$key]}"
    file="${timestamp_file#*:}"

    # Process file content and add dataset_name attribute to RESULT lines
    while IFS= read -r line; do
        if [[ $line =~ ^RESULT ]]; then
            # Add dataset_name attribute after RESULT
            echo "$line dataset_name=$dataset" >> "$OUTPUT_FILE"
        else
            # Copy non-RESULT lines as-is
            echo "$line" >> "$OUTPUT_FILE"
        fi
    done < "$file"
done

echo ""
echo "Combined benchmark data written to: $OUTPUT_FILE"
echo "Total lines: $(wc -l < "$OUTPUT_FILE")"
echo "File size: $(du -h "$OUTPUT_FILE" | cut -f1)"