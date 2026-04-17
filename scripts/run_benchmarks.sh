#!/bin/bash

# Ensure to be in the root folder of the project
if [ ! -f "CMakeLists.txt" ]; then
  echo "Please run this script from the root folder of the project."
  exit 1
fi

set -e

mkdir -p build results
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

LOG_FILE="../results/deLi_testbed_$(date +%Y%m%d_%H%M%S).log"
: > "$LOG_FILE"
echo "Writing benchmark log to $LOG_FILE"

# List the file in ../data/enabled_datasets.txt and run the benchmark for each of them
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" == \#* ]]; then
      echo "Skipping $line"
      continue
    fi

    # Parse optional "entire" keyword (e.g. "books_200M_uint64 entire")
    dataset=$(awk '{print $1}' <<< "$line")
    entire_flag=""
    if grep -qw "entire" <<< "$line"; then
      entire_flag="--entire_dataset"
    fi

    if [ ! -f "../data/$dataset" ]; then
      echo "File ../data/$dataset does not exist. Skipping."
      continue
    fi
    echo "Running benchmark for $dataset${entire_flag:+ (entire dataset)}"
    DELI_RMI_PATH=../rmi_data ./deLi_testbed --keys_file=../data/$dataset --max_batches=5 --batch_size=2048 --output_folder=../results --print_batch_stats --pareto $entire_flag >> "$LOG_FILE" 2>&1
done < "../data/enabled_datasets.txt"

cd ../plotter
./plot.py --output plots.tex ../results/

cd ..
