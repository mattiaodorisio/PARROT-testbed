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

# List the file in ../data/enabled_datasets.txt and run the benchmark for each of them
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" == \#* ]]; then
      echo "Skipping $line"
      continue
    else
      if [ ! -f "../data/$line" ]; then
        echo "File ../data/$line does not exist. Skipping."
        continue
      fi
      echo "Running benchmark for $line"
      ./deLi_testbed --keys_file=../data/$line --num_batches=3 --batch_size=8192 --output_folder=../results --print_batch_stats --pareto
    fi
done < "../data/enabled_datasets.txt"

cd ../plotter
./plot.py --output plots.tex ../results/

cd ..
