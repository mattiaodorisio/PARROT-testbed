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

# Run the benchmark for each file in the data folder
for file in ../data/*; do
    echo "Running benchmark for $file"
    ./deLi_testbed --keys_file=$file --batch_size=100000 --output_folder=../results --print_batch_stats
    break
done

cd ..
