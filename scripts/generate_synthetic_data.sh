#!/bin/bash

# Ensure to be in the root folder of the project
if [ ! -f "CMakeLists.txt" ]; then
  echo "Please run this script from the root folder of the project."
  exit 1
fi

set -e

cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc) generate_datasets

./generate_datasets --data_dir=../data

cd ..

# Plot CDFs for all generated binary datasets.
set +e
trap 'echo "Error while plotting the distributions, but the datasets have been generated correctly!"; exit 0' ERR
set -e
echo "Plotting CDFs..."
for f in \
    data/normal_uint32      \
    data/exponential_uint32 \
    data/mix_gauss_uint32   \
    data/zipf_uint32        \
    data/uniform_uint32
do
    if [ -f "$f" ]; then
        python3 plotter/plot_cdf.py "$f"
    else
        echo "  Skipping missing file: $f"
    fi
done
echo "CDFs saved in data/."
