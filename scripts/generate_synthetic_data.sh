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
