#! /usr/bin/env python3
# This removes the huge values at the end of facebook dataset
import numpy as np
import struct
import sys

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <dataset_name>")
    sys.exit(1)

dataset_name = sys.argv[1]

data = np.fromfile(dataset_name, dtype=np.uint64)
print("FB data read")

data = data[1:]
data = data[(data < np.quantile(data, 0.99999))]

with open(dataset_name + "_cleaned", "wb") as f:
    f.write(struct.pack("Q", len(data)))
    data.tofile(f)

print(f"The FB dataset has been cleaned and now contains {len(data)} items")
