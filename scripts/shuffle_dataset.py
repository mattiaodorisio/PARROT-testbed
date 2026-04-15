#!/usr/bin/env python3
"""
Shuffles a binary dataset file.

File format (matches load_binary_data in src/utils.h):
  - 8 bytes  : uint64  number of elements
  - N * size : raw elements (uint32 or uint64)

Usage:
  python3 scripts/shuffle_dataset.py <input_file> [output_file]

The element type is inferred from the filename suffix (_uint32 / _uint64).
If no output file is given, the shuffled data is written to <input_file>_shuffled.
"""

import sys
import numpy as np


def infer_dtype(filename: str) -> str:
    if "uint64" in filename:
        return "uint64"
    if "uint32" in filename:
        return "uint32"
    raise ValueError(
        f"Cannot infer element type from filename '{filename}'. "
        "Expected '_uint32' or '_uint64' in the name."
    )


def shuffle_dataset(infile: str, outfile: str) -> None:
    dtype = infer_dtype(infile)

    with open(infile, "rb") as f:
        size = np.frombuffer(f.read(8), dtype=np.uint64)[0]
        data = np.frombuffer(
            f.read(int(size) * np.dtype(dtype).itemsize), dtype=dtype
        ).copy()

    rng = np.random.default_rng(42)
    rng.shuffle(data)

    with open(outfile, "wb") as f:
        np.array([size], dtype=np.uint64).tofile(f)
        data.tofile(f)

    print(f"Shuffled {size} {dtype} elements  ->  {outfile}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input_file> [output_file]")
        sys.exit(1)

    infile = sys.argv[1]
    outfile = sys.argv[2] if len(sys.argv) > 2 else infile + "_shuffled"

    shuffle_dataset(infile, outfile)
