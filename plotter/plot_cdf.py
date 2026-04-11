import sys, os
import struct
import numpy as np
import matplotlib.pyplot as plt


def read_binary_dataset(filename):
  """Read a dataset written by generate_datasets.cpp.

  Format: uint64_t size, then N × uint32_t or uint64_t values.
  The element type is inferred from the filename suffix (_uint32 / _uint64).
  """
  if filename.endswith('_uint32'):
    dtype = np.dtype('<u4')   # little-endian uint32
  elif filename.endswith('_uint64'):
    dtype = np.dtype('<u8')   # little-endian uint64
  else:
    raise ValueError(f"Cannot infer element type from filename '{filename}'. "
                     "Expected suffix _uint32 or _uint64.")

  with open(filename, 'rb') as f:
    size = struct.unpack('<Q', f.read(8))[0]   # uint64_t header
    data = np.frombuffer(f.read(size * dtype.itemsize), dtype=dtype)

  if len(data) != size:
    raise ValueError(f"Expected {size} elements but read {len(data)}.")

  return data


def plot_cdf_binary(filename, output=None):
  """Plot the CDF of a binary dataset file produced by generate_datasets.cpp.

  output: path for the saved PNG; defaults to cdf_<basename>.png next to the input file.
  """
  data = read_binary_dataset(filename)
  data_sorted = np.sort(data)
  cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)

  plt.figure(figsize=(10, 6))
  plt.plot(data_sorted, cdf, linewidth=2)
  plt.xlabel('Value')
  plt.ylabel('Cumulative Probability')
  plt.title(f'CDF — {os.path.basename(filename).replace("_", " ")}')
  plt.grid(True, alpha=0.3)
  plt.tight_layout()

  if output is None:
    dirpart  = os.path.dirname(filename) or '.'
    basename = os.path.basename(filename)
    output   = os.path.join(dirpart, f'cdf_{basename}.png')

  plt.savefig(output)
  print(f"Saved to {output}")
  plt.close()


def plot_cdf(filename, output=None):
  """Plot the CDF of a plain-text file (one number per line).

  output: path for the saved PNG; defaults to cdf_<basename>.png next to the input file.
  """
  numbers = []
  with open(filename, 'r') as f:
    for line in f:
      try:
        numbers.append(float(line.strip()))
      except ValueError:
        pass

  numbers.sort()
  cdf = np.arange(1, len(numbers) + 1) / len(numbers)

  plt.figure(figsize=(10, 6))
  plt.plot(numbers, cdf, linewidth=2)
  plt.xlabel('Value')
  plt.ylabel('Cumulative Probability')
  plt.title(f'CDF — {os.path.basename(filename).replace("_", " ")}')
  plt.grid(True, alpha=0.3)
  plt.tight_layout()

  if output is None:
    dirpart  = os.path.dirname(filename) or '.'
    basename = os.path.basename(filename)
    output   = os.path.join(dirpart, f'cdf_{basename}.png')

  plt.savefig(output)
  print(f"Saved to {output}")
  plt.close()


if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("Usage: python plot_cdf.py <filename> [output.png]")
    print("  Binary files (suffix _uint32 or _uint64) are detected automatically.")
    sys.exit(1)

  fname  = sys.argv[1]
  output = sys.argv[2] if len(sys.argv) >= 3 else None

  if fname.endswith('_uint32') or fname.endswith('_uint64'):
    plot_cdf_binary(fname, output)
  else:
    plot_cdf(fname, output)