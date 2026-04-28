# PARROT Testbed

This repository contains the experimental evaluation code for the PARtitioned RObin hood Table (PARROT).

## Reproducibility

**1. Download this repo.**
Clone the repo and its dependencies
```bash
git clone https://github.com/mattiaodorisio/PARROT-testbed.git
git submodule update --init --recursive
mkdir build
```

**2. Generate datasets.**
Generate the synthetic datasets and/or download the SOSD datasets:

```bash
./script/generate_synthetic_data.sh
./script/download_datasets.sh
```

**3. Run experiments.**

```bash
./script/run_benchmarks.sh
```

This compiles and runs a subset of index configurations. To include all configurations, comment out the `FAST_COMPILE` macro in `src/utills.h` (note: this significantly increases compilation time due to the large number of template instantiations).

**4. Generate plots.**

```bash
cd plotter
make
```

The generated plots are placed in `plotter/build`.

---

> **Note:** The experimental evaluation is currently being extended.
