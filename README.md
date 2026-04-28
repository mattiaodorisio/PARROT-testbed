# PARROR Testbed

This repo contains the code for the experimental evaluation for the PARtitioned RObin hood Table.

## Reproducibility

Genereate the synthetic datasets and/or download the SOSD datasets.

```bash
./script/generate_synthetic_data.sh
./script/download_datasets.sh
```

Run the experiments.

```bash
./script/run_benchmarks.sh
```

This runs a subset of the many configurations of the indexes. To run them all, comment the FAST_COMPILE macro on ```src/utills.h``` (this causes much slower compilation due to the many combinations of template parameters to be compiled).

Generate the plots.

```bash
cd plotter
make
```

The generated plots are in ```plotter/build```


TODOs: we are expanding the experimental evaluation.
