#! /usr/bin/env bash
# Generate RMI models for DeLI-testbed benchmark datasets.
# Mirrors SOSD/scripts/build_rmis.sh.
#
# Must be run from the project root directory.
#
# Usage:
#   scripts/generate_rmi_models.sh
#
# For datasets that already have a spec in scripts/rmi_specs/<dataset>.json
# the spec is used directly (--param-grid), exactly like SOSD.
# For datasets without a pre-made spec the optimizer is run first to produce
# one, which is then saved for future runs.
#
# Generated files:
#   indices/rmi/   — C++ model sources (.h / .cpp / _data.h)
#   rmi_data/      — runtime parameter binaries (*_L*_PARAMETERS)
#
# After generation set DELI_RMI_PATH=<abs-path-to-rmi_data> before running
# the benchmark, or keep the default (rmi_data/ relative to CWD).

mkdir -p rmi_data indices/rmi


function build_rmi_set() {
    DATA_NAME=$1
    HEADER_PATH=indices/rmi/${DATA_NAME}_0.h
    JSON_PATH=scripts/rmi_specs/${DATA_NAME}.json

    # Skip if data file is absent
    if [ ! -f "data/$DATA_NAME" ]; then
        echo "Skipping $DATA_NAME (data/$DATA_NAME not found)"
        return
    fi

    # If no pre-made spec, generate one with the optimizer and save it
    if [ ! -f "$JSON_PATH" ]; then
        echo "No spec found for $DATA_NAME — running optimizer to create $JSON_PATH ..."
        indices/RMI/target/release/rmi data/$DATA_NAME \
            --optimize "$JSON_PATH" \
            --zero-build-time
    fi

    # Skip if already built
    if [ -f "$HEADER_PATH" ]; then
        echo "Skipping $DATA_NAME (already built)"
        return
    fi

    echo "Building RMI set for $DATA_NAME"
    indices/RMI/target/release/rmi data/$DATA_NAME \
        --param-grid "$JSON_PATH" \
        -d rmi_data/ \
        --threads 8 \
        --zero-build-time
    mv ${DATA_NAME}_* indices/rmi/
}


cd indices/RMI && cargo build --release && cd ../..


# ── Datasets with pre-made SOSD specs ────────────────────────────────────────
build_rmi_set books_200M_uint32
build_rmi_set books_800M_uint64
build_rmi_set fb_200M_uint64
build_rmi_set osm_cellids_800M_uint64
build_rmi_set wiki_ts_200M_uint64

# ── DeLI synthetic / real-world datasets (spec auto-generated on first run) ──
# build_rmi_set uniform_50M_uint32
# build_rmi_set lognormal_50M_uint32
# build_rmi_set normal_50M_uint32
# build_rmi_set exponential_50M_uint32
# build_rmi_set mix_gauss_50M_uint32
# build_rmi_set zipf_50M_uint32
# build_rmi_set companynet_uint32


# ── Regenerate indices/rmi/all_rmis.h ────────────────────────────────────────
scripts/rmi_specs/gen.sh
