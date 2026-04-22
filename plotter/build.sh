#!/bin/bash

set -e

OUTPUT_DIR=build/figures
mkdir -p "$OUTPUT_DIR"

./plot.py --output-dir "$OUTPUT_DIR" --multiplot-template paper_plots_big.tex \
    --global-filter "index_name!=DeLI-Static-Payload;index_name!=DeLI-Dynamic-Payload" \
    ../results_big/

for tex_file in "$OUTPUT_DIR"/*.tex; do
    echo "Compiling $tex_file ..."
    if pdflatex -output-directory="$OUTPUT_DIR" "$tex_file"; then
        base="${tex_file%.tex}"
        rm -f "${base}.aux" "${base}.log" "${base}.out"
    else
        echo "WARNING: compilation failed for $tex_file"
    fi
done

echo "Done. PDFs are in $OUTPUT_DIR/"
