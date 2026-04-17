#!/bin/bash

set -e
./plot.py --output plots.tex --multiplot-template paper_plots.tex ../results/
pdflatex -output-directory=build plots.tex
open build/plots.pdf
