#!/bin/bash

set -e

cp performance_plot.tex build/performance_plot_build.tex
~/projects/sqlplot-tools/build/src/sqlplot-tools build/performance_plot_build.tex
pdflatex -output-directory=build -jobname=performance_plot build/performance_plot_build.tex
rm build/performance_plot.aux build/performance_plot.log
open build/performance_plot.pdf
