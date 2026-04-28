figures: build/summary_figure.pdf build/existing_figure.pdf build/books_LOOKUP_UNIFORM.png build/wiki_LOOKUP_UNIFORM.png

build/summary_figure.tex: summary_figure.tex
	./plot_sql.py summary_figure.tex --output build/summary_figure.tex

build/existing_figure.tex: existing_figure.tex
	./plot_sql.py existing_figure.tex --output build/existing_figure.tex

build/summary_figure.pdf: build/summary_figure.tex
	cd build && pdflatex summary_figure.tex

build/existing_figure.pdf: build/existing_figure.tex
	cd build && pdflatex existing_figure.tex

build/books_LOOKUP_UNIFORM.png: best_variants.py ../results_def/benchmark_books_800M_shuffled_uint64_20260419_124436.txt
	./best_variants.py ../results_def/benchmark_books_800M_shuffled_uint64_20260419_124436.txt --out build/books

build/wiki_LOOKUP_UNIFORM.png: best_variants.py ../results_def/benchmark_wiki_ts_200M_shuffled_uint64_20260420_171748.txt
	./best_variants.py ../results_def/benchmark_wiki_ts_200M_shuffled_uint64_20260420_171748.txt --out build/wiki
