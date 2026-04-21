#!/usr/bin/env python3

import re
import sys
import argparse
import statistics
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.cm as cm

RESULT_RE = re.compile(r'^RESULT\s+')
FIELD_RE = re.compile(r'(\w+)=(\S+)')


def auto_cast(v):
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v


PINNED_VARIANTS = {
    # index_name -> variant string to plot, or None for best
    # 'DeLI-Static': 'GFB;1;70;BI;16;512',
    # 'DeLI-Dynamic': 'N;1;70;BI;16;512',
    # 'DeLI-Static-Payload': 'GFB;0;70;BI;16;512',
    # 'DeLI-Dynamic-Payload': 'N;0;70;BI;16;512',
}


def parse_file(path):
    records = []
    with open(path) as f:
        for line in f:
            if not RESULT_RE.match(line):
                continue
            rec = {k: auto_cast(v) for k, v in FIELD_RE.findall(line)}
            if 'throughput' in rec and isinstance(rec['throughput'], float):
                records.append(rec)
    return records


def compute_medians(records):
    # {(workload_type, index_name, index_variant): [throughput, ...]}
    groups = defaultdict(list)
    for r in records:
        key = (r['workload_type'], r['index_name'], r['index_variant'])
        groups[key].append(r['throughput'])
    return {k: statistics.median(v) for k, v in groups.items()}


def top3_per_index(medians):
    # {workload_type: {index_name: [(variant, median), ...]}}
    by_workload = defaultdict(lambda: defaultdict(list))
    for (wt, idx, var), med in medians.items():
        by_workload[wt][idx].append((var, med))
    result = {}
    for wt, indices in by_workload.items():
        result[wt] = {}
        for idx, variants in indices.items():
            variants.sort(key=lambda x: x[1], reverse=True)
            result[wt][idx] = variants[:3]
    return result


def print_results(top3):
    for wt in sorted(top3):
        print(f"\n=== WORKLOAD: {wt} ===")
        for idx in sorted(top3[wt]):
            print(f"  [{idx}]")
            for rank, (var, med) in enumerate(top3[wt][idx], 1):
                print(f"    {rank}. {var:<40}  {med:>15,.2f} ops/s")


def make_bar_plots(top3, out_prefix, medians):
    colors = cm.tab10.colors
    for wt in sorted(top3):
        indices = sorted(top3[wt])
        def pick(idx):
            pinned = PINNED_VARIANTS.get(idx)
            if pinned is not None:
                return (pinned, medians.get((wt, idx, pinned), 0.0))
            return top3[wt][idx][0]

        best = [(idx, pick(idx)) for idx in indices if top3[wt][idx]]
        names = [b[0] for b in best]
        throughputs = [b[1][1] for b in best]
        variant_labels = [b[1][0] for b in best]

        fig, ax = plt.subplots(figsize=(max(8, len(names) * 1.4), 5))
        bar_colors = [colors[i % len(colors)] for i in range(len(names))]
        bars = ax.bar(range(len(names)), throughputs, color=bar_colors)

        ax.set_xticks(range(len(names)))
        ax.set_xticklabels(names, rotation=0, ha='center', fontsize=10)
        ax.set_ylabel('Median throughput (ops/s)')
        ax.set_title(f'Best variant per index — {wt}')

        for bar, label in zip(bars, variant_labels):
            if label == 'none':
                continue
            label = str(label).replace(';512', '')
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.01,
                label,
                ha='center', va='bottom', fontsize=10, rotation=0,
            )

        ax.margins(y=0.2)
        plt.tight_layout()
        safe_wt = wt.replace('/', '_')
        out_path = f"{out_prefix}_{safe_wt}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"  [plot saved] {out_path}")


def main():
    parser = argparse.ArgumentParser(description='Show top-3 variants per index and plot best ones.')
    parser.add_argument('results_file', help='Path to benchmark results .txt file')
    parser.add_argument('--out', default='best_variants', help='Output PNG prefix (default: best_variants)')
    args = parser.parse_args()

    records = parse_file(args.results_file)
    if not records:
        print("No valid RESULT lines found.", file=sys.stderr)
        sys.exit(1)

    medians = compute_medians(records)
    top3 = top3_per_index(medians)
    print_results(top3)
    print()
    make_bar_plots(top3, args.out, medians)


if __name__ == '__main__':
    main()
