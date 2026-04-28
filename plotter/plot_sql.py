#!/usr/bin/env python3
"""
plot_sql.py — SQL-driven LaTeX pgfplots generator.

Loads a RESULT benchmark log into an in-memory SQLite database, then processes
a .tex template that contains special directives:

    %% IMPORT-DATA <table> <file>  — load a RESULT log file into a named table
    %% SQL <stmt>          — execute arbitrary SQL (preprocessing / views)
    %% MULTIPLOT <SELECT>  — emit \\addplot blocks; SELECT must return x, y, color [, marker]
    %% ROBUST_Y_MAX        — replace the ymax={...} in the same axis with a computed value
    %% SHARED_LEGEND       — emit shared \\addlegendimage/\\addlegendentry blocks

Usage:
    python plot_sql.py <template_file> [--output <out.tex>] [data_file] [--table <name>]
"""

import argparse
import math
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Color / marker palettes — same lists as plotter/plot.py
# ---------------------------------------------------------------------------

_AVAILABLE_COLORS: List[str] = [
    'blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray',
    'olive', 'cyan', 'magenta', 'yellow', 'black', 'teal', 'lime',
    '{rgb,255:red,0;green,128;blue,128}',
    '{rgb,255:red,0;green,100;blue,0}',
    '{rgb,255:red,70;green,130;blue,180}',
    '{rgb,255:red,255;green,140;blue,0}',
    '{rgb,255:red,75;green,0;blue,130}',
    '{rgb,255:red,220;green,20;blue,60}',
    '{rgb,255:red,0;green,206;blue,209}',
    '{rgb,255:red,255;green,215;blue,0}',
    '{rgb,255:red,107;green,142;blue,35}',
    '{rgb,255:red,72;green,61;blue,139}',
    '{rgb,255:red,160;green,82;blue,45}',
]

_AVAILABLE_MARKERS: List[str] = [
    '*', 'square*', 'triangle*', 'diamond*', 'pentagon*',
    '+', 'x', 'o', 'square', 'triangle', 'diamond', 'pentagon',
]

_series_color: Dict[str, str] = {}
_series_marker: Dict[str, str] = {}
_color_idx: int = 0
_marker_idx: int = 0


def reset_color_mapping() -> None:
    global _series_color, _series_marker, _color_idx, _marker_idx
    _series_color = {}
    _series_marker = {}
    _color_idx = 0
    _marker_idx = 0


def get_series_color(name: str) -> str:
    global _color_idx
    if name not in _series_color:
        _series_color[name] = _AVAILABLE_COLORS[_color_idx % len(_AVAILABLE_COLORS)]
        _color_idx += 1
    return _series_color[name]


def get_series_marker(name: str) -> str:
    global _marker_idx
    if name not in _series_marker:
        _series_marker[name] = _AVAILABLE_MARKERS[_marker_idx % len(_AVAILABLE_MARKERS)]
        _marker_idx += 1
    return _series_marker[name]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(conn: sqlite3.Connection, path: str, table: str = 'result') -> None:
    """Parse RESULT lines and insert into a SQLite table."""
    rows: List[Dict] = []

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith('RESULT'):
                continue
            data_part = line[len('RESULT'):].strip()
            pairs = re.findall(r'(\w+)=([^\s]+)', data_part)
            row: Dict = {}
            for key, value in pairs:
                try:
                    row[key] = int(value) if '.' not in value else float(value)
                except ValueError:
                    row[key] = value
            if row:
                rows.append(row)

    if not rows:
        raise ValueError(f"No RESULT lines found in {path}")

    # Collect all column names preserving first-seen order
    seen_cols: Dict[str, None] = {}
    for row in rows:
        for k in row:
            seen_cols[k] = None
    columns = list(seen_cols)

    col_defs = ', '.join(f'"{c}" TEXT' for c in columns)
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

    placeholders = ', '.join('?' for _ in columns)
    for row in rows:
        values = [str(row.get(c, '')) if row.get(c, '') != '' else None for c in columns]
        conn.execute(f'INSERT INTO "{table}" VALUES ({placeholders})', values)

    conn.commit()
    print(f"Loaded {len(rows)} rows into table '{table}' ({len(columns)} columns)",
          file=sys.stderr)


# ---------------------------------------------------------------------------
# Robust ymax — ported from plotter/plot.py
# ---------------------------------------------------------------------------

def _quantile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("empty data")
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    n = len(sorted_values)
    pos = (n - 1) * q
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def compute_robust_ymax(y_values: List[float], padding: float = 0.1) -> float:
    finite = sorted(v for v in y_values if math.isfinite(v))
    if not finite:
        return 1.0
    if len(finite) >= 4:
        q1, q3 = _quantile(finite, 0.25), _quantile(finite, 0.75)
        iqr = q3 - q1
        cutoff = q3 + 1.5 * iqr if iqr > 0 else q3
        non_outliers = [v for v in finite if v <= cutoff]
        base = max(non_outliers) if non_outliers else finite[-1]
    else:
        base = finite[-1]
    if base > 0:
        return base * (1.0 + padding)
    if base < 0:
        return base * (1.0 - padding)
    return 1.0


# ---------------------------------------------------------------------------
# LaTeX helpers
# ---------------------------------------------------------------------------

def escape_latex(s: str) -> str:
    replacements = {
        '\\': r'\textbackslash{}', '&': r'\&', '%': r'\%',
        '$': r'\$', '#': r'\#', '_': r'\_', '{': r'\{', '}': r'\}',
    }
    return ''.join(replacements.get(c, c) for c in str(s))


# ---------------------------------------------------------------------------
# Directive regexes
# ---------------------------------------------------------------------------

_MULTIPLOT_RE    = re.compile(r'^%%\s+MULTIPLOT\s+(.+)$',    re.IGNORECASE)
_SQL_RE          = re.compile(r'^%%\s+SQL\s+(.+)$',          re.IGNORECASE)
_IMPORT_DATA_RE  = re.compile(r'^%%\s+IMPORT-DATA\s+(\S+)\s+(.+)$',  re.IGNORECASE)
_ROBUST_RE       = re.compile(r'^%%\s+ROBUST_Y_MAX\s*$',      re.IGNORECASE)
_SHARED_RE       = re.compile(r'^%%\s+SHARED_LEGEND\s*$',     re.IGNORECASE)
_YMAX_LINE_RE    = re.compile(r'^(\s*)ymax=\{[^}]*\}(,?)\s*$')
_YMAX_CMT_RE     = re.compile(r'^\s*%\s*ymax\s*=')


# ---------------------------------------------------------------------------
# Series collection (pre-scan for SHARED_LEGEND)
# ---------------------------------------------------------------------------

def collect_all_series(conn: sqlite3.Connection, template: str) -> List[str]:
    """Execute every MULTIPLOT query and return sorted distinct color values."""
    series: Dict[str, None] = {}
    for line in template.splitlines():
        m = _MULTIPLOT_RE.match(line.strip())
        if not m:
            continue
        sql = m.group(1).strip()
        try:
            cur = conn.execute(sql)
            col_names = [d[0].lower() for d in cur.description]
            if 'color' not in col_names:
                continue
            color_idx = col_names.index('color')
            for row in cur.fetchall():
                val = row[color_idx]
                if val is not None:
                    series[str(val)] = None
        except Exception as e:
            print(f"Warning: could not collect series from query: {e}", file=sys.stderr)
    return sorted(series)


def run_import_data_directives(
    conn: sqlite3.Connection,
    template: str,
    template_dir: str,
) -> None:
    """Execute all %% IMPORT-DATA directives, loading each file into its own table."""
    for line in template.splitlines():
        m = _IMPORT_DATA_RE.match(line.strip())
        if not m:
            continue
        table = m.group(1).strip()
        filepath = m.group(2).strip()
        resolved = (
            os.path.join(template_dir, filepath)
            if not os.path.isabs(filepath)
            else filepath
        )
        print(f"IMPORT-DATA: loading '{resolved}' into table '{table}'", file=sys.stderr)
        load_data(conn, resolved, table)


def run_sql_directives(conn: sqlite3.Connection, template: str) -> None:
    """Execute all %% SQL directives in document order."""
    for line in template.splitlines():
        m = _SQL_RE.match(line.strip())
        if not m:
            continue
        stmt = m.group(1).strip()
        try:
            conn.execute(stmt)
            conn.commit()
            print(f"SQL: {stmt[:80]}{'...' if len(stmt) > 80 else ''}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: SQL directive failed: {e}\n  Statement: {stmt}", file=sys.stderr)


# ---------------------------------------------------------------------------
# addplot generation
# ---------------------------------------------------------------------------

def generate_addplots(
    rows: List[sqlite3.Row],
    col_names: List[str],
) -> Tuple[str, List[str], List[float]]:
    """
    Group query rows by color, emit \\addplot blocks.
    Returns (latex_str, series_names_in_appearance_order, all_y_values).
    """
    has_marker = 'marker' in col_names
    x_i     = col_names.index('x')
    y_i     = col_names.index('y')
    color_i = col_names.index('color')
    marker_i = col_names.index('marker') if has_marker else None

    # Group by color preserving insertion order
    groups: Dict[str, List] = {}
    for row in rows:
        color_val = str(row[color_i]) if row[color_i] is not None else ''
        groups.setdefault(color_val, []).append(row)

    out_lines: List[str] = []
    series_names: List[str] = []
    all_y: List[float] = []

    def safe_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    for color_val, group_rows in groups.items():
        marker_key = color_val
        if has_marker and group_rows:
            mk = group_rows[0][marker_i]
            if mk is not None:
                marker_key = str(mk)

        color  = get_series_color(color_val)
        marker = get_series_marker(marker_key)
        series_names.append(color_val)

        group_sorted = sorted(group_rows, key=lambda r: safe_float(r[x_i]))

        out_lines.append(f'% Series: {color_val}')
        out_lines.append(
            f'\\addplot[color={color}, mark={marker}, mark size=1.1pt] coordinates {{'
        )
        for row in group_sorted:
            x_val = row[x_i]
            y_val = row[y_i]
            try:
                all_y.append(float(y_val))
            except (TypeError, ValueError):
                pass
            try:
                x_fmt = f'{float(x_val):.6g}'
                y_fmt = f'{float(y_val):.6g}'
            except (TypeError, ValueError):
                x_fmt, y_fmt = str(x_val), str(y_val)
            out_lines.append(f'    ({x_fmt}, {y_fmt})')
        out_lines.append('};')
        out_lines.append('')

    return '\n'.join(out_lines), series_names, all_y


# ---------------------------------------------------------------------------
# Shared legend generation
# ---------------------------------------------------------------------------

def generate_shared_legend(series_names: List[str], max_entries: int = 30) -> str:
    capped = series_names[:max_entries]
    lines: List[str] = []
    for name in capped:
        color  = get_series_color(name)
        marker = get_series_marker(name)
        lines.append(f'\\addlegendimage{{color={color}, mark={marker}, mark size=1.1pt}}')
        lines.append(f'\\addlegendentry{{{escape_latex(name)}}}')
    if len(series_names) > max_entries:
        lines.append(f'% ({len(series_names) - max_entries} additional entries omitted)')
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Axis buffer processing
# ---------------------------------------------------------------------------

def process_axis_buffer(
    conn: sqlite3.Connection,
    buffer: List[str],
    shared_series: Optional[List[str]],
) -> List[str]:
    """
    Process one buffered axis environment and return the output lines.

    shared_series: the globally ordered series list when SHARED_LEGEND is active;
    new series found here are appended to it in-place.
    """
    has_robust = any(_ROBUST_RE.match(l.strip()) for l in buffer)

    multiplot_sql: Optional[str] = None
    multiplot_idx: Optional[int] = None
    for i, line in enumerate(buffer):
        m = _MULTIPLOT_RE.match(line.strip())
        if m:
            multiplot_sql = m.group(1).strip()
            multiplot_idx = i
            break

    shared_legend_idx: Optional[int] = None
    for i, line in enumerate(buffer):
        if _SHARED_RE.match(line.strip()):
            shared_legend_idx = i
            break

    addplot_latex = ''
    axis_series: List[str] = []
    robust_ymax: Optional[float] = None

    if multiplot_sql is not None:
        try:
            cur = conn.execute(multiplot_sql)
            col_names = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
            if not {'x', 'y', 'color'}.issubset(col_names):
                print(
                    f"Warning: MULTIPLOT query must return x, y, color columns. "
                    f"Got: {col_names}",
                    file=sys.stderr,
                )
            else:
                addplot_latex, axis_series, y_values = generate_addplots(rows, col_names)
                if has_robust and y_values:
                    robust_ymax = compute_robust_ymax(y_values)
                if shared_series is not None:
                    for s in axis_series:
                        if s not in shared_series:
                            shared_series.append(s)
        except Exception as e:
            print(f"Warning: MULTIPLOT query failed: {e}", file=sys.stderr)
            addplot_latex = f'% MULTIPLOT query failed: {escape_latex(str(e))}'

    out: List[str] = []
    skip_next_ymax_comment = False

    for i, line in enumerate(buffer):
        stripped = line.strip()

        # Drop %% ROBUST_Y_MAX marker
        if _ROBUST_RE.match(stripped):
            skip_next_ymax_comment = True
            continue

        # Drop the "% ymax=..." hint comment that immediately follows
        if skip_next_ymax_comment and _YMAX_CMT_RE.match(stripped):
            skip_next_ymax_comment = False
            continue
        skip_next_ymax_comment = False

        # Replace ymax={...} value
        if robust_ymax is not None:
            m = _YMAX_LINE_RE.match(line)
            if m:
                indent, comma = m.group(1), m.group(2)
                is_log_y = any('ymode=log' in l for l in buffer)
                out.append(f'{indent}ymax={{{robust_ymax:.6g}}}{comma}')
                if not is_log_y: # This should prevent extreme outliers to cause latex crash
                    out.append(f'{indent}restrict y to domain=0:{robust_ymax * 10:.6g},')
                continue

        # Drop %% SQL / %% IMPORT-DATA lines (already executed in pre-pass)
        if _SQL_RE.match(stripped) or _IMPORT_DATA_RE.match(stripped):
            continue

        # Replace %% MULTIPLOT line with \addplot blocks
        if i == multiplot_idx:
            if addplot_latex:
                out.append(addplot_latex)
            continue

        # Replace %% SHARED_LEGEND with actual entries
        if i == shared_legend_idx:
            legend_series = shared_series if shared_series is not None else axis_series
            out.append(generate_shared_legend(legend_series))
            continue

        out.append(line)

    return out


# ---------------------------------------------------------------------------
# Template processing
# ---------------------------------------------------------------------------

def process_template(
    conn: sqlite3.Connection,
    template: str,
    shared_series: Optional[List[str]],
) -> str:
    lines = template.splitlines(keepends=True)
    out: List[str] = []
    axis_depth = 0
    axis_buffer: List[str] = []

    for line in lines:
        stripped_line = line.rstrip('\n')

        enters = len(re.findall(r'\\begin\{axis\}', stripped_line))
        exits  = len(re.findall(r'\\end\{axis\}',   stripped_line))

        if axis_depth == 0 and enters == 0:
            # Outside any axis — pass through, but drop directive lines
            s = stripped_line.strip()
            if not _SQL_RE.match(s) and not _IMPORT_DATA_RE.match(s):
                out.append(line)
            continue

        # Entering or inside an axis
        axis_buffer.append(stripped_line)
        axis_depth += enters - exits

        if axis_depth == 0:
            processed = process_axis_buffer(conn, axis_buffer, shared_series)
            for pl in processed:
                out.append(pl + '\n')
            axis_buffer = []

    if axis_buffer:
        processed = process_axis_buffer(conn, axis_buffer, shared_series)
        for pl in processed:
            out.append(pl + '\n')

    return ''.join(out)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

class _MedianAgg:
    """SQLite custom aggregate for MEDIAN."""
    def __init__(self):
        self.values: List[float] = []

    def step(self, value) -> None:
        if value is not None:
            try:
                self.values.append(float(value))
            except (TypeError, ValueError):
                pass

    def finalize(self):
        if not self.values:
            return None
        return _quantile(sorted(self.values), 0.5)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate LaTeX pgfplots from a RESULT benchmark log via SQL directives'
    )
    parser.add_argument(
        'data_file', nargs='?', default=None,
        help='Path to benchmark RESULT log file (optional when IMPORT-DATA directives are used)',
    )
    parser.add_argument('template_file', help='Path to .tex template with directives')
    parser.add_argument('--output', '-o', default='-',
                        help='Output .tex file path (default: stdout)')
    parser.add_argument('--table', default='result',
                        help='SQLite table name for primary data_file (default: result)')
    args = parser.parse_args()

    conn = sqlite3.connect(':memory:')
    conn.create_aggregate('MEDIAN', 1, _MedianAgg)

    template_path = Path(args.template_file)
    template = template_path.read_text()
    template_dir = str(template_path.parent)

    # Load primary data file if given
    if args.data_file:
        load_data(conn, args.data_file, args.table)

    # Execute IMPORT-DATA directives (must happen before SQL directives that create views)
    run_import_data_directives(conn, template, template_dir)

    # Execute all %% SQL directives so views/tables are available to queries
    run_sql_directives(conn, template)

    # If SHARED_LEGEND is anywhere in the template, pre-assign colors globally
    shared_series: Optional[List[str]] = None
    if re.search(r'^%%\s+SHARED_LEGEND', template, re.MULTILINE | re.IGNORECASE):
        all_series = collect_all_series(conn, template)
        for name in all_series:
            get_series_color(name)
            get_series_marker(name)
        shared_series = list(all_series)
        print(
            f"SHARED_LEGEND: pre-assigned {len(all_series)} series: {all_series}",
            file=sys.stderr,
        )

    result = process_template(conn, template, shared_series)

    if args.output == '-':
        sys.stdout.write(result)
    else:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(result)
        print(f"Output written to {args.output}", file=sys.stderr)


if __name__ == '__main__':
    main()
