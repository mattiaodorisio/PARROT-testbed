#!/usr/bin/env python3
"""
Benchmark log parser and LaTeX pgfplot generator

This script parses benchmark logs in SQL-style format and generates LaTeX files
with pgfplots for visualization.

Example log format:
RESULT index_name=PGM init_num_keys=500 total_num_keys=1000 batch_size=1000 insert_frac=0.500000 lookup_distribution=zipf time_limit=0.500000 batch_no=2 cumulative_operations=1500 cumulative_lookups=1000 cumulative_inserts=500 safe_lookup_throughput=4191817.57 safe_insert_throughput=108248538.64 safe_overall_throughput=6168295.78 cumulative_time=0.000243

This is intended to be used as a more lightweight (but less powerful) alternative to sqlplot-tools by @bingmann.
A first draft of the code in this file has been generated with the help of an AI tool.
"""

import re
import os
import sys
import math
from typing import Dict, List, Tuple, Optional
import argparse
import statistics
import glob
from datetime import datetime

# Global color scheme for consistent series coloring
_SERIES_COLOR_MAP = {}
_AVAILABLE_COLORS = [
    # Primary colors
    'blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray',
    'olive', 'cyan', 'magenta', 'yellow', 'black', 'teal', 'lime',
    
    # RGB-defined colors for better compatibility (12 additional colors)
    '{rgb,255:red,0;green,128;blue,128}',      # maroon
    '{rgb,255:red,0;green,100;blue,0}',        # dark green
    '{rgb,255:red,70;green,130;blue,180}',     # steel blue  
    '{rgb,255:red,255;green,140;blue,0}',      # dark orange
    '{rgb,255:red,75;green,0;blue,130}',       # indigo
    '{rgb,255:red,220;green,20;blue,60}',      # crimson
    '{rgb,255:red,0;green,206;blue,209}',      # dark turquoise
    '{rgb,255:red,255;green,215;blue,0}',      # gold
    '{rgb,255:red,107;green,142;blue,35}',     # olive drab
    '{rgb,255:red,72;green,61;blue,139}',      # dark slate blue
    '{rgb,255:red,160;green,82;blue,45}',      # saddle brown
]
_COLOR_INDEX = 0

# Global marker scheme for consistent variant marking
_SERIES_MARKER_MAP = {}
_AVAILABLE_MARKERS = [
    '*',         # filled circle
    'square*',   # filled square
    'triangle*', # filled triangle
    'diamond*',  # filled diamond
    'pentagon*', # filled pentagon
    '+',         # plus
    'x',         # cross
    'o',         # hollow circle
    'square',    # hollow square
    'triangle',  # hollow triangle
    'diamond',   # hollow diamond
    'pentagon',  # hollow pentagon
]
_MARKER_INDEX = 0


def get_series_marker(variant_name: str) -> str:
    """
    Get a consistent marker for an index variant.
    Same variant will always get the same marker across all plots.

    Args:
        variant_name (str): Name of the variant (e.g., index_variant value)

    Returns:
        str: Marker name for pgfplots
    """
    global _SERIES_MARKER_MAP, _MARKER_INDEX

    if variant_name not in _SERIES_MARKER_MAP:
        _SERIES_MARKER_MAP[variant_name] = _AVAILABLE_MARKERS[_MARKER_INDEX % len(_AVAILABLE_MARKERS)]
        _MARKER_INDEX += 1

    return _SERIES_MARKER_MAP[variant_name]


def get_series_color(series_name: str) -> str:
    """
    Get a consistent color for a data series (e.g., index name).
    Same series will always get the same color across all plots.
    
    Args:
        series_name (str): Name of the data series (should be just the index_name)
        
    Returns:
        str: Color name for pgfplots
    """
    global _SERIES_COLOR_MAP, _COLOR_INDEX
    
    if series_name not in _SERIES_COLOR_MAP:
        if _COLOR_INDEX < len(_AVAILABLE_COLORS):
            _SERIES_COLOR_MAP[series_name] = _AVAILABLE_COLORS[_COLOR_INDEX]
            _COLOR_INDEX += 1
        else:
            # If we run out of predefined colors, cycle through them
            _SERIES_COLOR_MAP[series_name] = _AVAILABLE_COLORS[_COLOR_INDEX % len(_AVAILABLE_COLORS)]
            _COLOR_INDEX += 1
    
    return _SERIES_COLOR_MAP[series_name]


def reset_color_mapping():
    """Reset the color and marker mappings - useful for testing or when starting a new document."""
    global _SERIES_COLOR_MAP, _COLOR_INDEX, _SERIES_MARKER_MAP, _MARKER_INDEX
    _SERIES_COLOR_MAP = {}
    _COLOR_INDEX = 0
    _SERIES_MARKER_MAP = {}
    _MARKER_INDEX = 0


_LINEBREAK = '\x00'

def format_legend_label(label: str) -> str:
    """Format a legend label for display: trim the last semicolon-separated field when
    there are 6 fields, and replace 'DeLI' with 'PARROT'.
    Long labels get a line-break marker before ' (' which is resolved to \\\\ after escaping."""
    parts = label.split(';')
    if len(parts) == 6:
        label = ';'.join(parts[:4]) + ')'
    # if len(label) > 20:
    #     label = label.replace(' (', _LINEBREAK + '(')
    return (label.replace('DeLI', 'PARROT')
        .replace("Dynamic", "Dyn")
        .replace("Static", "Stat")
        .replace("Payload", "PL")
        .replace("SEA21-YFast", "YFastTrie")
        .replace("GFB", "GF"))


def escape_legend_label(label: str) -> str:
    """Format, escape, then resolve line-break markers to LaTeX \\\\."""
    formatted = format_legend_label(label)
    parts = formatted.split(_LINEBREAK)
    return '\\\\'.join(escape_latex_text(p) for p in parts)


def escape_latex_text(value) -> str:
    """Escape LaTeX special characters in text content."""
    text = str(value)
    replacements = {
        '\\': r'\textbackslash{}',
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
    }
    return ''.join(replacements.get(char, char) for char in text)


def initialize_color_mapping(data: List[Dict], groupby_col: str):
    """
    Initialize color mapping by discovering all unique series names in the data.
    This ensures consistent color assignment order across the entire document.
    
    Args:
        data (List[Dict]): All benchmark data
        groupby_col (str): Primary groupby column (e.g., 'index_name')
    """
    # Parse groupby clause to get the primary column name
    primary_groupby, _ = parse_groupby_clause(groupby_col)
    
    # Collect all unique series names (index names)
    series_names = set()
    for row in data:
        if primary_groupby in row:
            # Convert to string to ensure consistent types for sorting
            series_names.add(str(row[primary_groupby]))
    
    # Pre-assign colors to ensure consistent ordering
    for series_name in sorted(series_names):
        get_series_color(series_name)
    
    print(f"Color mapping initialized for {len(series_names)} series: {sorted(series_names)}")
    print(f"Color assignments: {dict(_SERIES_COLOR_MAP)}")


def parse_filter(filter_str: str) -> Optional[List[Tuple[str, str]]]:
    """
    Parse a filter string with multiple conditions separated by semicolons.
    Example: 'workload_type=lookup_only;batch_size=1000' into [(key1, value1), (key2, value2)].
    
    Args:
        filter_str (str): Filter string with conditions separated by semicolons
        
    Returns:
        List[Tuple[str, str]] or None: List of (key, value) pairs or None if no filter
    """
    if not filter_str or filter_str.strip() == '':
        return None
    
    conditions = []
    
    # First, temporarily replace escaped semicolons with a placeholder
    placeholder = "___ESCAPED_SEMICOLON___"
    temp_str = filter_str.replace('\\;', placeholder)
    
    # Split by unescaped semicolons to get individual conditions
    for condition in temp_str.split(';'):
        condition = condition.strip()
        # Match comparison operators (>=, <=, >, <) before falling back to =
        cmp_match = re.match(r'^([^><=!]+)(>=|<=|>|<)(.+)$', condition)
        if cmp_match:
            key = cmp_match.group(1).strip().replace(placeholder, ';')
            op  = cmp_match.group(2)
            value = cmp_match.group(3).strip().replace(placeholder, ';')
            conditions.append((key, op + value))
        elif '=' in condition:
            key, value = condition.split('=', 1)
            key = key.strip().replace(placeholder, ';')
            value = value.strip().replace(placeholder, ';')
            conditions.append((key, value))
    
    return conditions if conditions else None


def parse_aggregation(col_str: str) -> Tuple[str, str]:
    """
    Parse a column string that might contain aggregation like 'MEDIAN(total_time)'.
    
    Args:
        col_str (str): Column string, possibly with aggregation
        
    Returns:
        Tuple[str, str]: (aggregation_function, column_name)
    """
    col_str = col_str.strip()
    
    # Check for aggregation functions
    aggregation_pattern = r'^(MEDIAN|AVG|AVERAGE|MIN|MAX|SUM)\(([^)]+)\)$'
    match = re.match(aggregation_pattern, col_str, re.IGNORECASE)
    
    if match:
        func_name = match.group(1).upper()
        column_name = match.group(2).strip()
        return (func_name, column_name)
    else:
        return ('NONE', col_str)


def parse_groupby_clause(groupby_str: str) -> Tuple[str, Optional[Tuple[str, str]]]:
    """
    Parse a groupby clause that might contain BEST aggregation.
    
    Examples:
        'index_name' -> ('index_name', None)
        'index_name;BEST(index_variant)' -> ('index_name', ('BEST', 'index_variant'))
    
    Args:
        groupby_str (str): Groupby string, possibly with BEST aggregation
        
    Returns:
        Tuple[str, Optional[Tuple[str, str]]]: (primary_groupby_column, (agg_func, agg_column) or None)
    """
    groupby_str = groupby_str.strip()

    # Temporarily replace escaped semicolons so the top-level split works correctly.
    # Escaped semicolons appear in PIN variant values (e.g. GFB\;1\;40\;BI\;12\;512).
    _ESC = "___ESCAPED_SEMICOLON___"
    temp_str = groupby_str.replace('\\;', _ESC)

    # Split by unescaped semicolons to separate primary groupby from aggregation modifier
    parts = [part.strip().replace(_ESC, '\\;') for part in temp_str.split(';')]
    
    primary_groupby = parts[0]
    best_aggregation = None
    
    # Check for BEST or PIN aggregation in remaining parts
    for part in parts[1:]:
        best_pattern = r'^BEST\(([^)]+)\)$'
        match = re.match(best_pattern, part, re.IGNORECASE)
        if match:
            best_aggregation = ('BEST', match.group(1))
            break

        # PIN(col:IndexA=variantA,IndexB=variantB\;with\;semicolons)
        pin_pattern = r'^PIN\(([^:]+):(.+)\)$'
        match = re.match(pin_pattern, part, re.IGNORECASE)
        if match:
            variant_col = match.group(1).strip()
            map_str = match.group(2)
            placeholder = "___ESCAPED_SEMICOLON___"
            map_str = map_str.replace('\\;', placeholder)
            pin_map = {}
            for entry in map_str.split(','):
                if '=' in entry:
                    k, v = entry.split('=', 1)
                    k = k.strip().replace(placeholder, ';')
                    v = v.strip().replace(placeholder, ';')
                    pin_map[k] = v
            best_aggregation = ('PIN', variant_col, pin_map)
            break

    return (primary_groupby, best_aggregation)


def select_best_variant(data: List[Dict], primary_groupby_col: str, best_col: str, 
                       y_col: str, x_col: str) -> List[Dict]:
    """
    For each group in primary_groupby_col, select the best variant based on y_col performance.
    The "best" variant is the one with the lowest total y_col value across all x values.
    
    Args:
        data (List[Dict]): Input data (already filtered and aggregated)
        primary_groupby_col (str): Primary column to group by (e.g., 'index_name')
        best_col (str): Column to select best from (e.g., 'index_variant') 
        y_col (str): Y-axis column for performance comparison (e.g., 'total_time')
        x_col (str): X-axis column for grouping performance measurements
        
    Returns:
        List[Dict]: Data with only the best variant selected for each primary group
    """
    # Group by primary groupby column
    primary_groups = {}
    for row in data:
        if primary_groupby_col in row and best_col in row:
            primary_key = row[primary_groupby_col]
            if primary_key not in primary_groups:
                primary_groups[primary_key] = []
            primary_groups[primary_key].append(row)
    
    result_data = []
    
    # For each primary group, select the best variant
    for primary_key, primary_group_data in primary_groups.items():
        # Group by variant to calculate total performance for each variant
        variant_performance = {}
        
        for row in primary_group_data:
            if best_col in row and y_col in row and is_valid_plot_value(row[y_col]):
                variant = row[best_col]
                if variant not in variant_performance:
                    variant_performance[variant] = []
                variant_performance[variant].append(row[y_col])
        
        # Calculate total (sum) performance for each variant
        variant_totals = {}
        for variant, values in variant_performance.items():
            # Use sum of all y values as the total performance metric
            # Lower total means better performance (assuming y_col represents time/cost)
            variant_totals[variant] = sum(values)
        
        # Find the best variant (lowest total)
        if variant_totals:
            best_variant = min(variant_totals.keys(), key=lambda v: variant_totals[v])
            
            print(f"Best variant for {primary_key}: {best_variant} (total {y_col}: {variant_totals[best_variant]:.6f})")
            
            # Add all data points for the best variant
            for row in primary_group_data:
                if row.get(best_col) == best_variant:
                    result_data.append(row)
        
    return result_data


def select_pinned_variant(data: List[Dict], primary_groupby_col: str, variant_col: str,
                          y_col: str, x_col: str, pin_map: Dict[str, str]) -> List[Dict]:
    """
    For each primary group, use the variant specified in pin_map if the group key
    appears there; otherwise fall back to selecting the best-performing variant.

    Args:
        data (List[Dict]): Input data (already filtered and aggregated)
        primary_groupby_col (str): Primary column to group by (e.g., 'index_name')
        variant_col (str): Column holding the variant identifier (e.g., 'index_variant')
        y_col (str): Y-axis column used for BEST fallback comparison
        x_col (str): X-axis column used for BEST fallback comparison
        pin_map (Dict[str, str]): Mapping from primary group key to the desired variant value

    Returns:
        List[Dict]: Data containing only the selected variant for each primary group
    """
    primary_groups: Dict = {}
    for row in data:
        if primary_groupby_col in row and variant_col in row:
            primary_key = str(row[primary_groupby_col])
            if primary_key not in primary_groups:
                primary_groups[primary_key] = []
            primary_groups[primary_key].append(row)

    result_data = []
    for primary_key, group_rows in primary_groups.items():
        if primary_key in pin_map:
            pinned = pin_map[primary_key]
            selected = [r for r in group_rows if pinned in str(r.get(variant_col, ''))]
            if selected:
                print(f"Pinned variant for {primary_key}: {pinned}")
                result_data.extend(selected)
            else:
                print(f"Warning: pinned variant '{pinned}' not found for {primary_key}, falling back to BEST")
                result_data.extend(
                    select_best_variant(group_rows, primary_groupby_col, variant_col, y_col, x_col)
                )
        else:
            result_data.extend(
                select_best_variant(group_rows, primary_groupby_col, variant_col, y_col, x_col)
            )

    return result_data


def apply_filter(data: List[Dict], filter_conditions: Optional[List[Tuple[str, str]]]) -> List[Dict]:
    """
    Apply multiple filter conditions to the data. All conditions must be satisfied (AND logic).
    
    Supports both exact matching and substring matching:
    - Exact match: key=value
    - Substring match: key=*substring* (checks if substring is contained in the field value)
    
    Args:
        data (List[Dict]): Input data
        filter_conditions (List[Tuple[str, str]] or None): List of (key, value) filter conditions
        
    Returns:
        List[Dict]: Filtered data
    """
    if filter_conditions is None:
        return data
    
    filtered_data = []
    
    for row in data:
        # Check if all filter conditions are satisfied
        all_conditions_met = True
        for key, value in filter_conditions:
            if key not in row:
                all_conditions_met = False
                break
            
            row_value = str(row[key])

            # Check for comparison operators (>=, <=, >, <)
            cmp_match = re.match(r'^(>=|<=|>|<)(.+)$', value)
            if cmp_match:
                op, threshold = cmp_match.group(1), cmp_match.group(2)
                try:
                    lhs = float(row[key])
                    rhs = float(threshold)
                    result = (op == '>=' and lhs >= rhs) or (op == '<=' and lhs <= rhs) or \
                             (op == '>'  and lhs >  rhs) or (op == '<'  and lhs <  rhs)
                    if not result:
                        all_conditions_met = False
                        break
                except (ValueError, TypeError):
                    all_conditions_met = False
                    break
                continue

            # Check if this is a substring filter (starts and ends with *)
            if value.startswith('*') and value.endswith('*') and len(value) > 2:
                # Substring matching - remove the * characters and check if substring is in the value
                substring = value[1:-1]  # Remove leading and trailing *
                if substring not in row_value:
                    all_conditions_met = False
                    break
            else:
                # Exact matching (original behavior)
                if row_value != value:
                    all_conditions_met = False
                    break
        
        if all_conditions_met:
            filtered_data.append(row)
    
    return filtered_data


def group_and_aggregate(data: List[Dict], groupby_col: str, agg_func: str, agg_col: str, x_col: str) -> List[Dict]:
    """
    Group data by a column and apply aggregation.
    
    Args:
        data (List[Dict]): Input data
        groupby_col (str): Column to group by (may include BEST aggregation)
        agg_func (str): Aggregation function (MEDIAN, AVG, etc.)
        agg_col (str): Column to aggregate
        x_col (str): X-axis column name
        
    Returns:
        List[Dict]: Grouped and aggregated data
    """
    # Parse groupby clause to check for BEST aggregation
    primary_groupby, best_aggregation = parse_groupby_clause(groupby_col)
    
    # Group data by primary groupby column and x_col
    groups = {}
    for row in data:
        if primary_groupby in row and agg_col in row and x_col in row:
            group_key = (row[primary_groupby], row[x_col])
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
    
    # Aggregate each group
    aggregated_data = []
    for (group_value, x_value), group_rows in groups.items():
        # Filter out invalid values
        valid_values = [row[agg_col] for row in group_rows if agg_col in row and row[agg_col] is not None]
        
        if not valid_values:
            continue
        
        # Apply aggregation function
        if agg_func == 'MEDIAN':
            agg_result = statistics.median(valid_values)
        elif agg_func in ['AVG', 'AVERAGE']:
            agg_result = statistics.mean(valid_values)
        elif agg_func == 'MIN':
            agg_result = min(valid_values)
        elif agg_func == 'MAX':
            agg_result = max(valid_values)
        elif agg_func == 'SUM':
            agg_result = sum(valid_values)
        else:  # NONE - just take the first value
            agg_result = valid_values[0]

        workload_type = group_rows[0].get('workload_type', '') if group_rows else ''
        if workload_type == 'INSERT_DELETE':
            agg_result = agg_result / x_value if x_value else agg_result
        else:
            agg_result = agg_result / 10000

        # Create result row
        result_row = {
            primary_groupby: group_value,
            x_col: x_value,
            f'{agg_func.lower()}_{agg_col}': agg_result,
            'group_size': len(valid_values)
        }
        
        # Add other fields from first row for reference
        if group_rows:
            for key, value in group_rows[0].items():
                if key not in result_row:
                    result_row[key] = value
        
        aggregated_data.append(result_row)
    
    # Apply BEST / PIN selection if specified
    if best_aggregation:
        agg_type = best_aggregation[0].upper()
        y_col_name = f'{agg_func.lower()}_{agg_col}' if agg_func != 'NONE' else agg_col

        if agg_type == 'BEST':
            best_col = best_aggregation[1]
            aggregated_data = select_best_variant(aggregated_data, primary_groupby, best_col, y_col_name, x_col)
        elif agg_type == 'PIN':
            variant_col, pin_map = best_aggregation[1], best_aggregation[2]
            aggregated_data = select_pinned_variant(aggregated_data, primary_groupby, variant_col, y_col_name, x_col, pin_map)
    
    return aggregated_data


def parse_benchmark_log(log_file_path: str) -> List[Dict]:
    """
    Parse benchmark log file in SQL-style format and return a list of dictionaries.
    
    Args:
        log_file_path (str): Path to the benchmark log file
        
    Returns:
        List[Dict]: Parsed benchmark data
    """
    print(f"Parsing benchmark log: {log_file_path}")
    
    data_rows = []
    
    with open(log_file_path, 'r') as file:
        for line_no, line in enumerate(file, 1):
            line = line.strip()
            
            # Skip empty lines and non-result lines
            if not line or not line.startswith('RESULT'):
                continue
            
            try:
                # Remove 'RESULT ' prefix
                data_part = line[7:].strip()
                
                # Parse key=value pairs
                row_data = {}
                # Split by spaces, but be careful with float values
                pairs = re.findall(r'(\w+)=([^\s]+)', data_part)
                
                for key, value in pairs:
                    # Try to convert to appropriate type
                    try:
                        # Try integer first
                        if '.' not in value:
                            row_data[key] = int(value)
                        else:
                            # Try float
                            row_data[key] = float(value)
                    except ValueError:
                        # Keep as string if conversion fails
                        row_data[key] = value
                
                data_rows.append(row_data)
                
            except Exception as e:
                print(f"Warning: Could not parse line {line_no}: {line}")
                print(f"Error: {e}")
                continue
    
    if not data_rows:
        raise ValueError(f"No valid benchmark data found in {log_file_path}")
    
    print(f"Successfully parsed {len(data_rows)} benchmark records")
    
    # Get unique column names
    all_columns = set()
    for row in data_rows:
        all_columns.update(row.keys())
    print(f"Columns found: {', '.join(sorted(all_columns))}")
    
    return data_rows

def generate_pgfplot_data(
    data: List[Dict],
    x_col: str,
    y_col: str,
    groupby_col: str = None,
    y_soft_max: Optional[float] = None,
    y_soft_max_ratio: float = 1.25,
) -> str:
    """
    Generate pgfplot data section for a given x and y column combination.
    
    Args:
        data (List[Dict]): Benchmark data (already filtered and aggregated)
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis  
        groupby_col (str): Column used for grouping (for legend, may include BEST aggregation)
        
        y_soft_max (float or None): Optional axis ymax used for soft clipping.
        y_soft_max_ratio (float): Keep points up to y_soft_max * ratio.

    Returns:
        str: Formatted data for pgfplot
    """
    data_lines = []
    max_legend_entry = 16
    
    # Group by the groupby column (usually index_name) to create separate plot lines
    if groupby_col:
        # Parse groupby clause to get the primary column name
        primary_groupby, best_aggregation = parse_groupby_clause(groupby_col)
        
        groups = {}
        for row in data:
            if primary_groupby in row:
                group_value = row[primary_groupby]
                if group_value not in groups:
                    groups[group_value] = []
                groups[group_value].append(row)
        
        # Sort each group data by x_col and create plot lines
        for group_value, group_data in sorted(groups.items()):
            # Filter out rows that don't have the required columns or have invalid values
            valid_data = [
                row for row in group_data 
                if x_col in row and y_col in row 
                and is_valid_plot_value(row[x_col]) and is_valid_plot_value(row[y_col])
            ]
            if y_soft_max is not None and y_soft_max > 0:
                # Clip values that would trigger TeX "Dimension too large" to the
                # safe cap instead of dropping them, so the line visibly shoots off
                # the top of the plot rather than connecting through a gap.
                soft_cap = y_soft_max * y_soft_max_ratio
                valid_data = [
                    {**row, y_col: soft_cap} if float(row[y_col]) > soft_cap else row
                    for row in valid_data
                ]
            if not valid_data:
                continue
                
            # Sort by x column
            valid_data.sort(key=lambda row: row[x_col])
            
            # Determine legend entry and marker key — include variant when BEST or PIN selects one.
            # For PIN, use the pinned string (not the full variant value) so that all series
            # matched by the same pin string share the same legend label and marker.
            if best_aggregation and best_aggregation[0].upper() == 'PIN':
                pin_map_ba = best_aggregation[2]
                pinned_str = pin_map_ba.get(str(group_value), '')
                if pinned_str:
                    legend_entry = f"{group_value} ({pinned_str})"
                    marker_key = pinned_str
                else:
                    legend_entry = group_value
                    marker_key = str(group_value)
            elif best_aggregation and best_aggregation[0].upper() == 'BEST':
                variant_col = best_aggregation[1]
                if valid_data and variant_col in valid_data[0]:
                    _mv = valid_data[0][variant_col]
                    if _mv and str(_mv).lower() != 'none':
                        legend_entry = f"{group_value} ({_mv})"
                        marker_key = str(_mv)
                    else:
                        legend_entry = group_value
                        marker_key = str(group_value)
                else:
                    legend_entry = group_value
                    marker_key = str(group_value)
            else:
                legend_entry = group_value
                marker_key = str(group_value)

            # Get consistent color for the series based on index_name only
            series_color = get_series_color(str(group_value))
            series_marker = get_series_marker(marker_key)

            data_lines.append(f"% Data for {legend_entry}")
            data_lines.append(f"\\addplot[color={series_color}, mark={series_marker}, mark size=1.1pt] coordinates {{")

            for row in valid_data:
                x_val = row[x_col]
                y_val = row[y_col]
                data_lines.append(f"    ({x_val}, {y_val})")
            
            data_lines.append("};")
            if max_legend_entry > 0:
                safe_legend_entry = escape_legend_label(legend_entry)
                data_lines.append(f"\\addlegendentry{{{safe_legend_entry}}}")
                max_legend_entry -= 1
            data_lines.append("")
    
    else:
        # No grouping, plot all data as a single series
        valid_data = [
            row for row in data 
            if x_col in row and y_col in row 
            and is_valid_plot_value(row[x_col]) and is_valid_plot_value(row[y_col])
        ]
        if y_soft_max is not None and y_soft_max > 0:
            soft_cap = y_soft_max * y_soft_max_ratio
            valid_data = [
                {**row, y_col: soft_cap} if float(row[y_col]) > soft_cap else row
                for row in valid_data
            ]
        if valid_data:
            valid_data.sort(key=lambda row: row[x_col])
            
            data_lines.append(f"\\addplot coordinates {{")
            for row in valid_data:
                x_val = row[x_col]
                y_val = row[y_col]
                data_lines.append(f"    ({x_val}, {y_val})")
            data_lines.append("};")
    
    return "\n".join(data_lines)


def substitute_caption_parameters(caption: str, filter_conditions: Optional[List[Tuple[str, str]]], 
                                 data: List[Dict], x_col: str, y_col: str, groupby_col: str) -> str:
    """
    Substitute parameters in caption string with actual values from filter conditions and data.
    
    Args:
        caption (str): Caption string with potential parameters like {dataset_name}
        filter_conditions (List[Tuple[str, str]] or None): Filter conditions
        data (List[Dict]): Benchmark data
        x_col (str): X-axis column name
        y_col (str): Y-axis column name  
        groupby_col (str): Group by column name
        
    Returns:
        str: Caption with parameters substituted
    """
    if not caption:
        return caption
    
    # Create a dictionary of available parameters
    parameters = {}
    
    # Add filter condition values as parameters
    if filter_conditions:
        for key, value in filter_conditions:
            parameters[key] = value
    
    # Add column names as parameters
    parameters['x_col'] = x_col
    parameters['y_col'] = y_col
    parameters['groupby_col'] = groupby_col
    
    # Add some derived parameters
    if 'dataset_name' in parameters:
        # Clean up dataset name for display (replace underscores with spaces)
        parameters['dataset_display_name'] = parameters['dataset_name'].replace('_', ' ').title()
    else:
        # Fallback for templates that use dataset placeholders without an explicit dataset filter.
        dataset_names = sorted({row.get('dataset_name') for row in data if row.get('dataset_name')})
        if len(dataset_names) == 1:
            parameters['dataset_name'] = dataset_names[0]
            parameters['dataset_display_name'] = dataset_names[0].replace('_', ' ').title()
    
    # Add workload type display name
    if 'workload_type' in parameters:
        workload_display_map = {
            'LOOKUP_IN_DISTRIBUTION': 'In-Distr. Lookup',
            'LOOKUP_EXISTING': 'Existing Lookup', 
            'INSERT_IN_DISTRIBUTION': 'In-Distr. Insert'
        }
        parameters['workload_display_name'] = workload_display_map.get(
            parameters['workload_type'], parameters['workload_type']
        )
    
    # Replace only identifier-like placeholders, leaving LaTeX braces untouched.
    placeholder_pattern = re.compile(r'\{([A-Za-z_][A-Za-z0-9_]*)\}')

    def replace_placeholder(match: re.Match) -> str:
        key = match.group(1)
        if key in parameters:
            return str(parameters[key])
        # Unresolved placeholders should not leak into final LaTeX.
        return ''

    return placeholder_pattern.sub(replace_placeholder, caption)


def extract_axis_from_figure(figure_latex: str) -> str:
    """
    Extract the axis code from a complete figure LaTeX.
    
    Args:
        figure_latex (str): Complete figure LaTeX code
        
    Returns:
        str: Just the axis and plot data portions
    """
    # Match the axis environment
    axis_pattern = r'\\begin\{axis\}.*?\\end\{axis\}'
    match = re.search(axis_pattern, figure_latex, re.DOTALL)
    if match:
        return match.group(0)
    return figure_latex


def compact_axis_for_subfigure(axis_code: str) -> str:
    """Make axis compact for dense multiplot layouts and remove local legend placement."""
    compact_axis = axis_code

    # Remove explicit legend placement from each subplot; a shared legend is added globally.
    compact_axis = re.sub(r'\s*legend\s+pos\s*=\s*[^,]+,\s*\n', '\n', compact_axis)

    # Force compact dimensions suitable for dense multiplot layouts.
    # Replace only dedicated axis option lines (not "line width" nor plot lines).
    compact_axis = re.sub(
        r'(^\s*)width\s*=\s*[^,\n]+,\s*$',
        r'\1width=0.82\\linewidth,',
        compact_axis,
        flags=re.MULTILINE,
    )
    compact_axis = re.sub(
        r'(^\s*)height\s*=\s*[^,\n]+,\s*$',
        r'\1height=0.70\\linewidth,',
        compact_axis,
        flags=re.MULTILINE,
    )

    # Keep labels readable while reducing footprint.
    compact_axis = compact_axis.replace(
        '\\begin{axis}[',
        '\\begin{axis}[\n    tick label style={font=\\normalsize},\n    label style={font=\\normalsize},\n    title style={font=\\normalsize},',
        1
    )

    return compact_axis


def remove_y_label_from_axis(axis_code: str) -> str:
    """Remove ylabel from an axis block (used for non-left subfigures)."""
    return re.sub(r'^\s*ylabel\s*=\s*\{[^}]*\},\s*$\n?', '', axis_code, flags=re.MULTILINE)


def extract_series_names_and_strip_legend(axis_code: str) -> Tuple[str, List[str]]:
    """Remove per-axis legend entries and return series names in stable order."""
    series_names = []
    for match in re.finditer(r'^% Data for (.+)$', axis_code, flags=re.MULTILINE):
        series_name = match.group(1).strip()
        if series_name and series_name not in series_names:
            series_names.append(series_name)

    axis_without_legend = re.sub(r'\n\\addlegendentry\{[^}]*\}\s*', '\n', axis_code)
    return axis_without_legend, series_names


def build_shared_legend(series_names: List[str]) -> str:
    """Build one shared legend block for all subfigures in a multiplot."""
    if not series_names:
        return ''

    max_legend_entries = 30
    legend_series_names = series_names[:max_legend_entries]
    omitted_count = len(series_names) - len(legend_series_names)

    legend_columns = min(3, len(legend_series_names))
    legend_lines = [
        "\\vspace{0.3em}",
        "\\begin{center}",
        "\\begin{tikzpicture}",
        f"\\begin{{axis}}[hide axis, xmin=0, xmax=1, ymin=0, ymax=1, legend columns={legend_columns}, legend style={{draw=none, font=\\scriptsize}}]",
    ]

    for series_name in legend_series_names:
        color_key = resolve_series_color_key(series_name)
        color = get_series_color(color_key)
        marker_key = resolve_series_marker_key(series_name)
        marker = get_series_marker(marker_key)
        legend_lines.append(f"\\addlegendimage{{color={color}, mark={marker}, mark size=1.1pt}}")
        legend_lines.append(f"\\addlegendentry{{{escape_legend_label(series_name)}}}")

    legend_lines.extend([
        "\\end{axis}",
        "\\end{tikzpicture}",
        "\\end{center}",
    ])

    if omitted_count > 0:
        legend_lines.append(f"\\begin{{center}}\\footnotesize ({omitted_count} additional legend entries omitted)\\end{{center}}")

    return "\n".join(legend_lines)


def resolve_series_color_key(series_name: str) -> str:
    """Map legend labels to the same color key used by plotted series."""
    name = str(series_name).strip()

    # If this exact name already has a color assignment, keep it.
    if name in _SERIES_COLOR_MAP:
        return name

    # BEST(...) labels append a variant as "<index_name> (<variant>)".
    # If stripping the last parenthesized suffix matches a known series, use it.
    match = re.match(r'^(.*)\s\([^()]*\)$', name)
    if match:
        base_name = match.group(1).strip()
        if base_name in _SERIES_COLOR_MAP:
            return base_name

    return name


def resolve_series_marker_key(series_name: str) -> str:
    """Map legend labels to the same marker key used by plotted series.

    For BEST(...) labels of the form "<index_name> (<variant>)", extract the
    variant part so the marker matches what was assigned in generate_pgfplot_data.
    For plain labels (e.g., a bare index_variant name), use the label directly.
    """
    name = str(series_name).strip()

    # If the exact label already has a marker assignment, use it directly.
    if name in _SERIES_MARKER_MAP:
        return name

    # Try to extract the variant from "<index_name> (<variant>)" form.
    match = re.match(r'^.*\s\(([^()]*)\)$', name)
    if match:
        variant = match.group(1).strip()
        if variant in _SERIES_MARKER_MAP:
            return variant

    return name


def get_workload_display_name(filter_conditions: Optional[List[Tuple[str, str]]]) -> str:
    """Return a human-readable workload name from filter conditions."""
    if not filter_conditions:
        return ""

    filter_dict = {k: v for k, v in filter_conditions}
    workload_type = filter_dict.get('workload_type', '')
    workload_display_map = {
        'LOOKUP_IN_DISTRIBUTION': 'In-Distr. Lookup',
        'LOOKUP_EXISTING': 'Existing Lookup',
        'INSERT_IN_DISTRIBUTION': 'In-Distr. Insert',
        'DELETE_EXISTING': 'Delete Existing',
        'MIXED': 'Mixed',
    }
    return workload_display_map.get(workload_type, workload_type.replace('_', ' ').title())


def create_figure_from_template(data: List[Dict], x_col: str, y_col: str, 
                              filter_conditions: Optional[List[Tuple[str, str]]], groupby_col: str,
                              title: str, caption: str, label: str,
                              figure_template_path: str = None) -> str:
    """
    Create a single figure using a template with filtering and grouping.
    
    Args:
        data (List[Dict]): Raw benchmark data
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis (may include aggregation)
        filter_conditions (List[Tuple[str, str]] or None): List of filter conditions
        groupby_col (str): Column to group by
        title (str): Plot title
        caption (str): Figure caption
        label (str): Figure label
        figure_template_path (str): Path to figure template (optional)
        
    Returns:
        str: LaTeX figure code
    """
    # Use default figure template if none provided
    if figure_template_path is None:
        figure_template_path = os.path.join(os.path.dirname(__file__), 'figure_template.tex')
    
    # Read figure template
    with open(figure_template_path, 'r') as f:
        template_content = f.read()
    
    # Parse aggregation from y_col
    agg_func, actual_y_col = parse_aggregation(y_col)
    
    # Apply filter
    filtered_data = apply_filter(data, filter_conditions)
    
    if not filtered_data:
        return f"% No data available for filter {filter_conditions}"
    
    # Parse groupby clause
    primary_groupby_col, best_aggregation = parse_groupby_clause(groupby_col)
    
    # If BEST or PIN aggregation is specified, narrow to selected variant before aggregation
    if best_aggregation:
        agg_type = best_aggregation[0].upper()
        if agg_type == 'BEST':
            best_col = best_aggregation[1]
            filtered_data = select_best_variant(filtered_data, primary_groupby_col, best_col, actual_y_col, x_col)
        elif agg_type == 'PIN':
            variant_col, pin_map = best_aggregation[1], best_aggregation[2]
            filtered_data = select_pinned_variant(filtered_data, primary_groupby_col, variant_col, actual_y_col, x_col, pin_map)
    
    # Group and aggregate data
    processed_data = group_and_aggregate(filtered_data, primary_groupby_col, agg_func, actual_y_col, x_col)
    
    if not processed_data:
        return f"% No aggregated data available for {title}"
    
    # Determine the actual y column name after aggregation
    actual_y_col_name = f'{agg_func.lower()}_{actual_y_col}' if agg_func != 'NONE' else actual_y_col

    y_values = [
        row[actual_y_col_name]
        for row in processed_data
        if actual_y_col_name in row and is_valid_plot_value(row[actual_y_col_name])
    ]
    y_max = compute_robust_ymax(y_values)
    
    # Generate plot data (pass original groupby_col to preserve BEST aggregation info)
    plot_data = generate_pgfplot_data(
        processed_data,
        x_col,
        actual_y_col_name,
        groupby_col,
        y_soft_max=y_max,
    )
    
    # Prepare better labels
    x_label = x_col.replace('_', ' ').title()
    y_label = actual_y_col.replace('_', ' ').title()

    # Special handling for certain column names
    if 'total_time' in actual_y_col:
        y_label = 'Batch time (s)'
    elif 'throughput' in actual_y_col:
        y_label = 'Throughput (ops/sec)'
    
    # Substitute parameters in caption and title
    caption = substitute_caption_parameters(caption, filter_conditions, data, x_col, y_col, groupby_col)
    title = substitute_caption_parameters(title, filter_conditions, data, x_col, y_col, groupby_col)
    
    # Substitute parameters in caption
    caption = substitute_caption_parameters(caption, filter_conditions, data, x_col, y_col, groupby_col)
    
    # Detect log-scale workloads
    filter_dict = {k: v for k, v in filter_conditions} if filter_conditions else {}
    y_scale_options = 'ymode=log,\n    ' if filter_dict.get('workload_type') == 'INSERT_DELETE' else ''

    # Apply replacements
    replacements = {
        '{{PLOT_DATA}}': plot_data,
        '{{X_LABEL}}': x_label,
        '{{Y_LABEL}}': y_label,
        '{{Y_MAX}}': f"{y_max:.10g}",
        '{{Y_SCALE_OPTIONS}}': y_scale_options,
        '{{TITLE}}': title,
        '{{CAPTION}}': caption,
        '{{LABEL}}': label
    }
    
    result_content = template_content
    for placeholder, replacement in replacements.items():
        result_content = result_content.replace(placeholder, replacement)
    
    return result_content


def create_multiplot_from_filters(data: List[Dict], x_col: str, y_col: str,
                                 filter_string: str, groupby_col: str,
                                 title: str, caption: str, label: str,
                                 figure_template_path: str = None,
                                 cols_per_row: int = 4,
                                 row_titles: List[str] = None) -> str:
    """
    Create a multiplot figure with multiple subfigures, one for each filter.
    
    Args:
        data (List[Dict]): Raw benchmark data
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis (may include aggregation)
        filter_string (str): Filters separated by | (e.g., 'filter1|filter2|filter3')
        groupby_col (str): Column to group by
        title (str): Main plot title
        caption (str): Figure caption
        label (str): Figure label
        figure_template_path (str): Path to figure template (optional)
        cols_per_row (int): Number of subfigures per row (default: 2)
        
    Returns:
        str: LaTeX multiplot figure code with subfigures
    """
    # Parse individual filters separated by |
    filters = [f.strip() for f in filter_string.split('|')]
    
    if not filters:
        return f"% No filters provided for multiplot {title}"
    
    # Generate axis code for each filter
    subfigures = []
    shared_series_names = []
    for filter_str in filters:
        filter_conditions = parse_filter(filter_str)
        
        # Create figure for this filter (it will include the outer figure environment)
        figure_content = create_figure_from_template(
            data, x_col, y_col, filter_conditions, groupby_col,
            "", "", "",  # Empty labels for subfigures
            figure_template_path
        )
        
        # Extract just the axis environment
        axis_code = extract_axis_from_figure(figure_content)
        axis_code, axis_series = extract_series_names_and_strip_legend(axis_code)
        axis_code = compact_axis_for_subfigure(axis_code)
        workload_caption = get_workload_display_name(filter_conditions)
        for series_name in axis_series:
            if series_name not in shared_series_names:
                shared_series_names.append(series_name)
        
        if axis_code and "% No data" not in axis_code:
            subfigures.append((axis_code, workload_caption))
    
    if not subfigures:
        return f"% No valid data for multiplot {title}"
    
    # Calculate subfigure width based on columns per row with layout margin.
    # Reserve ~2% per inter-column gap; the y-label on the leftmost plot eats a
    # little extra space, so we keep a small asymmetric nudge for that column.
    gap_total = 0.02 * (cols_per_row - 1)
    subfig_width = f"{(0.95 - gap_total) / cols_per_row:.3f}\\textwidth"
    
    # Build multiplot LaTeX
    latex_lines = [
        "\\begin{figure}[H]",
        "\\centering",
    ]
    
    total_subfigures = len(subfigures)
    last_row_count = total_subfigures % cols_per_row
    if last_row_count == 0:
        last_row_count = cols_per_row

    for i, (axis_code, workload_caption) in enumerate(subfigures):
        # Add newline before starting new row (except first row)
        if i > 0 and i % cols_per_row == 0:
            latex_lines.append("")

        # Insert row title at the start of each row, if provided
        if i % cols_per_row == 0 and row_titles:
            row_idx = i // cols_per_row
            if row_idx < len(row_titles) and row_titles[row_idx]:
                rule = "" if row_idx == 0 else "{\\color{lightgray}\\rule{\\linewidth}{0.2pt}}\\\\[-0.5ex]"
                latex_lines.append(f"\\par\\noindent{rule}\\textbf{{{escape_latex_text(row_titles[row_idx])}}}\\par\\smallskip")

        # Keep y-axis label only on the leftmost subplot of each row.
        axis_code_for_slot = axis_code
        if i % cols_per_row != 0:
            axis_code_for_slot = remove_y_label_from_axis(axis_code_for_slot)
        
        latex_lines.append(f"\\begin{{subfigure}}{{{subfig_width}}}")
        latex_lines.append("\\centering")
        latex_lines.append("\\begin{tikzpicture}")
        latex_lines.append(axis_code_for_slot)
        latex_lines.append("\\end{tikzpicture}")
        if workload_caption:
            latex_lines.append("\\captionsetup{justification=centering}")
            latex_lines.append(f"\\caption{{{escape_latex_text(workload_caption)}}}")
        latex_lines.append("\\end{subfigure}")

        is_end_of_row = (i + 1) % cols_per_row == 0
        is_last = i == len(subfigures) - 1
        position_in_row = i % cols_per_row
        in_last_row = i >= (total_subfigures - last_row_count)
        position_in_last_row = i - (total_subfigures - last_row_count)
        if not is_last:
            if is_end_of_row:
                latex_lines.append("\\par\\medskip")
            elif in_last_row and last_row_count < cols_per_row:
                # Fixed gap in the incomplete last row → subfigures left-aligned.
                latex_lines.append("\\hspace{0.02\\textwidth}")
            elif position_in_row == 0:
                # Extra room after leftmost plot to compensate for the y-label width.
                latex_lines.append("\\hspace{0.02\\textwidth}")
            # else:
            #     latex_lines.append("\\hfill")

    # Add one shared legend for all subfigures.
    shared_legend = build_shared_legend(shared_series_names)
    if shared_legend:
        latex_lines.append("")
        latex_lines.append(shared_legend)
    
    # Use the first filter as representative context for caption placeholders
    # such as {dataset_display_name} in multiplot-level captions.
    caption_filter_conditions = parse_filter(filters[0]) if filters else None

    latex_lines.append(f"\\label{{fig:{label}}}")
    latex_lines.append("\\end{figure}")
    
    return "\n".join(latex_lines)


def split_directive_fields(body: str) -> List[str]:
    """
    Split a directive body by commas, ignoring commas inside parentheses.
    This lets PIN(col:A=v1,B=v2) live in a field without being broken apart
    by the top-level field separator.
    """
    fields: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in body:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            fields.append(''.join(current))
            current = []
        else:
            current.append(ch)
    if current:
        fields.append(''.join(current))
    return fields


def join_multiline_directives(content: str) -> str:
    """
    Join PLOT/MULTIPLOT directives that have been split across multiple lines.

    A directive line that starts with '%% {{MULTIPLOT:' or '%% {{PLOT:' but does
    not contain '}}' is continued by subsequent lines that begin with '%%' followed
    by whitespace.  The '%%' prefix (and any leading whitespace after it) is stripped
    from each continuation line before appending, so the caller can break the directive
    at any point and indent freely:

        %% {{MULTIPLOT:x,y,
        %%   filter1|filter2,
        %%   groupby,Title,Caption,Label}}
    """
    lines = content.split('\n')
    result: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r'^%% \{\{(MULTIPLOT|PLOT):', line) and '}}' not in line:
            combined = line
            i += 1
            while i < len(lines):
                cont = re.match(r'^%%\s+(.*)', lines[i])
                if cont:
                    combined += cont.group(1)
                    i += 1
                    if '}}' in combined:
                        break
                else:
                    break
            result.append(combined)
        else:
            result.append(line)
            i += 1
    return '\n'.join(result)


def split_template_documents(template_content: str):
    """
    If the template contains multiple \\begin{document}...\\end{document} blocks,
    returns a list of (output_name, full_content) tuples where the shared preamble
    (everything before the first \\begin{document}) is prepended to each block.
    Returns None when there is only one document block (single-file mode).
    """
    begin_positions = [m.start() for m in re.finditer(r'\\begin\{document\}', template_content)]
    if len(begin_positions) <= 1:
        return None

    preamble = template_content[:begin_positions[0]].rstrip('\n')

    documents = []
    for i, begin_pos in enumerate(begin_positions):
        end_match = re.search(r'\\end\{document\}', template_content[begin_pos:])
        if not end_match:
            continue
        end_pos = begin_pos + end_match.end()
        body = template_content[begin_pos:end_pos]

        if i == 0:
            preceding = preamble
        else:
            prev_end_re = re.search(r'\\end\{document\}', template_content[begin_positions[i - 1]:])
            prev_end = begin_positions[i - 1] + prev_end_re.end()
            preceding = template_content[prev_end:begin_pos]

        output_match = re.search(r'^%% OUTPUT:\s*(\S+)', preceding, re.MULTILINE)
        name = output_match.group(1) if output_match else f"figure_{i + 1:02d}"

        documents.append((name, preamble + '\n\n' + body))

    return documents


def _process_template_content(data: List[Dict], template_content: str,
                               figure_template_path: str = None) -> str:
    """Process PLOT/MULTIPLOT directives in template_content and return the result."""
    template_content = join_multiline_directives(template_content)

    # Detect directive lines and split fields with a parenthesis-aware splitter so that
    # commas inside PIN(...) or similar modifiers are not mistaken for field separators.
    # Each entry is (full_line, x_col, y_col, filter_str, groupby_col, title, caption, label).
    multiplot_matches = []
    plot_matches = []
    directive_detect = re.compile(r'^%% \{\{(MULTIPLOT|PLOT):(.+)\}\}$', re.MULTILINE)
    for m in directive_detect.finditer(template_content):
        kind = m.group(1)
        fields = split_directive_fields(m.group(2))
        if len(fields) in (7, 8, 9) and kind == 'MULTIPLOT' or len(fields) == 7 and kind == 'PLOT':
            # Keep the original matched line as placeholder; strip only for processing.
            parsed = (m.group(0),) + tuple(f.strip() for f in fields)
            if kind == 'MULTIPLOT':
                multiplot_matches.append(parsed)
            else:
                plot_matches.append(parsed)
    
    # Validate that at least one plot directive exists
    if not plot_matches and not multiplot_matches:
        # Debug: show the first few lines of joined content to diagnose
        preview = '\n'.join(template_content.splitlines()[:30])
        print(f"DEBUG joined content (first 30 lines):\n{preview}\n", file=sys.stderr)
        raise ValueError("No plots defined in template content. "
                         "Template must contain at least one %% {{PLOT:...}} or %% {{MULTIPLOT:...}} directive.")
    
    # Initialize color mapping for consistent colors across all plots
    try:
        initialize_color_mapping(data, 'index_name')
    except Exception as e:
        print(f"Warning: Could not initialize color mapping: {e}")
    
    result_content = template_content
    
    # Process MULTIPLOT directives
    for match in multiplot_matches:
        row_titles = []
        if len(match) == 10:
            placeholder, x_col, y_col, filter_str, groupby_col, title, caption, label, cols_per_row_str, row_titles_str = match
            cols_per_row = int(cols_per_row_str) if cols_per_row_str else 4
            row_titles = [t.strip() for t in row_titles_str.split('|')]
        elif len(match) == 9:
            placeholder, x_col, y_col, filter_str, groupby_col, title, caption, label, cols_per_row_str = match
            cols_per_row = int(cols_per_row_str) if cols_per_row_str else 4
        else:
            placeholder, x_col, y_col, filter_str, groupby_col, title, caption, label = match
            cols_per_row = 4
        
        # Parse aggregation from y_col
        agg_func, actual_y_col = parse_aggregation(y_col)
        
        # Parse groupby clause to get primary groupby column
        primary_groupby, best_aggregation = parse_groupby_clause(groupby_col)
        
        # Validate that required columns exist
        has_required_cols = any(
            x_col in row and actual_y_col in row and primary_groupby in row 
            for row in data
        )
        
        if best_aggregation:
            best_col = best_aggregation[1]
            has_required_cols = has_required_cols and any(best_col in row for row in data)
        
        # Check if any filter in the multiplot can be applied
        filter_list = [f.strip() for f in filter_str.split('|')]
        filters_valid = True
        for single_filter in filter_list:
            filter_conditions = parse_filter(single_filter)
            if filter_conditions:
                for filter_key, _ in filter_conditions:
                    if not any(filter_key in row for row in data):
                        filters_valid = False
                        break
        
        if has_required_cols and filters_valid:
            figure = create_multiplot_from_filters(
                data, x_col, y_col, filter_str, groupby_col,
                title, caption, label, figure_template_path,
                cols_per_row=cols_per_row,
                row_titles=row_titles,
            )
            result_content = result_content.replace(placeholder, figure)
        else:
            missing_info = []
            if not has_required_cols:
                missing_info.append(f"missing columns")
            if not filters_valid:
                missing_info.append(f"filters not applicable")
            result_content = result_content.replace(placeholder,
                f"% Skipped multiplot {title} - {', '.join(missing_info)}")
            print(f"Warning: Skipping multiplot {title} - {', '.join(missing_info)}")
    
    # Process PLOT directives
    for match in plot_matches:
        placeholder, x_col, y_col, filter_str, groupby_col, title, caption, label = match
        
        # Parse filter condition
        filter_conditions = parse_filter(filter_str)
        
        # Parse aggregation from y_col
        agg_func, actual_y_col = parse_aggregation(y_col)
        
        # Parse groupby clause to get primary groupby column
        primary_groupby, best_aggregation = parse_groupby_clause(groupby_col)
        
        # Check if the required columns exist in the data
        has_required_cols = any(
            x_col in row and actual_y_col in row and primary_groupby in row 
            for row in data
        )
        
        # If BEST aggregation is specified, also check for the best column
        if best_aggregation:
            best_col = best_aggregation[1]
            has_required_cols = has_required_cols and any(best_col in row for row in data)
        
        # Check if filter can be applied
        filter_applicable = True
        if filter_conditions:
            # Check if all filter keys exist in at least one row
            for filter_key, filter_value in filter_conditions:
                if not any(filter_key in row for row in data):
                    filter_applicable = False
                    break
        
        if has_required_cols and filter_applicable:
            figure = create_figure_from_template(
                data, x_col, y_col, filter_conditions, groupby_col,
                title, caption, label, figure_template_path
            )
            result_content = result_content.replace(placeholder, figure)
        else:
            missing_info = []
            if not has_required_cols:
                missing_cols = [x_col, actual_y_col, primary_groupby]
                if best_aggregation:
                    missing_cols.append(best_aggregation[1])
                missing_info.append(f"missing columns {'/'.join(missing_cols)}")
            if not filter_applicable:
                missing_info.append(f"filter not applicable: {filter_str}")
            
            result_content = result_content.replace(placeholder, 
                f"% Skipped plot {title} - {', '.join(missing_info)}")
            print(f"Warning: Skipping plot {title} - {', '.join(missing_info)}")
    
    return result_content


def create_performance_plot(data: List[Dict], multiplot_template_path: str = None,
                            figure_template_path: str = None) -> str:
    if multiplot_template_path is None:
        multiplot_template_path = os.path.join(os.path.dirname(__file__), 'multiplot_template.tex')
    try:
        with open(multiplot_template_path, 'r') as f:
            template_content = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Multiplot template file not found: {multiplot_template_path}")
    except Exception as e:
        raise Exception(f"Error reading multiplot template file {multiplot_template_path}: {e}")
    return _process_template_content(data, template_content, figure_template_path)


def find_benchmark_files(directory_path: str) -> Dict[str, str]:
    """
    Find benchmark files in directory and return the most recent file for each dataset.
    
    Args:
        directory_path (str): Path to directory containing benchmark files
        
    Returns:
        Dict[str, str]: Mapping of dataset_name -> most_recent_file_path
    """
    # Pattern: benchmark_<dataset_name>_<timestamp>.txt
    pattern = os.path.join(directory_path, 'benchmark_*_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9].txt')
    
    # Find all matching files
    matching_files = glob.glob(pattern)
    
    if not matching_files:
        raise ValueError(f"No benchmark files found in {directory_path} matching pattern benchmark_*_YYYYMMDD_HHMMSS.txt")
    
    # Group files by dataset name
    dataset_files = {}
    
    for file_path in matching_files:
        filename = os.path.basename(file_path)
        
        # Extract dataset name and timestamp
        # Pattern: benchmark_<dataset_name>_<YYYYMMDD_HHMMSS>.txt
        match = re.match(r'^benchmark_(.+)_(\d{8}_\d{6})\.txt$', filename)
        
        if match:
            dataset_name = match.group(1)
            timestamp_str = match.group(2)
            
            # Parse timestamp for comparison
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            except ValueError:
                print(f"Warning: Could not parse timestamp from {filename}, skipping")
                continue
            
            # Keep track of most recent file for each dataset
            if dataset_name not in dataset_files or timestamp > dataset_files[dataset_name][1]:
                dataset_files[dataset_name] = (file_path, timestamp)
    
    # Extract just the file paths (most recent for each dataset)
    result = {dataset_name: file_info[0] for dataset_name, file_info in dataset_files.items()}
    
    print(f"Found {len(matching_files)} benchmark files, {len(result)} unique datasets")
    for dataset_name, file_path in result.items():
        print(f"  {dataset_name}: {os.path.basename(file_path)}")
    
    return result


def parse_multiple_benchmark_logs(dataset_files: Dict[str, str]) -> List[Dict]:
    """
    Parse multiple benchmark log files and combine the data with dataset names.
    
    Args:
        dataset_files (Dict[str, str]): Mapping of dataset_name -> file_path
        
    Returns:
        List[Dict]: Combined benchmark data with dataset_name field added
    """
    all_data = []
    
    for dataset_name, file_path in dataset_files.items():
        print(f"\nParsing {dataset_name}: {file_path}")
        
        try:
            # Parse individual file
            file_data = parse_benchmark_log(file_path)
            
            # Add dataset_name field to each row
            for row in file_data:
                row['dataset_name'] = dataset_name
            
            all_data.extend(file_data)
            print(f"Added {len(file_data)} records from {dataset_name}")
            
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
            continue
    
    return all_data


def is_valid_plot_value(value) -> bool:
    """
    Check if a value is valid for plotting (numerical and not containing "error").
    
    Args:
        value: The value to check
        
    Returns:
        bool: True if the value is valid for plotting, False otherwise
    """
    if value is None:
        return False
    
    # Convert to string to check for "error" keyword
    str_value = str(value).lower()
    if "error" in str_value:
        return False
    
    # Check if it's a valid number
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def _quantile(sorted_values: List[float], q: float) -> float:
    """Compute quantile with linear interpolation on sorted data."""
    if not sorted_values:
        raise ValueError("Cannot compute quantile of empty data")
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]

    n = len(sorted_values)
    pos = (n - 1) * q
    low_idx = int(math.floor(pos))
    high_idx = int(math.ceil(pos))

    if low_idx == high_idx:
        return sorted_values[low_idx]

    low_val = sorted_values[low_idx]
    high_val = sorted_values[high_idx]
    weight = pos - low_idx
    return low_val + (high_val - low_val) * weight


def compute_robust_ymax(y_values: List[float], padding_ratio: float = 0.1) -> float:
    """
    Compute a per-axis ymax that excludes upper outliers using Tukey's rule.
    """
    finite_values = []
    for value in y_values:
        try:
            num = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(num):
            finite_values.append(num)

    if not finite_values:
        return 1.0

    finite_values.sort()
    data_max = finite_values[-1]

    if len(finite_values) >= 4:
        q1 = _quantile(finite_values, 0.25)
        q3 = _quantile(finite_values, 0.75)
        iqr = q3 - q1
        outlier_cutoff = q3 + 1.5 * iqr if iqr > 0 else q3
        non_outliers = [v for v in finite_values if v <= outlier_cutoff]
        base_max = max(non_outliers) if non_outliers else data_max
    else:
        base_max = data_max

    if base_max > 0:
        return base_max * (1.0 + padding_ratio)
    if base_max < 0:
        return base_max * (1.0 - padding_ratio)
    return 1.0


def main():
    """Main function to parse arguments and execute the plotting workflow."""
    parser = argparse.ArgumentParser(description='Generate LaTeX plots from benchmark logs')
    parser.add_argument('input_path', help='Path to directory containing benchmark log files, or single benchmark log file')
    parser.add_argument('--multiplot-template', help='Path to multiplot template file')
    parser.add_argument('--figure-template', help='Path to figure template file (for individual figures)')
    parser.add_argument('--output', help='Output LaTeX file path (single-doc mode)',
                       default='benchmark_plot.tex')
    parser.add_argument('--output-dir', help='Output directory for multi-document mode',
                       default='build')

    args = parser.parse_args()
    
    try:
        # Check if input is a directory or file
        if os.path.isdir(args.input_path):
            # Directory mode: find and parse multiple benchmark files
            dataset_files = find_benchmark_files(args.input_path)
            if not dataset_files:
                raise ValueError(f"No benchmark files found in directory {args.input_path}")
            data = parse_multiple_benchmark_logs(dataset_files)
        elif os.path.isfile(args.input_path):
            # Single file mode: parse single benchmark log (backward compatibility)
            print(f"Single file mode: {args.input_path}")
            data = parse_benchmark_log(args.input_path)
            # For single file mode, try to extract dataset name from filename if possible
            filename = os.path.basename(args.input_path)
            match = re.match(r'^benchmark_(.+)_(\d{8}_\d{6})\.txt$', filename)
            if match:
                dataset_name = match.group(1)
                for row in data:
                    row['dataset_name'] = dataset_name
                print(f"Extracted dataset name: {dataset_name}")
        else:
            raise ValueError(f"Input path {args.input_path} is neither a file nor a directory")
        
        # Print summary statistics
        print("\n=== Benchmark Summary ===")
        indices = set(row.get('index_name', 'Unknown') for row in data)
        datasets = set(row.get('dataset_name', 'Unknown') for row in data)
        print(f"Datasets: {', '.join(sorted(datasets))}")
        print(f"Indices tested: {', '.join(sorted(indices))}")
        print(f"Total records: {len(data)}")
        
        # Get all unique columns
        all_columns = set()
        for row in data:
            all_columns.update(row.keys())
        print(f"Columns available: {', '.join(sorted(all_columns))}")
        
        # Check whether the template has multiple \begin{document} blocks
        multi_docs = None
        if args.multiplot_template:
            try:
                with open(args.multiplot_template, 'r') as f:
                    raw_template = f.read()
                multi_docs = split_template_documents(raw_template)
                print(f"DEBUG: split_template_documents found {len(multi_docs) if multi_docs else 0} documents", file=sys.stderr)
            except Exception as e:
                print(f"DEBUG: split_template_documents failed: {e}", file=sys.stderr)

        print(f"\n=== Output ===")
        if multi_docs:
            # Multi-document mode: write one .tex per block into output_dir
            output_dir = args.output_dir
            os.makedirs(output_dir, exist_ok=True)
            for name, content in multi_docs:
                processed = _process_template_content(data, content, args.figure_template)
                out_path = os.path.join(output_dir, name + '.tex')
                with open(out_path, 'w') as f:
                    f.write(processed)
                print(f"LaTeX file generated: {out_path}")
            print(f"To compile: run build.sh")
        else:
            # Single-document mode (original behaviour)
            latex_content = create_performance_plot(
                data, args.multiplot_template, args.figure_template
            )
            output_path = args.output
            with open(output_path, 'w') as f:
                f.write(latex_content)
            print(f"LaTeX file generated: {output_path}")
            print(f"To compile: pdflatex {output_path}")
        
        # Print data sample
        print(f"\n=== Data Sample ===")
        for i, row in enumerate(data[:5]):  # Show first 5 rows
            print(f"Row {i+1}: {row}")
            if i >= 4:  # Only show first 5
                break
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()