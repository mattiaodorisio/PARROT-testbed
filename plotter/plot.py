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
    """Reset the color mapping - useful for testing or when starting a new document."""
    global _SERIES_COLOR_MAP, _COLOR_INDEX
    _SERIES_COLOR_MAP = {}
    _COLOR_INDEX = 0


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
        if '=' in condition:
            key, value = condition.split('=', 1)
            # Restore escaped semicolons in both key and value
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
    
    # Split by semicolon to separate primary groupby from aggregation
    parts = [part.strip() for part in groupby_str.split(';')]
    
    primary_groupby = parts[0]
    best_aggregation = None
    
    # Check for BEST aggregation in remaining parts
    for part in parts[1:]:
        best_pattern = r'^BEST\(([^)]+)\)$'
        match = re.match(best_pattern, part, re.IGNORECASE)
        if match:
            best_aggregation = ('BEST', match.group(1))
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
    
    # Apply BEST selection if specified
    if best_aggregation:
        best_func, best_col = best_aggregation
        if best_func.upper() == 'BEST':
            # Determine the y column name after aggregation
            y_col_name = f'{agg_func.lower()}_{agg_col}' if agg_func != 'NONE' else agg_col
            
            # Select best variants
            aggregated_data = select_best_variant(aggregated_data, primary_groupby, best_col, y_col_name, x_col)
    
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

def generate_pgfplot_data(data: List[Dict], x_col: str, y_col: str, groupby_col: str = None) -> str:
    """
    Generate pgfplot data section for a given x and y column combination.
    
    Args:
        data (List[Dict]): Benchmark data (already filtered and aggregated)
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis  
        groupby_col (str): Column used for grouping (for legend, may include BEST aggregation)
        
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
            if not valid_data:
                continue
                
            # Sort by x column
            valid_data.sort(key=lambda row: row[x_col])
            
            # Determine legend entry - include variant if BEST aggregation was used
            if best_aggregation and best_aggregation[0] == 'BEST':
                variant_col = best_aggregation[1]
                # Get the variant from the first valid data point (they should all be the same due to BEST selection)
                if valid_data and variant_col in valid_data[0]:
                    variant_value = valid_data[0][variant_col]
                    if variant_value and str(variant_value).lower() != 'none':
                        legend_entry = f"{group_value} ({variant_value})"
                    else:
                        legend_entry = group_value
                else:
                    legend_entry = group_value
            else:
                legend_entry = group_value

            # Get consistent color for the series based on index_name only
            series_color = get_series_color(str(group_value))

            data_lines.append(f"% Data for {legend_entry}")
            data_lines.append(f"\\addplot[color={series_color}, mark=*, mark size=1.5pt] coordinates {{")
            
            for row in valid_data:
                x_val = row[x_col]
                y_val = row[y_col]
                data_lines.append(f"    ({x_val}, {y_val})")
            
            data_lines.append("};")
            if max_legend_entry > 0:
                data_lines.append(f"\\addlegendentry{{{legend_entry}}}")
                max_legend_entry -= 1
            data_lines.append("")
    
    else:
        # No grouping, plot all data as a single series
        valid_data = [
            row for row in data 
            if x_col in row and y_col in row 
            and is_valid_plot_value(row[x_col]) and is_valid_plot_value(row[y_col])
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
    
    # Add workload type display name
    if 'workload_type' in parameters:
        workload_display_map = {
            'LOOKUP_IN_DISTRIBUTION': 'In-Distribution Lookup',
            'LOOKUP_EXISTING': 'Existing Lookup', 
            'INSERT_IN_DISTRIBUTION': 'In-Distribution Insert'
        }
        parameters['workload_display_name'] = workload_display_map.get(
            parameters['workload_type'], parameters['workload_type']
        )
    
    # Substitute parameters in caption
    try:
        substituted_caption = caption.format(**parameters)
        return substituted_caption
    except KeyError as e:
        print(f"Warning: Parameter {e} not found for caption substitution in: {caption}")
        return caption
    except Exception as e:
        print(f"Warning: Error substituting parameters in caption '{caption}': {e}")
        return caption


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
    
    # If BEST aggregation is specified, select the best variant
    if best_aggregation and best_aggregation[0] == 'BEST':
        best_col = best_aggregation[1]
        filtered_data = select_best_variant(filtered_data, primary_groupby_col, best_col, actual_y_col, x_col)
    
    # Group and aggregate data
    processed_data = group_and_aggregate(filtered_data, primary_groupby_col, agg_func, actual_y_col, x_col)
    
    if not processed_data:
        return f"% No aggregated data available for {title}"
    
    # Determine the actual y column name after aggregation
    actual_y_col_name = f'{agg_func.lower()}_{actual_y_col}' if agg_func != 'NONE' else actual_y_col
    
    # Generate plot data (pass original groupby_col to preserve BEST aggregation info)
    plot_data = generate_pgfplot_data(processed_data, x_col, actual_y_col_name, groupby_col)
    
    # Prepare better labels
    x_label = x_col.replace('_', ' ').title()
    if agg_func != 'NONE':
        y_label = f"{agg_func.title()} {actual_y_col.replace('_', ' ').title()}"
    else:
        y_label = actual_y_col.replace('_', ' ').title()
    
    # Special handling for certain column names
    if 'total_time' in actual_y_col:
        y_label += ' (seconds)'
    elif 'throughput' in actual_y_col:
        y_label += ' (ops/sec)'
    
    # Substitute parameters in caption and title
    caption = substitute_caption_parameters(caption, filter_conditions, data, x_col, y_col, groupby_col)
    title = substitute_caption_parameters(title, filter_conditions, data, x_col, y_col, groupby_col)
    
    # Substitute parameters in caption
    caption = substitute_caption_parameters(caption, filter_conditions, data, x_col, y_col, groupby_col)
    
    # Apply replacements
    replacements = {
        '{{PLOT_DATA}}': plot_data,
        '{{X_LABEL}}': x_label,
        '{{Y_LABEL}}': y_label,
        '{{TITLE}}': title,
        '{{CAPTION}}': caption,
        '{{LABEL}}': label
    }
    
    result_content = template_content
    for placeholder, replacement in replacements.items():
        result_content = result_content.replace(placeholder, replacement)
    
    return result_content


def create_performance_plot(data: List[Dict], multiplot_template_path: str = None, 
                          figure_template_path: str = None) -> str:
    """
    Create a comprehensive performance plot comparing all indices using templates.
    
    Args:
        data (List[Dict]): Benchmark data
        multiplot_template_path (str): Path to multiplot template (optional)
        figure_template_path (str): Path to figure template (optional)
        
    Returns:
        str: Complete LaTeX document code
    """
    # Path to multiplot template
    if multiplot_template_path is None:
        multiplot_template_path = os.path.join(os.path.dirname(__file__), 'multiplot_template.tex')
    
    # Read multiplot template
    try:
        with open(multiplot_template_path, 'r') as f:
            template_content = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Multiplot template file not found: {multiplot_template_path}")
    except Exception as e:
        raise Exception(f"Error reading multiplot template file {multiplot_template_path}: {e}")
    
    # Parse PLOT placeholders from template (looking for new 7-parameter format)
    plot_pattern = r'^%% \{\{PLOT:([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^}]+)\}\}$'
    plot_matches = re.findall(plot_pattern, template_content, re.MULTILINE)
    
    # Validate that there's at least one plot defined
    if not plot_matches:
        raise ValueError(f"No plots defined in template file {multiplot_template_path}. "
                        f"Template must contain at least one %% {{{{PLOT:...}}}} directive with 7 parameters.")
    
    # Initialize color mapping for consistent colors across all plots
    # Always use 'index_name' as the basis for color consistency
    try:
        initialize_color_mapping(data, 'index_name')
    except Exception as e:
        print(f"Warning: Could not initialize color mapping: {e}")
    
    result_content = template_content
    
    # Process each plot placeholder
    for match in plot_matches:
        x_col, y_col, filter_str, groupby_col, title, caption, label = match
        
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
            # Generate the figure
            figure = create_figure_from_template(
                data, x_col, y_col, filter_conditions, groupby_col,
                title, caption, label, figure_template_path
            )
            
            # Replace the placeholder with the actual figure
            placeholder = f"%% {{{{PLOT:{x_col},{y_col},{filter_str},{groupby_col},{title},{caption},{label}}}}}"
            result_content = result_content.replace(placeholder, figure)
        else:
            # Remove the placeholder if no data available
            placeholder = f"%% {{{{PLOT:{x_col},{y_col},{filter_str},{groupby_col},{title},{caption},{label}}}}}"
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


def main():
    """Main function to parse arguments and execute the plotting workflow."""
    parser = argparse.ArgumentParser(description='Generate LaTeX plots from benchmark logs')
    parser.add_argument('input_path', help='Path to directory containing benchmark log files, or single benchmark log file')
    parser.add_argument('--multiplot-template', help='Path to multiplot template file')
    parser.add_argument('--figure-template', help='Path to figure template file (for individual figures)')
    parser.add_argument('--output', help='Output LaTeX file path', 
                       default='benchmark_plot.tex')
    
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
        
        # Generate plot using template system
        latex_content = create_performance_plot(
            data, args.multiplot_template, args.figure_template
        )
        
        # Write output file
        output_path = args.output
        with open(output_path, 'w') as f:
            f.write(latex_content)
        
        print(f"\n=== Output ===")
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