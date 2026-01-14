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
from typing import Dict, List
import argparse


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

def generate_pgfplot_data(data: List[Dict], x_col: str, y_col: str) -> str:
    """
    Generate pgfplot data section for a given x and y column combination.
    
    Args:
        data (List[Dict]): Benchmark data
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis
        
    Returns:
        str: Formatted data for pgfplot
    """
    data_lines = []
    
    # Group by index_name to create separate plots for each index
    indices = {}
    for row in data:
        index_name = row.get('index_name', 'Unknown')
        if index_name not in indices:
            indices[index_name] = []
        indices[index_name].append(row)
    
    # Sort each index data by x_col
    for index_name, index_data in indices.items():
        # Filter out rows that don't have the required columns
        valid_data = [row for row in index_data if x_col in row and y_col in row]
        if not valid_data:
            continue
            
        # Sort by x column
        valid_data.sort(key=lambda row: row[x_col])
        
        data_lines.append(f"% Data for {index_name}")
        data_lines.append(f"\\addplot coordinates {{")
        
        for row in valid_data:
            x_val = row[x_col]
            y_val = row[y_col]
            data_lines.append(f"    ({x_val}, {y_val})")
        
        data_lines.append("};")
        data_lines.append(f"\\addlegendentry{{{index_name}}}")
        data_lines.append("")
    
    return "\n".join(data_lines)


def create_figure_from_template(data: List[Dict], x_col: str, y_col: str, 
                              title: str, caption: str, label: str,
                              figure_template_path: str = None) -> str:
    """
    Create a single figure using a template.
    
    Args:
        data (List[Dict]): Benchmark data
        x_col (str): Column name for x-axis
        y_col (str): Column name for y-axis
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
    
    # Generate plot data
    plot_data = generate_pgfplot_data(data, x_col, y_col)
    
    # Apply replacements
    replacements = {
        '{{PLOT_DATA}}': plot_data,
        '{{X_LABEL}}': x_col.replace('_', ' ').title(),
        '{{Y_LABEL}}': y_col.replace('_', ' ').title(),
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
    
    # Parse PLOT placeholders from template (looking for %% {{PLOT:...}} format)
    plot_pattern = r'%% \{\{PLOT:([^,]+),([^,]+),([^,]+),([^,]+),([^}]+)\}\}'
    plot_matches = re.findall(plot_pattern, template_content)
    
    # Validate that there's at least one plot defined
    if not plot_matches:
        raise ValueError(f"No plots defined in template file {multiplot_template_path}. "
                        f"Template must contain at least one %% {{{{PLOT:...}}}} directive.")
    
    result_content = template_content
    
    # Process each plot placeholder
    for match in plot_matches:
        x_col, y_col, title, caption, label = match
        
        # Check if the required columns exist in the data
        has_data = any(x_col in row and y_col in row for row in data)
        
        if has_data:
            # Generate the figure
            figure = create_figure_from_template(
                data, x_col, y_col, title, caption, label, figure_template_path
            )
            
            # Replace the placeholder with the actual figure
            placeholder = f"%% {{{{PLOT:{x_col},{y_col},{title},{caption},{label}}}}}"
            result_content = result_content.replace(placeholder, figure)
        else:
            # Remove the placeholder if no data available
            placeholder = f"%% {{{{PLOT:{x_col},{y_col},{title},{caption},{label}}}}}"
            result_content = result_content.replace(placeholder, 
                f"% Skipped plot {title} - missing columns {x_col} or {y_col}")
            print(f"Warning: Skipping plot {title} - missing columns {x_col} or {y_col}")
    
    return result_content


def main():
    """Main function to parse arguments and execute the plotting workflow."""
    parser = argparse.ArgumentParser(description='Generate LaTeX plots from benchmark logs')
    parser.add_argument('log_file', help='Path to benchmark log file')
    parser.add_argument('--multiplot-template', help='Path to multiplot template file')
    parser.add_argument('--figure-template', help='Path to figure template file (for individual figures)')
    parser.add_argument('--output', help='Output LaTeX file path', 
                       default='benchmark_plot.tex')
    
    args = parser.parse_args()
    
    try:
        # Parse benchmark log
        data = parse_benchmark_log(args.log_file)
        
        # Print summary statistics
        print("\n=== Benchmark Summary ===")
        indices = set(row.get('index_name', 'Unknown') for row in data)
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