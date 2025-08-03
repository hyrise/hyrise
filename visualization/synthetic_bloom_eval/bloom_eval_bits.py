#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import numpy as np

# Parse command-line arguments
if len(sys.argv) != 3:
    print("Usage: python bloom_eval_bits.py <runtime_csv> <hits_csv>")
    sys.exit(1)

runtime_csv = sys.argv[1]
hits_csv = sys.argv[2]

# Read the CSV files
print(f"Reading runtime data from {runtime_csv}...")
runtime_df = pd.read_csv(runtime_csv)
print("Runtime data read successfully.")

# Drop unnecessary columns from runtime_df before merging
columns_to_drop = ['hits', 'saturation', 'bit_distribution']
runtime_df = runtime_df.drop(columns=[col for col in columns_to_drop if col in runtime_df.columns])

# Group runtime data and calculate medians
print("Grouping runtime data and calculating medians...")
grouped_runtime_df = runtime_df.groupby(['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k']).agg(
    median_build_time_ns=('build_time_ns', 'median'),
    median_probe_time_ns=('probe_time_ns', 'median')
).reset_index()
print("Runtime data grouped successfully.")

print(f"Reading hits data from {hits_csv}...")
hits_df = pd.read_csv(hits_csv)
print("Hits data read successfully.")

# Parse bit distribution and convert to quarters
def parse_bit_distribution(bit_dist_str):
    """Parse bit distribution string and group into quarters"""
    if pd.isna(bit_dist_str):
        return None, None
    
    counts = list(map(int, bit_dist_str.split(':')))
    # Group into quarters (every 25 values)
    quarter_size = len(counts) // 4
    quarters = []
    for i in range(4):
        start_idx = i * quarter_size
        end_idx = (i + 1) * quarter_size if i < 3 else len(counts)
        quarter_sum = sum(counts[start_idx:end_idx])
        quarters.append(quarter_sum)
    
    # Calculate distribution percentages
    total_ones = sum(quarters)
    if total_ones > 0:
        quarter_percentages = [round(q / total_ones * 100, 1) for q in quarters]
    else:
        quarter_percentages = [0, 0, 0, 0]
    
    return quarters, quarter_percentages

# Apply bit distribution parsing
print("Parsing bit distribution...")
hits_df[['quarter_counts', 'quarter_percentages']] = hits_df['bit_distribution'].apply(
    lambda x: pd.Series(parse_bit_distribution(x))
)
print("Finished parsing bit distribution.")

# Add combined times and filter rate
print("Calculating filter rate...")
hits_df['filter_rate'] = 1 - (hits_df['hits'] / hits_df['vector_size'])
print("Filter rate calculation complete.")

# Drop unnecessary columns from runtime_df if they exist
columns_to_drop = ['hits', 'bit_distribution', 'run', 'build_time_ns', 'probe_time_ns']
hits_df = hits_df.drop(columns=[col for col in columns_to_drop if col in hits_df.columns])

# Join grouped runtime data with hits data
print("Joining runtime data with hits data...")
merged_df = pd.merge(
    grouped_runtime_df,
    hits_df,
    on=['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k'],
    how='inner'
)

# Check for missing join partners
if len(grouped_runtime_df) != len(merged_df) or len(hits_df) != len(merged_df):
    print("Error: Some rows in the runtime data have no matching partner in the hits data.")
    sys.exit(1)

print("Join successful.")

# Add combined median times
print("Calculating combined median times...")
merged_df['combined_median_time_ns'] = merged_df['median_build_time_ns'] + merged_df['median_probe_time_ns']
merged_df['median_build_time_ms'] = merged_df['median_build_time_ns'] / 1_000_000
merged_df['median_probe_time_ms'] = merged_df['median_probe_time_ns'] / 1_000_000
merged_df['combined_median_time_ms'] = merged_df['combined_median_time_ns'] / 1_000_000
print("Combined median times calculated.")

# Ensure all columns are printed
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

#print(merged_df)

# Generate facet plots for each vector size
vector_sizes = merged_df['vector_size'].unique()
distinctiveness_values = merged_df['distinctiveness'].unique()

for vector_size in vector_sizes:
    for distinctiveness in distinctiveness_values:
        subset = merged_df[
            (merged_df['vector_size'] == vector_size) &
            (merged_df['distinctiveness'] == distinctiveness)
        ]

        for time_metric, y_label, file_suffix in [
            ('median_build_time_ms', 'Build Time (ms)', 'build_time'),
            ('median_probe_time_ms', 'Probe Time (ms)', 'probe_time'),
            ('combined_median_time_ms', 'Combined Time (ms)', 'combined_time')
        ]:
            # Create facet grid
            g = sns.FacetGrid(
                subset,
                row="overlap",
                col="filter_size",
                margin_titles=True,
                height=4,
                aspect=1.5
            )

            # Define scatterplot with lines connecting points of the same hash function
            def scatterplot_with_lines(data, **kwargs):
                sns.scatterplot(
                    data=data,
                    x="filter_rate",
                    y=time_metric,
                    hue="hash_function",
                    style="k",  # Different symbols for different k values
                    **kwargs
                )
                for hash_function in data['hash_function'].unique():
                    hash_subset = data[data['hash_function'] == hash_function]
                    plt.plot(hash_subset['filter_rate'], hash_subset[time_metric], label=f"Hash {hash_function}")
                
                # Add light table in the background
                table_data = []
                for _, row in data.iterrows():
                    table_data.append([
                        row['hash_function'],
                        row['k'],
                        f"{row['saturation']:.3f}",
                        row['quarter_counts']
                    ])
                
                # Add individual titles for each scatterplot
                for _, row in data.iterrows():
                    range_min_0 = 0
                    range_max_0 = row['vector_size'] * row['distinctiveness']
                    range_min_1 = range_max_0 * (1 - row['overlap'])
                    range_max_1 = range_max_0 + range_min_1
                    plt.text(
                        0.5, -0.1,  # Positioning beneath the scatterplot
                        f"Build Range: [{range_min_0}, {range_max_0}] | Probe Range: [{range_min_1}, {range_max_1}]",
                        fontsize=8,
                        color='black',
                        ha='center',
                        va='center',
                        transform=plt.gca().transAxes
                    )
                
                # Create table as a background element
                for i, row in enumerate(table_data):
                    table_text = f"Hash: {row[0]}, k: {row[1]}, Sat: {row[2]}, Q: {row[3]}"
                    plt.text(
                        0.5, 0.5 - i * 0.05,  # Positioning in the background of the scatterplot
                        table_text,
                        fontsize=8,
                        color='darkgrey',  # Slightly darker grey for better visibility
                        alpha=0.5,  # Moderate transparency for subtle appearance
                        ha='center',
                        va='center',
                        transform=plt.gca().transAxes
                    )
            
            g.map_dataframe(scatterplot_with_lines)

            # Add titles and labels
            g.set_titles(row_template="Overlap: {row_name}", col_template="Filter Size: {col_name}")
            g.set_axis_labels("Filter Rate", y_label)
            
            # Add legend with both hash function and k information
            g.add_legend(title="Hash Function / k")

            # Save the plot to a PDF
            pdf_filename = f"bloom_eval_{vector_size}_{distinctiveness}_{file_suffix}.pdf"
            plt.savefig(pdf_filename)
            print(f"Saved plot to {pdf_filename}")

            # Close the figure to prevent memory issues
            plt.close()