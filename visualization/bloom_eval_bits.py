#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import numpy as np

# Parse command-line argument
if len(sys.argv) != 2:
    print("Usage: python bloom_eval.py <csv_file>")
    sys.exit(1)

csv_file = sys.argv[1]

# CSV columns expected:
# vector_size,distinctiveness,overlap,filter_size,k,hash_function,run,build_time_ns,probe_time_ns,hits,saturation

raw_df = pd.read_csv(csv_file)

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
raw_df[['quarter_counts', 'quarter_percentages']] = raw_df['bit_distribution'].apply(
    lambda x: pd.Series(parse_bit_distribution(x))
)

# Add combined times and filter rate
raw_df['filter_rate'] = 1 - (raw_df['hits'] / raw_df['vector_size'])

# Remove the first row of each group (warmup)
raw_df = raw_df.groupby(['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k']).apply(lambda group: group.iloc[1:]).reset_index(drop=True)

# Group by relevant columns and calculate median values for build_time_ns and probe_time_ns
grouped = raw_df.groupby(['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k'])
df = grouped.agg(
    median_build_time_ns=('build_time_ns', 'median'),
    median_probe_time_ns=('probe_time_ns', 'median'),
    filter_rate=('filter_rate', 'first'),  # Filter rate is equal for all groups
    saturation=('saturation', 'first'),    # Saturation is equal for all groups
    quarter_counts=('quarter_counts', 'first'),
    quarter_percentages=('quarter_percentages', 'first')
).reset_index()

# Add combined median times
df['combined_median_time_ns'] = df['median_build_time_ns'] + df['median_probe_time_ns']

# Convert nanoseconds to milliseconds for better readability
df['median_build_time_ms'] = df['median_build_time_ns'] / 1_000_000
df['median_probe_time_ms'] = df['median_probe_time_ns'] / 1_000_000
df['combined_median_time_ms'] = df['combined_median_time_ns'] / 1_000_000

# print(df.to_string())  # Print the aggregated DataFrame for verification

# exit(0)

# Print unique filter rates for each combination of vector_size, overlap, and filter_size
# print("\nUnique filter rates for each combination of vector_size, overlap, and filter_size:")
# for vector_size in median_df['vector_size'].unique():
#     for overlap in median_df['overlap'].unique():
#         for filter_size in median_df['filter_size'].unique():
#             subset = median_df[
#                 (median_df['vector_size'] == vector_size) &
#                 (median_df['overlap'] == overlap) &
#                 (median_df['filter_size'] == filter_size)
#             ]
#             print(f"Vector Size: {vector_size}, Overlap: {overlap}, Filter Size: {filter_size}, Filter Rates: {subset['filter_rate'].unique()}")

# Generate facet plots for each vector size
vector_sizes = df['vector_size'].unique()
distinctiveness_values = df['distinctiveness'].unique()

for vector_size in vector_sizes:
    for distinctiveness in distinctiveness_values:
        subset = df[
            (df['vector_size'] == vector_size) &
            (df['distinctiveness'] == distinctiveness)
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
                
                # Add annotations for each point
                for _, row in data.iterrows():
                    if pd.notna(row['quarter_counts']) and pd.notna(row['quarter_percentages']):
                        annotation_text = f"Sat: {row['saturation']:.3f}\nQ: {row['quarter_counts']}\n%: {row['quarter_percentages']}"
                    else:
                        annotation_text = f"Sat: {row['saturation']:.3f}"
                    
                    plt.annotate(
                        annotation_text,
                        (row['filter_rate'], row[time_metric]),
                        xytext=(5, 5),
                        textcoords='offset points',
                        fontsize=6,
                        alpha=0.7,
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor='none')
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