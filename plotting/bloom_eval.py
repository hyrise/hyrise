#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys

# Parse command-line argument
if len(sys.argv) != 2:
    print("Usage: python bloom_eval.py <csv_file>")
    sys.exit(1)

csv_file = sys.argv[1]

df = pd.read_csv(csv_file)

# Filter for distinctiveness = 0.1
df = df[df['distinctiveness'] == 0.1]

# Add combined times and filter rate
df['combined_time_ns'] = df['build_time_ns'] + df['probe_time_ns']
df['filter_rate'] = 1 - (df['hits'] / df['vector_size'])

# Group by relevant columns and calculate median values
grouped = df.groupby(['vector_size', 'overlap', 'filter_size', 'hash_function', 'k'])
median_df = grouped[['combined_time_ns', 'filter_rate', 'saturation']].median().reset_index()
print(median_df.to_string())  # Print the first few rows of the median DataFrame for verification

# Print unique filter rates for each combination of vector_size, overlap, and filter_size
print("\nUnique filter rates for each combination of vector_size, overlap, and filter_size:")
for vector_size in median_df['vector_size'].unique():
    for overlap in median_df['overlap'].unique():
        for filter_size in median_df['filter_size'].unique():
            subset = median_df[
                (median_df['vector_size'] == vector_size) &
                (median_df['overlap'] == overlap) &
                (median_df['filter_size'] == filter_size)
            ]
            print(f"Vector Size: {vector_size}, Overlap: {overlap}, Filter Size: {filter_size}, Filter Rates: {subset['filter_rate'].unique()}")

# Generate facet plots for each vector size
vector_sizes = median_df['vector_size'].unique()
for vector_size in vector_sizes:
    subset = median_df[median_df['vector_size'] == vector_size]

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
            y="combined_time_ns",
            hue="hash_function",
            style="hash_function",
            markers=["o", "s"],
            **kwargs
        )
        for hash_function in data['hash_function'].unique():
            hash_subset = data[data['hash_function'] == hash_function]
            plt.plot(hash_subset['filter_rate'], hash_subset['combined_time_ns'], label=f"Hash {hash_function}")

    g.map_dataframe(scatterplot_with_lines)

    # Add titles and labels
    g.set_titles(row_template="Overlap: {row_name}", col_template="Filter Size: {col_name}")
    g.set_axis_labels("Filter Rate", "Combined Time (ns)")
    g.add_legend(title="Hash Function")

    # Save the plot to a PDF
    pdf_filename = f"bloom_eval_{vector_size}.pdf"
    plt.savefig(pdf_filename)
    print(f"Saved plot to {pdf_filename}")