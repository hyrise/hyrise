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

# CSV columns expected:
# vector_size,distinctiveness,overlap,filter_size,k,hash_function,run,build_time_ns,probe_time_ns,hits,saturation

raw_df = pd.read_csv(csv_file)

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
    saturation=('saturation', 'first')    # Saturation is equal for all groups
).reset_index()

# Add combined median times
df['combined_median_time_ns'] = df['median_build_time_ns'] + df['median_probe_time_ns']

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
            ('median_build_time_ns', 'Build Time (ns)', 'build_time'),
            ('median_probe_time_ns', 'Probe Time (ns)', 'probe_time'),
            ('combined_median_time_ns', 'Combined Time (ns)', 'combined_time')
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

            g.map_dataframe(scatterplot_with_lines)

            # Add titles and labels
            g.set_titles(row_template="Overlap: {row_name}", col_template="Filter Size: {col_name}")
            g.set_axis_labels("Filter Rate", y_label)
            g.add_legend(title="Hash Function / k")

            # Save the plot to a PDF
            pdf_filename = f"bloom_eval_{vector_size}_{distinctiveness}_{file_suffix}.pdf"
            plt.savefig(pdf_filename)
            print(f"Saved plot to {pdf_filename}")

            # Close the figure to prevent memory issues
            plt.close()