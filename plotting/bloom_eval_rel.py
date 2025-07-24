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
df = raw_df.groupby(['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k']).apply(lambda group: group.iloc[1:]).reset_index(drop=True)

# Convert nanoseconds to milliseconds for better readability
df['build_time_ms'] = df['build_time_ns'] / 1_000_000
df['probe_time_ms'] = df['probe_time_ns'] / 1_000_000
df['combined_time_ms'] = df['build_time_ms'] + df['probe_time_ms']

# Add a unique group identifier for each combination of hash_function and k
df['group_id'] = df['hash_function'].astype(str) + "_" + df['k'].astype(str)

# Generate relplots for each vector size
vector_sizes = df['vector_size'].unique()
distinctiveness_values = df['distinctiveness'].unique()

for vector_size in vector_sizes:
    for distinctiveness in distinctiveness_values:
        subset = df[
            (df['vector_size'] == vector_size) &
            (df['distinctiveness'] == distinctiveness)
        ]

        for time_metric, y_label, file_suffix in [
            ('build_time_ms', 'Build Time (ms)', 'build_time'),
            ('probe_time_ms', 'Probe Time (ms)', 'probe_time'),
            ('combined_time_ms', 'Combined Time (ms)', 'combined_time')
        ]:
            # Create relplot with individual data points
            g = sns.relplot(
                data=subset,
                x="group_id",  # Use group_id for x-axis
                y=time_metric,
                hue="hash_function",
                style="k",
                row="overlap",
                col="filter_size",
                kind="scatter",
                height=4,
                aspect=1.5,
                alpha=0.7,  # Make points slightly transparent to see overlapping points
                s=50  # Point size
            )

            # Add titles and labels
            g.set_titles(row_template="Overlap: {row_name}", col_template="Filter Size: {col_name}")
            g.set_axis_labels("Hash Function / k", y_label)

            # Rotate x-axis labels for better readability
            for ax in g.axes.flat:
                ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

            # Adjust layout to prevent clipping
            plt.tight_layout()

            # Save the plot to a PDF using the correct figure
            pdf_filename = f"bloom_eval_{vector_size}_{distinctiveness}_{file_suffix}.pdf"
            g.fig.savefig(pdf_filename)
            print(f"Saved plot to {pdf_filename}")

            # Show the plot for debugging (optional, can be removed later)
            # plt.show()

            # Close the figure to prevent memory issues
            plt.close(g.fig)