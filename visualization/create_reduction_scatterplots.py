#!/usr/bin/env python3

import pandas as pd
import sys
import matplotlib.pyplot as plt

# Set font sizes for better readability
plt.rc('font', size=16)
plt.rc('axes', titlesize=18)
plt.rc('axes', labelsize=16)
plt.rc('xtick', labelsize=14)
plt.rc('ytick', labelsize=14)
plt.rc('legend', fontsize=14)
plt.rc('figure', titlesize=20)

# Load the data
if len(sys.argv) < 2:
    print("Usage: python create_reduction_scatterplots.py <reduction_stats.csv> [max_input_count_millions]")
    sys.exit(1)

data = pd.read_csv(sys.argv[1])

# Filter for semi-join reduction type only
df = data[data['reduction_type'] == 'semi'].copy()

# Convert to millions
df['input_count'] = df['input_count'] / 1e6
df['output_count'] = df['output_count'] / 1e6

# Optional: filter by max input count if second parameter is given
max_input_count = None
if len(sys.argv) >= 3:
    try:
        max_input_count = float(sys.argv[2])
        df = df[df['input_count'] <= max_input_count]
    except ValueError:
        print("Second parameter must be a number (max input count in millions).")
        sys.exit(1)

# Define colors for each benchmark
benchmark_colors = {
    'joinorder': '#1f77b4',   # blue
    'starschema': '#ff7f0e',  # orange
    'tpcds': '#2ca02c',       # green
    'tpch': '#d62728'         # red
}

# Create figure with two subplots side by side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# Function to plot data on a given axis
def plot_reduction(ax, use_log=False):
    # Plot each benchmark with different colors
    for benchmark in df['benchmark'].unique():
        benchmark_data = df[df['benchmark'] == benchmark]
        ax.scatter(
            benchmark_data['input_count'], 
            benchmark_data['output_count'],
            color=benchmark_colors.get(benchmark, 'gray'),
            label=benchmark.upper() if benchmark != 'joinorder' else 'Join Order',
            alpha=0.7,
            s=100
        )
    
    # Add y=x reference line without legend label
    max_val = max(df['input_count'].max(), df['output_count'].max())
    max_out = df['output_count'].max() * 1.15
    if use_log:
        # For log scale, start from a small positive value
        min_val = min(df['input_count'].min(), df['output_count'].min()) * 0.5
        ax.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.5, label=None)
    else:
        ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.5, label=None)
    
    # Set labels
    ax.set_xlabel('Input Count [Millions]')
    ax.set_ylabel('Output Count [Millions]')
    ax.set_ylim((0, max_out))
    
    # Set scale
    if use_log:
        ax.set_xscale('log')
        ax.set_yscale('log')
        title = 'Semi-Join Reduction (Log Scale)'
    else:
        title = 'Semi-Join Reduction (Linear Scale)'
    
    # Add filter info to title if max_input_count is set
    if max_input_count is not None:
        title += f"\n(Input Count ≤ {max_input_count}M)"
    ax.set_title(title)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Add legend
    ax.legend(loc='upper left')

# Plot with linear scale
plot_reduction(ax1, use_log=False)

# Plot with log scale
plot_reduction(ax2, use_log=True)

# Adjust layout to prevent overlap
plt.tight_layout()

# Save the combined plot
plt.savefig('reduction_scatterplots.pdf', dpi=300, bbox_inches='tight')
plt.show()

# Print statistics
print("\nReduction Statistics by Benchmark:")
print("-" * 50)
for benchmark in df['benchmark'].unique():
    benchmark_data = df[df['benchmark'] == benchmark]
    avg_reduction = (1 - benchmark_data['output_count'] / benchmark_data['input_count']).mean() * 100
    count = len(benchmark_data)
    print(f"{benchmark.upper():12} | Queries: {count:3d} | Avg reduction: {avg_reduction:5.1f}%")

# Additional statistics for extreme cases
print("\nExtreme Cases:")
print("-" * 50)
print("Best reductions (top 5):")
df['reduction_rate'] = 1 - df['output_count'] / df['input_count']
top_reductions = df.nlargest(5, 'reduction_rate')[['benchmark', 'query', 'input_count', 'output_count', 'reduction_rate']]
for _, row in top_reductions.iterrows():
    print(f"  {row['benchmark']:12} | {row['query']:20} | {row['reduction_rate']*100:5.1f}% reduction")

print("\nWorst reductions (bottom 5):")
worst_reductions = df.nsmallest(5, 'reduction_rate')[['benchmark', 'query', 'input_count', 'output_count', 'reduction_rate']]
for _, row in worst_reductions.iterrows():
    print(f"  {row['benchmark']:12} | {row['query']:20} | {row['reduction_rate']*100:5.1f}% reduction")

# Print middle 5 reductions
print("\nMiddle reductions (middle 5):")
middle_idx = len(df) // 2
# If less than 5 rows, just print all
if len(df) < 5:
    middle_reductions = df.sort_values('reduction_rate')
else:
    middle_reductions = df.sort_values('reduction_rate').iloc[middle_idx-2:middle_idx+3]
middle_reductions = middle_reductions[['benchmark', 'query', 'input_count', 'output_count', 'reduction_rate']]
for _, row in middle_reductions.iterrows():
    print(f"  {row['benchmark']:12} | {row['query']:20} | {row['reduction_rate']*100:5.1f}% reduction")