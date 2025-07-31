#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from scipy.stats import gmean

# CSV columns expected:
# vector_size,distinctiveness,overlap,filter_size,k,hash_function,run,build_time_ns,probe_time_ns,hits,saturation

# Parse command-line argument
if len(sys.argv) != 2:
    print("Usage: python bloom_eval.py <csv_file>")
    sys.exit(1)

csv_file = sys.argv[1]

# CSV columns expected:
# vector_size,distinctiveness,overlap,filter_size,k,hash_function,run,build_time_ns,probe_time_ns,hits,saturation

raw_df = pd.read_csv(csv_file)

# Ensure only one row per combination of specified columns
grouped_df = raw_df.groupby(['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'hash_function', 'k'])

# Compute filter rate
raw_df['filter_rate'] = 1 - (raw_df['hits'] / raw_df['vector_size'])

# Map hash function IDs to names
hash_function_names = {
    0: "std::hash",
    1: "boost::hash_combine",
    2: "xxhash",
    3: "duckdb's MurmurHash"
}

# Aggregate mean, squared mean, min, and max filter rates for each hash function
aggregated_df = raw_df.groupby('hash_function')['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Add hash function names to the result and drop the hash_function column
aggregated_df['hash_function'] = aggregated_df['hash_function'].map(hash_function_names)

# Compute stats for the impact of different choices of k on filter rate (overall)
k_impact_overall_df = raw_df.groupby('k')['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Compute stats for the impact of different choices of k on filter rate (per hash function)
k_impact_per_hash_df = raw_df.groupby(['hash_function', 'k'])['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Add hash function names to the per-hash-function result
k_impact_per_hash_df['hash_function'] = k_impact_per_hash_df['hash_function'].map(hash_function_names)

# Compute global aggregated stats
global_aggregated_df = raw_df['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
)

# Compute aggregated stats per filter size
filter_size_aggregated_df = raw_df.groupby('filter_size')['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Compute stats per filter size and hash function
filter_size_hash_df = raw_df.groupby(['filter_size', 'hash_function'])['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Add hash function names to the result
filter_size_hash_df['hash_function'] = filter_size_hash_df['hash_function'].map(hash_function_names)

# Compute stats per filter size and k
filter_size_k_df = raw_df.groupby(['filter_size', 'k'])['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Compute stats per filter size, hash function, and k
filter_size_hash_k_df = raw_df.groupby(['filter_size', 'hash_function', 'k'])['filter_rate'].agg(
    mean_filter_rate='mean',
    squared_mean_filter_rate=lambda x: ((x**2).mean())**0.5,
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Add hash function names to the result
filter_size_hash_k_df['hash_function'] = filter_size_hash_k_df['hash_function'].map(hash_function_names)

# Ensure full DataFrame display for large outputs
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Print results
print("Aggregated filter rate stats per hash function:")
print(aggregated_df)
print("\nImpact of k on filter rate (overall):")
print(k_impact_overall_df)
print("\nImpact of k on filter rate (per hash function):")
print(k_impact_per_hash_df)

# Print global results
print("\nGlobal aggregated filter rate stats:")
print(global_aggregated_df)

# Print results per filter size
print("\nAggregated filter rate stats per filter size:")
print(filter_size_aggregated_df)

print("\nAggregated filter rate stats per filter size and hash function:")
print(filter_size_hash_df)

print("\nAggregated filter rate stats per filter size and k:")
print(filter_size_k_df)

print("\nAggregated filter rate stats per filter size, hash function, and k:")
print(filter_size_hash_k_df)

