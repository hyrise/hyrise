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
raw_df['filter_rate'] = raw_df['hits'] / raw_df['filter_size']

# Map hash function IDs to names
hash_function_names = {
    0: "std::hash",
    1: "boost::hash_combine",
    2: "xxhash",
    3: "duckdb's MurmurHash"
}

# Aggregate mean, geometric mean, min, and max filter rates for each hash function
aggregated_df = raw_df.groupby('hash_function')['filter_rate'].agg(
    mean_filter_rate='mean',
    geometric_mean_filter_rate=lambda x: gmean(x + 1e-9),  # Add small value to avoid log(0)
    min_filter_rate='min',
    max_filter_rate='max'
).reset_index()

# Add hash function names to the result and drop the hash_function column
aggregated_df['hash_function'] = aggregated_df['hash_function'].map(hash_function_names)

print(aggregated_df)

