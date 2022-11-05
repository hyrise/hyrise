#!/usr/bin/env python3

#
# Takes a FileIO benchmark output csv and plots read/write filesize against needed iterations for different I/O types.
# Expects a benchmark csv as created by running FileIOReadMicroBenchmark and FileIOWriteMicroBenchmark with `--benchmark_format=csv`.
#

import pandas as pd
import sys
import matplotlib.pyplot as plt
import seaborn as sns

#set plot styles
sns.set_style('whitegrid')
plt.style.use('ggplot')

if len(sys.argv) != 2:
    sys.exit("Usage: " + sys.argv[0] + " benchmark.csv")

with open(sys.argv[1]) as csv_file:
    df = pd.read_csv(csv_file)
    df[['fixture', 'type', 'filesize_mb']] = df['name'].str.split('/', 2, expand=True)
    df['filesize_mb'] = pd.to_numeric(df['filesize_mb'])

benchmark_results = sns.lineplot(data=df,
                                 x='filesize_mb',
                                 y='iterations',
                                 hue='type',
                                 marker='o')

# make file size axis logarithmic
benchmark_results.set(xscale='log', xticks=df['filesize_mb'], xticklabels=df['filesize_mb'])

benchmark_results.set(xlabel ='Filesize in MB', ylabel = 'Iteration Count', title ='Different I/O method speed dependent on filesize')

plt.legend(title='I/O Type')
plt.show()

