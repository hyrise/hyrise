#!/usr/bin/env python3

#
# Takes a FileIO benchmark output csv and plots read/write filesize against throughput in MB/s.
# Expects a benchmark csv as created by running FileIOReadMicroBenchmark and FileIOWriteMicroBenchmark with
# `--benchmark_format=csv`.
# To get a plot with error bars/statistical information simply supply a csv with multiple measurements for
# a <benchmark_type/filesize> combination. This can be achieved e.g. by using the `--benchmark_repetitions=x` argument.
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

#TODO: make pretty with arguments if statistical evaluation should be done

with open(sys.argv[1]) as csv_file:
    df = pd.read_csv(csv_file)
    df[['fixture', 'type', 'filesize_mb']] = df['name'].str.split('/', 2, expand=True)

    #drop rows containing pre-calculated statistical data (if provided)
    df.drop(df[df.filesize_mb.str.contains('_mean|_median|_stddev|_cv')].index, inplace=True)

    df['filesize_mb'] = pd.to_numeric(df['filesize_mb'])
    df['real_time_sec'] = df['real_time'] / 1000000000
    df['mb_per_sec'] =  df['filesize_mb'] / df['real_time_sec']

benchmark_results = sns.barplot(data=df,
                                 x='filesize_mb',
                                 y='mb_per_sec',
                                 hue='type',
                                 capsize=.1,
                                 errwidth=1)

benchmark_results.set(xlabel ='Filesize in MB', ylabel = 'Throughput in MB/s', title ='Different I/O method speed dependent on filesize')

plt.legend(title='I/O Type')
plt.show()

