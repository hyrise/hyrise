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

# set plot styles
plt.style.use("ggplot")

if len(sys.argv) != 2:
    sys.exit("Usage: " + sys.argv[0] + " benchmark.csv")

# TODO: make pretty with arguments if statistical evaluation should be done

df = pd.read_csv(sys.argv[1])

df[["fixture", "io_type", "filesize_mb", "threads", "real_time_appendix"]] = df["name"].str.split("/", 4, expand=True)

# drop rows containing pre-calculated statistical data (if provided)
df.drop(df[df.real_time_appendix.str.contains("_mean|_median|_stddev|_cv")].index, inplace=True)

# drop sequential benchmarks to better plot random access
# df.drop(df[df.io_type.str.contains("SEQUENTIAL")].index, inplace=True)

# drop MAP_PRIVATE benchmarks to better plot MAP_SEQUENTIAL benchmarks
# df.drop(df[df.io_type.str.contains("MAP_PRIVATE")].index, inplace=True)

df["filesize_mb"] = pd.to_numeric(df["filesize_mb"])
df["real_time_sec"] = pd.to_numeric(df["real_time"]) / 1000000000
df["mb_per_sec"] = df["filesize_mb"] / df["real_time_sec"]
df['bytes_per_second'].fillna(-1)
df.loc[df['bytes_per_second'] > 0, 'mb_per_sec'] = df["bytes_per_second"] / 1000000

for filesize in df['filesize_mb'].unique():
    df_filesize = df[df['filesize_mb'] == filesize]

    benchmark_results = sns.lineplot(data=df_filesize, x="threads", y="mb_per_sec", hue="io_type", marker='o', err_style='bars', err_kws={'capsize':10})

    benchmark_results.set(
        xlabel="#threads", ylabel="Throughput in MB/s", title=f"Different I/O method speed dependent on threads for {filesize}MB"
    )

    plt.legend(title="I/O Type")
    plt.show()

