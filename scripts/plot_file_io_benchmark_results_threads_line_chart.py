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

if len(sys.argv) < 3:
    sys.exit("Usage: " + sys.argv[0] + " benchmark.csv fio_results.csv <read/write>")

if len(sys.argv) != 4:
    print("You did not specify the benchmark type. If you want only fio read or only fio write benchmarks plotted, please do so.")

# TODO: make pretty with arguments if statistical evaluation should be done

benchmark_df = pd.read_csv(sys.argv[1])
fio_df = pd.read_csv(sys.argv[2])

benchmark_df[["fixture", "io_type", "filesize_mb", "threads", "real_time_appendix"]] = benchmark_df["name"].str.split("/", 4, expand=True)
fio_df[["fixture", "io_type", "filesize_mb", "threads", "real_time_appendix"]] = fio_df["name"].str.split("/", 4, expand=True)

# drop rows containing pre-calculated statistical data (if provided)
benchmark_df.drop(benchmark_df[benchmark_df.real_time_appendix.str.contains("_mean|_median|_stddev|_cv")].index, inplace=True)

# if benchmark_type argument is given only use fitting fio measurements
if len(sys.argv) >= 4:
    benchmark_type = sys.argv[3]
    fio_df = fio_df[fio_df.io_type.str.contains(benchmark_type)]

# drop sequential benchmarks to better plot random access
# benchmark_df.drop(benchmark_df[benchmark_df.io_type.str.contains("SEQUENTIAL")].index, inplace=True)

# drop MAP_PRIVATE benchmarks to better plot MAP_SEQUENTIAL benchmarks
# benchmark_df.drop(benchmark_df[benchmark_df.io_type.str.contains("MAP_PRIVATE")].index, inplace=True)

# calculate mb_per_sec for benchmark_df
benchmark_df["filesize_mb"] = pd.to_numeric(benchmark_df["filesize_mb"])
fio_df["filesize_mb"] = pd.to_numeric(fio_df["filesize_mb"]) #adapt fio df for consistency
benchmark_df["real_time_sec"] = pd.to_numeric(benchmark_df["real_time"]) / 1000000000
benchmark_df["mb_per_sec"] = benchmark_df["filesize_mb"] / benchmark_df["real_time_sec"]

#calculate mb_per_sec for fio_df
fio_df['mb_per_sec'] = fio_df["bytes_per_second"] / 1000000
benchmark_df = benchmark_df.append(fio_df)

for filesize in benchmark_df['filesize_mb'].unique():
    benchmark_df_filesize = benchmark_df[benchmark_df['filesize_mb'] == filesize]

    benchmark_results = sns.lineplot(data=benchmark_df_filesize, x="threads", y="mb_per_sec", hue="io_type", marker='o', err_style='bars', err_kws={'capsize':10})

    benchmark_results.set(
        xlabel="#threads", ylabel="Throughput in MB/s", title=f"Different I/O method speed dependent on threads for {filesize}MB"
    )

    plt.legend(title="I/O Type")
    plt.show()

