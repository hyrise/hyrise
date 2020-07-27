#!/usr/bin/env python3

import argparse
from collections import OrderedDict
import json
import matplotlib
import os

# To prevent _tkinter.TclError: https://stackoverflow.com/a/37605654
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# This script compares different multi-threaded benchmark results.
# It takes the paths of multiple result folders (as created by benchmark_multithreaded.py),
# and creates a performance and scale-up comparison graph, per default as pdf,
# optionally as png via --format 'png'.
#
# Example usage:
# python3 ./scripts/compare_benchmarks_multithreaded.py path/to/result_folder_1 path/to/result_folder_2


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "results", nargs="+", help="Paths to individual result folders created by benchmark_multithreaded.py"
    )
    parser.add_argument("-f", "--format", choices=["pdf", "png"], type=str.lower, default="pdf")

    return parser


def is_square(n):
    return n ** 0.5 == int(n ** 0.5)


def get_subplot_row_and_column_count(num_plots):
    while not is_square(num_plots):
        num_plots += 1
    return num_plots ** 0.5


def plot_performance(flipped_results, n_rows_cols, numa_borders, max_cores):
    fig = plt.figure(figsize=(n_rows_cols * 3, n_rows_cols * 2))

    plot_pos = 0
    legend = OrderedDict()

    for tpch, multiple_results in flipped_results.items():
        plot_pos += 1
        ax = fig.add_subplot(n_rows_cols, n_rows_cols, plot_pos)
        ax.set_title(tpch)

        for numa_border in numa_borders:
            ax.axvline(numa_border, color="gray", linestyle="dashed", linewidth=1.0)

        for label, one_result in multiple_results.items():
            multithreaded_plot = ax.plot(one_result["cores"], one_result["items_per_second"], label=label, marker=".")
            if label not in legend:
                legend[label] = multithreaded_plot[0]

            if "singlethreaded" in one_result:
                label_single = label + " single-threaded"
                singlethreaded_plot = ax.axhline(
                    one_result["singlethreaded"],
                    color=multithreaded_plot[0].get_color(),
                    linestyle="dashed",
                    linewidth=1.0,
                    label=label_single,
                )
                if label_single not in legend:
                    legend[label_single] = singlethreaded_plot

        # The throuput can be from 0 queries/s to an arbitrarily large number, so no upper limit for the y-axis
        ax.set_ylim(ymin=0)
        ax.set_xlim(xmin=0, xmax=max_cores)

    # Create one legend for the whole plot
    plt.figlegend((line_type for line_type in legend.values()), (label for label in legend.keys()), "lower right")

    # This should prevent axes from different plots to overlap etc
    plt.tight_layout()

    # Add big axis descriptions for all plots
    axis_description = fig.add_subplot(111, frameon=False)
    plt.tick_params(labelcolor="none", top=False, bottom=False, left=False, right=False)
    axis_description.set_xlabel("Utilized cores", labelpad=10)
    axis_description.set_ylabel("Throughput (queries / s)", labelpad=20)

    result_plot_file = os.path.join(os.getcwd(), "benchmark_comparison_performance." + args.format)
    plt.savefig(result_plot_file, bbox_inches="tight")
    print("Plot saved as: " + result_plot_file)


def plot_scaleup(flipped_results, n_rows_cols, numa_borders, max_cores):
    fig = plt.figure(figsize=(n_rows_cols * 3, n_rows_cols * 2))

    plot_pos = 0
    legend = OrderedDict()

    for tpch, multiple_results in flipped_results.items():
        plot_pos += 1
        ax = fig.add_subplot(n_rows_cols, n_rows_cols, plot_pos)
        ax.set_title(tpch)

        for numa_border in numa_borders:
            ax.axvline(numa_border, color="gray", linestyle="dashed", linewidth=1.0)

        for label, one_result in multiple_results.items():
            ips, cores = one_result["items_per_second"], one_result["cores"]
            # Throughput per number of cores, relative to single-threaded performance
            scaleup = [y / (x * one_result["singlethreaded"]) for y, x in zip(ips, cores)]
            multithreaded_plot = ax.plot(cores, scaleup, label=label, marker=".")
            if label not in legend:
                legend[label] = multithreaded_plot[0]

        ax.set_ylim(ymin=0.0, ymax=1.0)
        ax.set_xlim(xmin=0, xmax=max_cores)

    # Create one legend for the whole plot
    plt.figlegend((line_type for line_type in legend.values()), (label for label in legend.keys()), "lower right")

    # This should prevent axes from different plots to overlap etc
    plt.tight_layout()

    # Add big axis descriptions for all plots
    axis_description = fig.add_subplot(111, frameon=False)
    plt.tick_params(labelcolor="none", top=False, bottom=False, left=False, right=False)
    axis_description.set_xlabel("Utilized cores", labelpad=10)
    axis_description.set_ylabel(
        "Throughput (queries / s) per core\n(relative to single-threaded without scheduler)", labelpad=20
    )

    result_plot_file = os.path.join(os.getcwd(), "benchmark_comparison_scaleup." + args.format)
    plt.savefig(result_plot_file, bbox_inches="tight")
    print("Plot saved as: " + result_plot_file)


def plot(args):
    max_cores = 0
    results = {}

    for result_dir in args.results:
        # The label that will appear in the plot. It consists of the --result-name
        # that was passed to benchmark_multithreaded.py
        label = result_dir.rstrip("/").split("/")[-1]
        results[label] = {}
        one_result = results[label]

        for _, _, files in os.walk(result_dir):
            json_files = [f for f in files if f.split(".")[-1] == "json"]
            # Add the results in sorted order (low core count -> high core count) for plotting later
            # The lambda extracts the number of cores from the filename
            for file in sorted(json_files, key=lambda filename: int(filename.split("-")[0])):
                with open(os.path.join(result_dir, file), "r") as json_file:
                    json_data = json.load(json_file)
                cores = json_data["context"]["cores"]
                if cores > max_cores:
                    max_cores = cores
                    utilized_cores_per_numa_node = json_data["context"]["utilized_cores_per_numa_node"]
                for benchmark in json_data["benchmarks"]:
                    name = benchmark["name"]
                    items_per_second = benchmark["items_per_second"]
                    if name not in one_result:
                        one_result[name] = {"cores": [], "items_per_second": []}
                    if cores == 0:
                        one_result[name]["singlethreaded"] = items_per_second
                    else:
                        one_result[name]["cores"].append(cores)
                        one_result[name]["items_per_second"].append(items_per_second)

    numa_borders = [sum(utilized_cores_per_numa_node[:x]) for x in range(1, len(utilized_cores_per_numa_node))]

    # Transform the results for plotting, since we create one subplot per TPC-H query
    flipped_results = {}
    for label, one_result in results.items():
        for tpch, data in one_result.items():
            if tpch not in flipped_results:
                flipped_results[tpch] = {}
            flipped_results[tpch][label] = data

    n_rows_cols = get_subplot_row_and_column_count(len(flipped_results))

    plot_performance(flipped_results, n_rows_cols, numa_borders, max_cores)
    plot_scaleup(flipped_results, n_rows_cols, numa_borders, max_cores)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    plot(args)
