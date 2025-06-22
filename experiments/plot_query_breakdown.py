#!/usr/bin/env python3

import os
import re
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns

def load_json_files(directory):
    if not os.path.isdir(directory):
        print(f"The provided path '{directory}' is not a directory.")
        return

    columns = None
    rows = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            if ".csv" in filename:
                continue
            [keys, values] = attributes_from_filename(filename)
            if not columns:
                columns = list(keys)
                print(columns)
                columns.append("query_name")
                columns.append("avg_duration_ns")

            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    print(f"Loaded JSON file: {filename}")
                    [query_names, avg_query_durations] = throughput_per_query(data)
                    for query_idx, query_name in enumerate(query_names):
                        row = list(values)
                        # print("qidx: ",query_idx)
                        row.append(query_name)
                        row.append(avg_query_durations[query_idx])
                        rows.append(row)
            except Exception as e:
                print(f"Failed to load JSON file '{filename}': {e}")
    # print(rows)
    df = pd.DataFrame(rows, columns=columns)
    df = df.sort_values(by=["rweights", "rnodes"])
    return df

def throughput_per_query(bench_json):
    bench_duration = None
    query_names = []
    avg_query_durations = []
    for benchmark in bench_json["benchmarks"]:
        query_names.append(benchmark["name"])
        if not bench_duration:
            bench_duration = benchmark["duration"]
        else:
            assert bench_duration == benchmark["duration"]
        item_durations = []
        for run in benchmark["successful_runs"]:
            item_durations.append(run["duration"])
        avg_query_durations.append(np.average(item_durations))
    return (query_names, avg_query_durations)

def attributes_from_filename(input_str):
    # print(input_str)
    pattern = r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{6})_N(?P<thread_binding>[\d\-]*)m(?P<memory_binding>[\d\-]*)s(?P<scale_factor>\d+)t(?P<exec_time>\d+)cl(?P<clients>\d+)c(?P<cores>\d+)(?P<mode>\w+)_rn(?P<rnodes>[\d\-]*)(?:rw(?P<rweights>[\d\-]*))(?:fc(?P<fixed_columns>[\d\-]*))?"

    match = re.match(pattern, input_str)
    if match:
        parsed_data = match.groupdict()
        return parsed_data.keys(), parsed_data.values()
    sys.exit("Failed: file name does not match known pattern.")

if __name__ == "__main__":
    is_print = False
    
    if len(sys.argv) < 2:
        print("Usage: python script.py <directory_path> [print]")
    if len(sys.argv) >= 3 and sys.argv[2] == "print":
        is_print = True

    directory_path = sys.argv[1]
    df = load_json_files(directory_path)
    df["rnodes"] = df["rnodes"].astype(str).str.replace("-",",")
    df["rweights"] = df["rweights"].astype(str).str.replace("-",":")
    df['fixed_columns'] = df['fixed_columns'].astype(int)
    df["avg_duration_s"] = df["avg_duration_ns"] / 1000000000
    df = df.sort_values("fixed_columns")
    df['fixed_columns'] = df['fixed_columns'].astype(str)

    if is_print:
      print(df)

    y_key = "avg_duration_s"
    x_key = "fixed_columns"

    unique_queries = sorted(df["query_name"].unique())
    num_queries = len(unique_queries)
    cols = 3  # Number of subplots per row
    rows = (num_queries // cols) + (num_queries % cols > 0)  # Calculate required rows

    fig, axes = plt.subplots(rows, cols, figsize=(8, 2.5 * rows), sharex=False, sharey=False)

    # Flatten axes array for easy iteration
    axes = axes.flatten()

    for i, query in enumerate(unique_queries):
        ax = axes[i]
        subset = df[df["query_name"] == query]

        sns.lineplot(data=subset, x=x_key, y=y_key, ax=ax, linewidth=2.5, marker="o")
        ax.set_title(query)
        ax.set_xlabel("# columns fixed in local memory")
        ax.set_ylabel("Latency [seconds]")
        ax.grid(axis='y', linestyle=':', alpha=0.8)
        ax.set_ylim(0, None)

    # Hide any unused subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.savefig(directory_path + "/page_column_placement_query_breakdown.pdf", bbox_inches="tight", pad_inches=0)
    plt.close('all')