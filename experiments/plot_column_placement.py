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

hpi_col = ["#f5a700","#dc6007","#b00539", "#6b009c", "#006d5b", "#0073e6", "#e6007a", "#00C800", "#FFD500", "#0033A0" ]

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
            row = list(values)
            if not columns:
                columns = list(keys)
                print(columns)
                columns.append("avg_throughput")

            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    print(f"Loaded JSON file: {filename}")
                    row.append(throughput(data))
                    rows.append(row)
            except Exception as e:
                print(f"Failed to load JSON file '{filename}': {e}")
    df = pd.DataFrame(rows, columns=columns)
    df = df.sort_values(by=["rweights", "rnodes"])
    return df

def throughput(bench_json):
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
    # print(query_names)
    # print(avg_query_durations)
    # return bench_duration / np.average(avg_query_durations)
    return 60*60*1000000000 / np.average(avg_query_durations) # query_per_hour

def attributes_from_filename(input_str):
    print(input_str)
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
    df["avg_throughput_client"] = df["avg_throughput"] / df["clients"].astype(int)
    df["pages_left"] = df["rweights"].apply(lambda x: int(x.split(":")[0]) if x != "" else None)
    df["pages_right"] = df["rweights"].apply(lambda x: int(x.split(":")[1]) if x != "" else None)
    df["weight_left"] = df["pages_left"] / (df["pages_left"] + df["pages_right"])
    df['weight_left'] = df['weight_left'].round(2)
    df['weight_left'].fillna(1, inplace=True)
    df['fixed_columns'] = df['fixed_columns'].astype(int)
    df = df[(df['fixed_columns'] <= 16) | (df['fixed_columns'] == 100)]
    df = df.sort_values(by="fixed_columns")

    if is_print:
      print(df)

    y_key = "avg_throughput_client"
    df_sub = df[(df["fixed_columns"] != 100) & (df["fixed_columns"] != 0)]
    if is_print:
      print(df_sub)
    
    df_baseline_local = df[df["fixed_columns"] == 100]
    assert len(df_baseline_local) == 1
    baseline_local = df_baseline_local[y_key].values[0]
    # baseline from page placement measurement with all data on CXL
    baseline_local = 38.903065947817524
    print(baseline_local)
    
    df_baseline_cxl = df[df["fixed_columns"] == 0]
    assert len(df_baseline_cxl) == 1
    baseline_cxl = df_baseline_cxl[y_key].values[0]
    print(baseline_cxl)

    ### Plot
    df_sub["fixed_columns"] = df_sub["fixed_columns"].astype(str)

    plt.rcParams.update({
        'text.usetex': True,
        'font.family': 'serif',
        'text.latex.preamble': r'\usepackage{libertine}'
    })

    fig = plt.figure(figsize=(3.4, 1.8))
    # df_weighted["x_val"] = df_weighted["weight_left"].astype(str) + " (" + df_weighted["rweights"] + ")"
    # df_weighted = df_weighted.sort_values(by=["x_val"])
    # print(df_weighted)
    # # Plot weighted interleaving
    # # assert len(df_weighted["rnodes"].unique()) == 1
    ax = sns.lineplot(data=df_sub, x="fixed_columns", y=y_key, marker="X", markersize=5, color=hpi_col[0])
    plt.ylim(0, baseline_local + 2)
    # plt.ylim(bottom=0, top=baseline_cxl + 10)
    # ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(2.5))
    ax.grid(axis='y', which='minor', linestyle='-', alpha=0.2)
    ax.grid(axis='y', which='major', linestyle='-', alpha=0.6)
    # ax.margins(x=0)
    # # sns.barplot(data=df_weighted, x="x_val", y="avg_throughput")
    plt.axhline(y=baseline_local, color='gray', linestyle='--', label='Data in\nCPU\nmemory\n(baseline)', linewidth=1)
    # plt.axhline(y=baseline_cxl, color='gray', linestyle='--', label='All CXL')
    plt.xlabel("Columns in CPU memory [Top-$\\emph{n}$ by access frequency]")
    plt.ylabel("Throughput\n[Queries/h/client]")
    # ax.yaxis.set_label_coords(-0.08, 0.48)
    # 33.5 / 38.9 = 0.87
    ax.axvline("9", color=hpi_col[1], linestyle=":", linewidth=1.0, label='$\\geq0.85\\times$\nbaseline')
    # plt.title("Interleaved across local (node 0) and 1 Blade (node 2)")


    columns = ['l_orderkey', 'l_partkey', 'l_shipdate', 'l_suppkey', 'l_quantity',
     'l_discount', 'l_shipmode', 'l_returnflag', 'o_orderkey', 'l_extendedprice',
     'l_linestatus', 'l_tax', 'o_custkey', 'o_orderdate', 'ps_partkey',
     'ps_suppkey', 'c_custkey', 'l_receiptdate', 'p_size', 'l_commitdate']
    columns = [columns[0]] + [f'+ {col}' for col in columns[1:]]
    # Replace column count by column names
    # tick_labels = [tick.get_text() for tick in ax.get_xticklabels()]
    # new_labels = [f"{columns[int(x)-1]} ({int(x)})" for x in tick_labels if int(x) < len(columns)]
    # ax.set_xticklabels(new_labels, rotation=45)
    # plt.xlabel("Top-N frequently accessed columns in CPU memory")

    # Add column text in graph
    xtick_vals = ax.get_xticks()
    for i, x in enumerate(xtick_vals):
        if i < len(columns):
            ax.text(x, 1, columns[i], 
                    rotation=90, ha='center', va='bottom', fontsize=8)
    legend = fig.legend(title="", loc="upper center", ncol=1, bbox_to_anchor=(1.08, 0.93),
    # legend = fig.legend(title="", loc="upper center", ncol=1, bbox_to_anchor=(1.155, 0.8),
    # legend = fig.legend(title="", loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.2),
          columnspacing=0.5,
          handlelength=0.8,
          handletextpad=0.4,
          labelspacing=0.8,
          borderpad=0.2
    )
    legend.get_frame().set_facecolor('white')
    legend.get_frame().set_alpha(1.0)
    plt.tight_layout()
    plt.savefig(directory_path + "/page_column_placement.pdf", bbox_inches="tight", pad_inches=0)
    plt.close('all')