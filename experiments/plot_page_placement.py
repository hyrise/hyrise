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
            [keys, values] = attributes_from_filename(filename)
            row = list(values)
            if not columns:
                columns = list(keys)
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
    # Single numa node (numactl)
    # pattern = r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{6})_N(?P<thread_binding>\d+)m(?P<memory_binding>\d+)s(?P<scale_factor>\d+)t(?P<exec_time>\d+)cl(?P<clients>\d+)c(?P<cores>\d+)(?P<mode>\w+)_rn(?P<rnodes>[\d\-]*)(?:rw(?P<rweights>[\d\-]*))?"
    pattern = r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{6})_N(?P<thread_binding>[\d\-]*)m(?P<memory_binding>[\d\-]*)s(?P<scale_factor>\d+)t(?P<exec_time>\d+)cl(?P<clients>\d+)c(?P<cores>\d+)(?P<mode>\w+)_rn(?P<rnodes>[\d\-]*)(?:rw(?P<rweights>[\d\-]*))?"

    match = re.match(pattern, input_str)
    if match:
        parsed_data = match.groupdict()
        return parsed_data.keys(), parsed_data.values()
    print(input_str)
    sys.exit("Failed: file name does not match known pattern.")

if __name__ == "__main__":
    is_weighted = False
    is_round_robin = False
    is_print = False
    
    if len(sys.argv) == 2:
        is_weighted = True
        is_round_robin = True
    elif len(sys.argv) == 3:
        if sys.argv[2] == "w":
            is_weighted = True
        elif sys.argv[2] == "r":
            is_round_robin = True
        elif sys.argv[2] == "print":
            is_print = True
    else:
        print("Usage: python script.py <directory_path> [type (w: weighted, r: round robin]")
        sys.exit(1)

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
    df['weight_left_percent'] = df['weight_left']*100
    df['weight_left_percent'] = df['weight_left_percent'].astype(int)
    df["placement"] = df["rnodes"]
    placement_map = {
        # Assumung threads are pinned to node 1
        "0": "Remote CPU",
        "1": "CPU",
        "1,2": "CPU+1Blade",
        "1,2,3": "CPU+2Blade",
        "1,2,3,4": "CPU+3Blade",
        "1,2,3,4,5": "CPU+4Blade",
        "2": "1$\\times$CXL",
        "2,3": "2$\\times$CXL",
        "2,3,4": "3$\\times$CXL",
        "2,3,4,5": "4$\\times$CXL"
    }
    df["placement"] = df["placement"].replace(placement_map)
    order_dict = mapping_dict = {'L': 0, 'R': 1, 'L+1B': 2, 'L+2B': 3, 'L+3B': 4, 'L+4B' : 5,
      '1B': 6, '2B': 7, '3B': 8, '4B' : 9
    }
    df['order'] = df['placement'].map(mapping_dict)
    df = df.sort_values(by=["order"])

    if is_print:
      print(df)

    plt.rcParams.update({
        'text.usetex': True,
        'font.family': 'serif',
        'text.latex.preamble': r'\usepackage{libertine}'
    })

    if is_round_robin:
        df_round_robin = df[df["rweights"] == ""]
        print(df_round_robin)
        # Plot round robin interleaving
        plt.figure(figsize=(1, 1.3))
        ax = sns.barplot(data=df_round_robin, x="placement", y="avg_throughput_client", color=hpi_col[0])
        plt.ylim(0)
        # ax.yaxis.set_major_locator(ticker.MultipleLocator(10))
        # ax.yaxis.set_minor_locator(ticker.MultipleLocator(2.5))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(2.5))
        ax.grid(axis='y', which='minor', linestyle='-', alpha=0.2)
        ax.grid(axis='y', which='major', linestyle='-', alpha=0.6)
        ax.set_axisbelow(True)
        plt.xlabel("Placement", labelpad=1)
        plt.ylabel("Throughput\n[Queries/h/client]")
        plt.title("0: local | 1: remote socket | 2,3,4,5: CXL blades")
        plt.title("")
        plt.xticks(rotation = 90)
        # ax.set_xticks([])
        # ax.set_xlabel("")
        # for bar, label in zip(ax.patches, df_round_robin["placement"]):
        #     x = bar.get_x() + bar.get_width() / 2
        #     y = bar.get_height()
        #     ax.text(x+0.1, 0.5, label, ha='center', va='bottom', rotation=90, fontsize=9)
        plt.savefig(directory_path + "/page_placement_round_robin.pdf", bbox_inches="tight", pad_inches=0)
        plt.close('all')

    if is_weighted:
        y_key = "avg_throughput_client"
        df_weighted = df[(df["rweights"] != "")]
        df_baseline_local = df[df["rnodes"] == "1"]
        assert len(df_baseline_local) == 1
        baseline_local = df_baseline_local[y_key].values[0]
        print(baseline_local)

        baseline_remote_socket = df[df["rnodes"] == "1"]
        assert len(baseline_remote_socket) == 1
        baseline_remote_socket = baseline_remote_socket[y_key].values[0]

        # df_weighted["x_val"] = df_weighted["weight_left_percent"].astype(str) + " (" + df_weighted["rweights"] + ")"
        df_weighted["x_val"] = df_weighted["rweights"]
        df_weighted = df_weighted.sort_values(by=["weight_left"])
        df_weighted["pages_left"] = df_weighted["pages_left"].astype(int)
        df_weighted["pages_right"] = df_weighted["pages_right"].astype(int)
        df_weighted = df_weighted[~df_weighted["pages_left"].isin([13, 16])]
        df_weighted = df_weighted[~df_weighted["pages_right"].isin([13, 16])]
        # df_weighted["x_val"] = pd.Categorical(df_weighted["x_val"], categories=df_weighted["x_val"], ordered=True)
        print(df_weighted)

        # Plot weighted interleaving
        # assert len(df_weighted["rnodes"].unique()) == 1
        fig = plt.figure(figsize=(3.4,1.9))
        ax = sns.lineplot(data=df_weighted, x="x_val", y=y_key, marker="X", markersize=5, color=hpi_col[0])
        plt.ylim(0, baseline_local + 2)
        plt.ylim(bottom=0)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(2.5))
        ax.grid(axis='y', which='minor', linestyle='-', alpha=0.2)
        ax.grid(axis='y', which='major', linestyle='-', alpha=0.6)
        ax.margins(x=0)
        # sns.barplot(data=df_weighted, x="x_val", y="avg_throughput")
        plt.axhline(y=baseline_local, color='gray', linestyle='--', label='Data in CPU memory (baseline)', linewidth=1)
        # plt.axhline(y=baseline_remote_socket, color='gray', linestyle='--', label='All Remote CPU')
        # plt.xlabel("Page placement\n[\% (pages CPU memory:pages CXL memory)]", labelpad=1)
        plt.xlabel("Page placement ratios\n[CPU memory:CXL memory]", labelpad=1)
        plt.ylabel("Throughput\n[Queries/h/client]")
        # ax.yaxis.set_label_coords(-0.13, 0.2)
        plt.title("Interleaved across local (node 0) and 1 Blade (node 2)")
        plt.title("")
        plt.xticks(rotation = 90)
        # 33.5 / 38.9 = 0.87
        ax.axvline("2:1", color=hpi_col[1], linestyle=":", linewidth=1.0, label='$\\geq0.85\\times$baseline')
        # plt.legend(loc='best',ncol=2)
        legend = fig.legend(title="", loc="upper center", ncol=1, bbox_to_anchor=(0.585, 0.65),
          columnspacing=0.5,
          handlelength=0.8,
          handletextpad=0.2,
          labelspacing=0,
          borderpad=0.2
        )
        legend.get_frame().set_facecolor('white')
        legend.get_frame().set_alpha(1.0)
        plt.tight_layout()
        plt.savefig(directory_path + "/page_placement_weighted.pdf", bbox_inches="tight", pad_inches=0)
        plt.close('all')