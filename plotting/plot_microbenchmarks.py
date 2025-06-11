#!/usr/bin/env python3

import sys
import json
import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from adjustText import adjust_text

def extract_case_and_technique(name):
    # Remove prefix
    if name.startswith("ReductionBenchmarks/"):
        name = name[len("ReductionBenchmarks/"):]
    # Extract case and technique
    # e.g. "WorstCaseSemiJoin", "BestCasePrototype/18/1"
    match = re.match(r"(WorstCase|BestCase|BadCase)([A-Za-z]+)(.*)", name)
    if match:
        case = match.group(1)
        technique = match.group(2)
        rest = match.group(3)
        # If there is a suffix like /18/2, append it to the technique
        if rest:
            # Remove leading slash if present
            rest = rest.lstrip("/")
            technique = f"{technique}{rest and rest.replace('/', '') and rest.replace('/', '/')}"
    else:
        case = None
        technique = None
    return pd.Series([case, technique])

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <microbenchmarks.json>")
        sys.exit(1)

    json_path = sys.argv[1]

    with open(json_path, "r") as f:
        data = json.load(f)

    benchmarks = data["benchmarks"]
    df = pd.DataFrame(benchmarks)

    # Drop rows without 'name'
    df = df[df["name"].notnull()]

    # Extract 'case' and 'technique'
    df[["case", "technique"]] = df["name"].apply(extract_case_and_technique)

    # Calculate runtime in ms and reduction rate
    df["runtime_ms"] = df["real_time"] / 1e6
    df["reduction_rate"] = 1 - (df["output_count"] / df["input_count"])

    # Keep only relevant columns
    df = df[["input_count", "output_count", "case", "technique", "runtime_ms", "reduction_rate"]]

    # Plot dual axis bar charts for each case
    for case in ["WorstCase", "BadCase", "BestCase"]:
        case_df = df[df["case"] == case].copy()
        if case_df.empty:
            continue

        min_bar_height = 0.01
        plot_reduction_rate = case_df["reduction_rate"].replace(0, min_bar_height)

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()

        x = np.arange(len(case_df["technique"]))
        bar_width = 0.4

        ax1.bar(x - bar_width/4, case_df["runtime_ms"], color="skyblue", width=bar_width/2, label="Runtime [ms]", align='center')
        ax2.bar(x + bar_width/4, plot_reduction_rate, color="salmon", width=bar_width/2, label="Reduction Rate", align='center')

        ax1.set_ylabel("Runtime [ms]", color="skyblue")
        ax2.set_ylabel("Reduction Rate", color="salmon")
        ax1.set_xlabel("Technique")
        plt.title(f"{case}: Runtime and Reduction Rate by Technique")
        ax1.set_xticks(x)
        ax1.set_xticklabels(case_df["technique"], rotation=45, ha='right')

        if case == "BestCase":
            ax2.set_ylim(0.99, 1)  # or 0.2, depending on your data
        else:
            ax2.set_ylim(0, 1)

        plt.tight_layout()
        plt.savefig(f"{case}_runtime_reduction_rate.pdf")
        plt.close()

    # Plot scatterplots for each case: runtime_ms (x) vs reduction_rate (y)
    for case in ["WorstCase", "BadCase", "BestCase"]:
        case_df = df[df["case"] == case].copy()
        if case_df.empty:
            continue

        plt.figure(figsize=(8, 6))
        plt.scatter(case_df["runtime_ms"], case_df["reduction_rate"], color="salmon")
        texts = []
        for i, row in case_df.iterrows():
            texts.append(
                plt.text(row["runtime_ms"], row["reduction_rate"], row["technique"], fontsize=9, ha='right', va='bottom')
            )
        adjust_text(texts, arrowprops=dict(arrowstyle='-', color='gray', lw=0.5))
        plt.xlabel("Runtime [ms]")
        plt.ylabel("Reduction Rate")
        plt.title(f"{case}: Reduction Rate vs Runtime")
        plt.tight_layout()
        plt.savefig(f"{case}_scatter_runtime_vs_reduction_rate.pdf")
        plt.close()

if __name__ == "__main__":
    main()
