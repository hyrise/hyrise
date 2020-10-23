#!/usr/bin/env python3

# Given a benchmark result json that was generated with --metrics, this script plots a heatmap of the different
# segment's access counters.

import json
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
import os
import pandas as pd
import sys


def normalize_filename(s):
    validsymbols = "-"
    out = ""
    for c in s:
        if str.isalpha(c) or str.isdigit(c) or (c in validsymbols):
            out += c
        else:
            out += "_"
    return out


if len(sys.argv) != 2:
    exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as file:
    data = json.load(file)

directory = "segment_heatmaps"
if not os.path.exists(directory):
    os.mkdir(directory)

if not "segments" in data:
    exit("JSON file does not contain segment access counters. Did you generate it with --metrics?")

df = pd.json_normalize(data["segments"])

df = df.sort_values(by=["table_name", "column_id", "chunk_id", "snapshot_id"]).reset_index()

df["all_counters"] = df["monotonic_accesses"] + df["point_accesses"] + df["random_accesses"] + df["sequential_accesses"]
df["all_counters_diff"] = df["all_counters"] - df["all_counters"].shift(1)
df["table_and_column_name"] = df["table_name"] + "." + df["column_name"]

init_values = df.loc[df["snapshot_id"] == 0].index
df.loc[init_values, ["all_counters_diff"]] = df.loc[init_values, "all_counters"]

for snapshot_id in df["snapshot_id"].unique():
    df_snapshot = df[df["snapshot_id"] == snapshot_id].sort_values("table_and_column_name").reset_index()
    moment = df_snapshot["moment"].iloc[0]

    piv = pd.pivot_table(df_snapshot, index="table_and_column_name", columns="chunk_id", values="all_counters_diff")
    piv = piv.reindex(index=piv.index[::-1])

    # Remove all columns that have not been touched at all
    piv = piv[(piv.T > 0).any()].dropna(axis=1, how="all")
    if piv.empty:
        print(f"No modifications to access counters seen at '{moment}' - skipping this snapshot")
        continue

    legend_width = 0.2
    fig, ax = plt.subplots(figsize=(0.25 * piv.shape[1] + legend_width, 0.25 * piv.shape[0]))
    ax.set_aspect("equal")
    ax.axis("off")

    heatmap = plt.pcolor(piv, edgecolors="white", linewidth=2)
    fig.tight_layout(pad=0)

    ax.axis("on")
    ax.set_title(moment)
    ax.set_yticks(np.arange(piv.shape[0]) + 0.5, minor=False)
    ax.set_yticklabels(reversed(sorted(piv.index)))

    # Create a legend
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size=legend_width, pad=0.05)
    cbar = plt.colorbar(heatmap, cax=cax)

    # Save and close
    filename = f"{snapshot_id:03}_{normalize_filename(moment)}.png"
    plt.savefig(f"{directory}/{filename}", bbox_inches="tight")
    plt.clf()
