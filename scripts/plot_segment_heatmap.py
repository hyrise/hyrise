#!/usr/bin/env python3

# Given a benchmark result json that was generated with --metrics, this script plots a heatmap of the different
# segment's access counters.

import copy
import json
import matplotlib as mpl
import matplotlib.colors as colors
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
import os
import pandas as pd
import sys

mpl.rcParams.update({'font.size': 16})

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

# Use this to filter by table name(s)
# df = df[df["table_name"].isin(['lineitem', 'orders'])]

df = df.sort_values(by=["table_name", "column_id", "chunk_id", "snapshot_id"]).reset_index()

counters = ["monotonic_accesses", "point_accesses", "random_accesses", "sequential_accesses", "dictionary_accesses"]

df["all_accesses"] = 0
for counter in counters:
    df["all_accesses"] += df[counter]

# TODO this breaks if a segment is added later on
df["all_accesses_diff"] = df["all_accesses"] - df["all_accesses"].shift(1)
df["table_and_column_name"] = df["table_name"] + "." + df["column_name"]

init_values = df.loc[df["snapshot_id"] == 0].index
df.loc[init_values, ["all_accesses_diff"]] = df.loc[init_values, "all_accesses"]


def plot(snapshot_id):
    if snapshot_id is not None:
        df_snapshot = df[df["snapshot_id"] == snapshot_id]
        moment = df_snapshot["moment"].iloc[0]
    else:
        df_snapshot = df[df["snapshot_id"] > 0]
        moment = "All accesses (without benchmark initialization)"

    df_snapshot = df_snapshot.sort_values("table_and_column_name").reset_index()

    # For snapshot_id == None, this sums all values
    piv = pd.pivot_table(df_snapshot, index="table_and_column_name", columns="chunk_id", values="all_accesses_diff")
    piv = piv.reindex(index=piv.index[::-1])

    # Remove all columns that have not been touched at all
    if snapshot_id is not None:
        piv = piv[(piv.T > 0).any()].dropna(axis=1, how="all")
        if piv.empty:
            print(f"No modifications to access counters seen at '{moment}' - skipping this snapshot")
            return

    # Add per-column average
    piv['Ø'] = piv.mean(numeric_only=True, axis=1)

    # Add per-chunk average
    per_chunk_accesses = {}
    for table_and_column_name, row in piv.iterrows():
        table = table_and_column_name.split('.')[0]
        if not table in per_chunk_accesses:
            per_chunk_accesses[table] = []
        per_chunk_accesses[table].append(row)
    for table, accesses in per_chunk_accesses.items():
        piv.loc[f"{table} Ø"] = np.mean(accesses, axis=0).tolist()
    piv = piv.sort_index()

    legend_width = 0.2
    fig, ax = plt.subplots(figsize=(0.25 * piv.shape[1] + legend_width, 0.25 * piv.shape[0]))
    ax.set_aspect("equal")
    ax.axis("off")

    cmap = copy.copy(mpl.cm.get_cmap("inferno"))
    cmap.set_bad("black", 1.0)

    heatmap = plt.pcolor(
        piv, edgecolors="white", linewidth=2, norm=colors.LogNorm(vmin=1, vmax=np.nanmax(piv.to_numpy())), cmap=cmap
    )
    fig.tight_layout(pad=0)

    # Add average marker to cells where needed
    for y in range(piv.shape[0]):
        for x in range(piv.shape[1]):
            if ('Ø' in str(piv.columns[x]) or 'Ø' in str(piv.index[y])) and piv.iloc[y, x] > 1:
                plt.text(x + 0.5, y + 0.5, 'Ø',
                         horizontalalignment='center',
                         verticalalignment='center',
                         color='darkgrey'
                         )
        if 'Ø' in str(piv.index[y]):
            ax.axhline(y = y, xmin = 0, xmax = piv.shape[1], color='lightgrey')

    ax.axis("on")
    ax.set_title(moment)
    ax.set_xticks(np.arange(piv.shape[1]) + 0.5, minor=False)
    ax.set_xticklabels(piv.columns.values.astype(str), rotation=270)
    ax.set_xlabel("Chunk ID")
    ax.set_yticks(np.arange(piv.shape[0]) + 0.5, minor=False)
    ax.set_yticklabels(piv.index.values.astype(str))
    ax.set_ylabel("Table and colum name")

    # Create a legend
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size=legend_width, pad=0.05)
    cbar = plt.colorbar(heatmap, cax=cax)

    # Save and close
    if snapshot_id is not None:
        filename = f"{snapshot_id:03}_{normalize_filename(moment)}.png"
    else:
        filename = "total_without_init.png"

    plt.savefig(f"{directory}/{filename}", bbox_inches="tight")
    plt.close("all")


for snapshot_id in df["snapshot_id"].unique():
    plot(snapshot_id)
plot(None)
