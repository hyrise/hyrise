#!/usr/bin/env python3

# TODO

from adjustText import adjust_text
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

if len(sys.argv) != 2:
    exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as file:
    data = json.load(file)

if not "segments" in data:
    exit("JSON file does not contain segment access counters. Did you generate it with --metrics?")

df = pd.json_normalize(data["segments"])

counters = ["monotonic_accesses", "point_accesses", "random_accesses", "sequential_accesses", "dictionary_accesses"]

df["all_accesses"] = 0
for counter in counters:
    df["all_accesses"] += df[counter]

assert df["moment"].iloc[0] == "init"

groupby = ["table_name", "column_id"]
agg = {"column_name": "max", "estimated_size_in_bytes": "max", "all_accesses": "sum"}

init_snapshot = df[df["snapshot_id"] == 0].groupby(by=groupby).agg(agg)

last_snapshot = df[df["snapshot_id"] == df["snapshot_id"].max()].groupby(by=groupby).agg(agg)

assert len(init_snapshot) == len(last_snapshot)

totals = last_snapshot.copy()
totals["all_accesses"] = last_snapshot["all_accesses"] - init_snapshot["all_accesses"] + 1

totals["estimated_size_in_bytes_log"] = np.log(totals["estimated_size_in_bytes"])
totals["all_accesses_log"] = np.log(totals["all_accesses"])
totals["estimated_size_in_bytes_z"] = (
            (totals["estimated_size_in_bytes_log"] - totals["estimated_size_in_bytes_log"].mean())
            / totals["estimated_size_in_bytes_log"].std()
        ).abs()
totals["all_accesses_z"] = (
            (totals["all_accesses_log"] - totals["all_accesses_log"].mean())
            / totals["all_accesses_log"].std()
        ).abs()
totals["max_z"] = totals[["estimated_size_in_bytes_z", "all_accesses_z"]].max(axis=1)

# TODO add table_name as color or letter instead of marker

fig, ax = plt.subplots()
ax.margins(x=.25, y=.25)

totals.plot(kind="scatter", x="estimated_size_in_bytes", y="all_accesses", ax=ax, logx=True, logy=True)

annotations = []
x = []
y = []
for v in totals.iterrows():
    x.append(v[1].estimated_size_in_bytes)
    y.append(v[1].all_accesses)
    if v[1].max_z > 1.2:
        annotations.append(plt.text(v[1].estimated_size_in_bytes, v[1].all_accesses, v[1].column_name))

adjust_text(annotations, x, y=y, expand_text=(1.2, 1.2), expand_points=(1.1, 1.1), arrowprops=dict(arrowstyle='-', color='grey'))

fig.savefig("segment_value.png")
