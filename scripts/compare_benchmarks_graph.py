#!/usr/bin/env python3

import json
import matplotlib
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import math
import numpy as np
import pandas as pd
import re
import sys

matplotlib.rcParams.update({'font.size': 16})

if len(sys.argv) < 2:
    exit("Usage: " + sys.argv[0] + " reference_benchmark.json benchmark2.json")

df = pd.DataFrame()

for path in sys.argv[1:]:
    with open(path) as file:
        items = json.load(file)["benchmarks"]
        for item in items:
            if len(item["unsuccessful_runs"]) > 0:
                print("Ignoring unsuccessful runs")

            item_df = pd.json_normalize(item["successful_runs"])
            item_df["item"] = item["name"]
            item_df["file"] = path.replace('.json', '')
            df = pd.concat([df, item_df])

normalize = True
fliers = False

# Normalize to milliseconds
df['duration'] /= 1e6

if normalize:
    df['normalized_duration'] = df['duration'] / df.groupby('item')['duration'].transform('mean')
    df.drop(df.columns.difference(['file', 'item', 'normalized_duration']), 1, inplace=True)
else:
    df.drop(df.columns.difference(['file', 'item', 'duration']), 1, inplace=True)

df = df.set_index(['file', 'item'], append=True).unstack('item')
df.columns = df.columns.droplevel()

num_items = len(df.columns)
fig, axes = plt.subplots(4, 6, figsize=(15, 8), sharey=(normalize and not fliers)) # TODO: hard-coded 22
plots = df.boxplot(by=["file"], rot=90, ax=axes.flat[:num_items], showfliers=fliers, flierprops={'marker': '.', 'markeredgecolor': 'None', 'markerfacecolor': (0,0,0,0.2)}, showmeans=True, meanprops={'marker': 'x', 'markeredgecolor': 'black'}, widths=.5, return_type='dict')

colors = plt.get_cmap('tab10') 

for key in plots.keys():
    plot = plots[key]
    for items in plot.values():
        for i, item in enumerate(items):
            color_idx = math.floor(i / (len(items) / (len(sys.argv) - 1))) / (len(sys.argv))
            plt.setp(item, color=colors(color_idx), linewidth=2)


def yformat(y, pos):
    diff = y - 1
    if diff == 0:
        return 0
    out = "" if diff < 0 else "+"
    return out + f"{diff:.0%}"

for i, ax in enumerate(axes.flat):
    if i >= num_items:
        ax.axis('off')
        continue

    ax.set_xlabel(None)
    if i >= 16 and i <= 21: # TODO get subplot dimension
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: re.sub('^[0-9]+_', '', sys.argv[x]).replace('_', ' ').replace('.json', '')))
    else:
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: ""))
    ax.grid(None, axis='x')

    if normalize:
        ax.set_yscale('log')
        ax.yaxis.set_major_locator(ticker.LogLocator(numticks=6, subs=[-.1, .1]))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(yformat))
        ax.yaxis.set_minor_locator(ticker.NullLocator())
        ax.axhline(1.0, linewidth=1, color="k")

        # yabs_max = abs(max(ax.get_ylim(), key=abs))
        # ax.set_ylim(ymin=1/yabs_max, ymax=yabs_max)

fig.suptitle(None)
if normalize:
    ylabel = "Runtime relative to average across all configurations"
else:
    ylabel = "Runtime in milliseconds"
ylabel += "\n(lower is better, x marks the arithmetic mean)"
fig.text(0.02, 0.5, ylabel, ha="center", va="center", rotation=90)

fig.tight_layout()

plt.subplots_adjust(wspace=.1, hspace=.5 if normalize else .8)

plt.savefig(f"boxplot.pdf", bbox_inches="tight")