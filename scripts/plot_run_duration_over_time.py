#!/usr/bin/env python3

# Takes a single benchmark output JSON and plots the duration of item executions over time

import json
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys

if(len(sys.argv) != 2):
    print('Usage: ' + sys.argv[0] + ' benchmark.json')
    exit()

with open(sys.argv[1]) as json_file:
    data_json = json.load(json_file)['benchmarks']

# Build flat list of {name, begin, duration} entries for every benchmark item run
data = []
benchmark_names = []
for benchmark_json in data_json:
	name = benchmark_json['name']
	benchmark_names.append(name)
	for run_json in benchmark_json['runs']:
		begin = run_json['begin']
		duration = run_json['duration']
		data.append({'name': name, 'begin': begin / 1e9, 'duration': duration})

df = pd.DataFrame(data)

# Set the colors
benchmark_names.sort()  # Sort the benchmarks for a deterministic color mapping
name_to_color = {}
prop_cycle = plt.rcParams['axes.prop_cycle']
default_colors = prop_cycle.by_key()['color']

name_to_color[benchmark_names[0]] = default_colors[0]
colors = np.where(df['name'] == benchmark_names[0], default_colors[0], '-')
for i in range(1, len(benchmark_names)):
	color = default_colors[i % len(default_colors)]
	name_to_color[benchmark_names[i]] = color
	colors[df['name'] == benchmark_names[i]] = color

# Plot combined graph
ax = df.reset_index().plot(kind='scatter', x='begin', y='duration', c=colors, s=.3, figsize=(12, 9))
ax.set_xlabel('Seconds since start')
ax.set_ylabel('Execution duration [ns]')
ax.grid(True, alpha=.3)

# Add legend to combined graph
handles = []
for name, color in name_to_color.items():
	handles.append(mpatches.Patch(label=name, color=color))
plt.legend(handles=handles)

# Write combined graph to file
basename = sys.argv[1].replace('.json', '')
plt.savefig(basename + '.pdf')
plt.savefig(basename + '.png')

# Plot detailed graph
grouped_df = df.reset_index().groupby('name')
fig, axes = plt.subplots(nrows=len(benchmark_names), ncols=1, figsize=(12, 3*len(benchmark_names)), sharex=True)
for (key, ax) in zip(grouped_df.groups.keys(), axes.flatten()):
	grouped_df.get_group(key).plot(ax=ax, kind='scatter', x='begin', y='duration', s=.6, c=name_to_color[key])
	ax.set_title(key)
	ax.set_xlabel('Seconds since start')
	ax.set_ylabel('Execution duration [ns]')
	ax.yaxis.set_label_coords(-.08, 0.5)
	ax.grid(True, alpha=.3)
fig.tight_layout()

# Write detailed graph to file
plt.savefig(basename + '_detailed.pdf')
plt.savefig(basename + '_detailed.png')
