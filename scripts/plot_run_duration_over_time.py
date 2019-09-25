#!/usr/bin/env python3

# Takes a single benchmark output JSON and plots the duration of item executions over time

import json
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys

if(len(sys.argv) != 2):
    exit('Usage: ' + sys.argv[0] + ' benchmark.json')

with open(sys.argv[1]) as json_file:
    data_json = json.load(json_file)['benchmarks']

# Build flat list of {name, begin, duration} entries for every benchmark item run
data = []
benchmark_names = []
for benchmark_json in data_json:
	name = benchmark_json['name']
	benchmark_names.append(name)
	for run_json in benchmark_json['successful_runs']:
		data.append({'name': name, 'begin': run_json['begin'] / 1e9, 'duration': run_json['duration'], 'success': True})
	for run_json in benchmark_json['unsuccessful_runs']:
		data.append({'name': name, 'begin': run_json['begin'] / 1e9, 'duration': run_json['duration'], 'success': False})


df = pd.DataFrame(data).reset_index()

# Set the colors
benchmark_names.sort()  # Sort the benchmarks for a deterministic color mapping
name_to_color = {}
prop_cycle = plt.rcParams['axes.prop_cycle']
default_colors = prop_cycle.by_key()['color']

fig, ax = plt.subplots()
for i in range(0, len(benchmark_names)):
	benchmark_name = benchmark_names[i]
	color = default_colors[i % len(default_colors)]
	name_to_color[benchmark_name] = color

	filtered_df = df[df['name'] == benchmark_name]

	# Plot into combined graph
	for success in [True, False]:
		if not filtered_df[filtered_df['success']==success].empty:
			filtered_df[filtered_df['success']==success].plot(ax=ax, kind='scatter', x='begin', y='duration', c=color, figsize=(12, 9), s=5, marker=('o' if success else 'x'), linewidth=1)

# Finish combined graph
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
	filtered_df = grouped_df.get_group(key)
	for success in [True, False]:
		if not filtered_df[filtered_df['success']==success].empty:
			filtered_df[filtered_df['success']==success].plot(ax=ax, kind='scatter', x='begin', y='duration', c=name_to_color[key], s=5, marker=('o' if success else 'x'), linewidth=1)
	ax.set_title(key)
	ax.set_xlabel('Seconds since start')
	ax.set_ylabel('Execution duration [ns]')
	ax.yaxis.set_label_coords(-.08, 0.5)
	ax.grid(True, alpha=.3)
fig.tight_layout()

# Write detailed graph to file
plt.savefig(basename + '_detailed.pdf')
plt.savefig(basename + '_detailed.png')