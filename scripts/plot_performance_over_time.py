#!/usr/bin/env python3

# Takes a single benchmark output JSON and plots the duration of item executions over time

import json
import matplotlib.colors as colors
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys

# https://stackoverflow.com/a/49601444/2204581
def lighten_color(color, amount=0.5):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])

if(len(sys.argv) != 2):
    exit('Usage: ' + sys.argv[0] + ' benchmark.json')

with open(sys.argv[1]) as json_file:
    data_json = json.load(json_file)

has_system_metrics = 'system_utilization' in data_json

# Build flat list of {name, begin, duration} entries for every benchmark item run
data = []
benchmark_names = []
for benchmark_json in data_json['benchmarks']:
	name = benchmark_json['name']
	benchmark_names.append(name)
	for run_json in benchmark_json['successful_runs']:
		data.append({'name': name, 'begin': run_json['begin'] / 1e9, 'duration': run_json['duration'], 'success': True})
	for run_json in benchmark_json['unsuccessful_runs']:
		data.append({'name': name, 'begin': run_json['begin'] / 1e9, 'duration': run_json['duration'], 'success': False})


df = pd.DataFrame(data).reset_index().sort_values('begin')

# Set the colors
benchmark_names.sort()  # Sort the benchmarks for a deterministic color mapping
name_to_color = {}
prop_cycle = plt.rcParams['axes.prop_cycle']
default_colors = prop_cycle.by_key()['color']

# Plot the combined (all items in one) graph; it contains the run durations and the system utilization in two subplots
fig, axes = plt.subplots(nrows=(2 if has_system_metrics else 1), sharex=True)
for i in range(0, len(benchmark_names)):
	benchmark_name = benchmark_names[i]
	color = default_colors[i % len(default_colors)]
	single_run_color = [colors.to_rgba(lighten_color(color, 0.7), 0.5)]
	name_to_color[benchmark_name] = color

	filtered_df = df[df['name'] == benchmark_name]

	# Plot runs into combined graph
	for success in [True, False]:
		if not filtered_df[filtered_df['success']==success].empty:
			filtered_df[filtered_df['success']==success].plot(ax=axes[0], kind='scatter', x='begin', y='duration', c=single_run_color, figsize=(12, 9), s=5, marker=('o' if success else 'x'), linewidth=1)

	# Plot rolling average
	rolling = filtered_df[filtered_df['success']==True].copy()
	window_size = max(1, int(len(rolling) * .05))
	rolling['rolling'] = rolling.duration.rolling(window_size).mean()
	rolling.plot(ax=axes[0], x='begin', y='rolling', c=color)

axes[0].set_xlabel('Seconds since start')
axes[0].set_ylabel('Execution duration [ns]')
axes[0].grid(True, alpha=.3)

# Add legend to combined graph
handles = []
for name, color in name_to_color.items():
	handles.append(mpatches.Patch(label=name, color=color))
axes[0].legend(handles=handles)

if has_system_metrics:
	# Add system utilization to combined graph
	system_utilization = pd.DataFrame(data_json['system_utilization'])
	system_utilization['timestamp'] = system_utilization['timestamp'] / 1e9
	y=['process_RSS']

	if 'allocated_memory' in system_utilization:
		system_utilization['allocated_memory'] = system_utilization['allocated_memory'] / 1e6
		y.append('allocated_memory')

	system_utilization['process_RSS'] = system_utilization['process_RSS'] / 1e6
	system_utilization.plot(ax=axes[1], x='timestamp', y=y, style='.:')

	axes[1].set_xlabel('Seconds since start')
	axes[1].set_ylabel('Memory [MB]')
	axes[1].grid(True, alpha=.3)

# Write combined graph to file
basename = sys.argv[1].replace('.json', '')
plt.savefig(basename + '.pdf')
plt.savefig(basename + '.png')

# Plot detailed graph
grouped_df = df.reset_index().groupby('name')
fig, axes = plt.subplots(nrows=len(benchmark_names), figsize=(12, 3*len(benchmark_names)), sharex=True)
for (key, ax) in zip(grouped_df.groups.keys(), axes):
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