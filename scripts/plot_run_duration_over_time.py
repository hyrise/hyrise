#!/usr/bin/env python3

# Takes a single benchmark output JSON and plots the duration of item executions over time

# TODO add to requirements.txt
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
		data.append({'name': name, 'begin': begin, 'duration': duration})

fig, ax = plt.subplots()
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

# Plot
df.reset_index().plot(kind='scatter', x='begin', y='duration', c=colors, s=.3)
ax.grid(True)

# Add legend
handles = []
for name, color in name_to_color.items():
	handles.append(mpatches.Patch(label=name, color=color))
plt.legend(handles=handles)

# Write to file
basename = sys.argv[1].replace('.json', '')
plt.savefig(basename + '.pdf')
plt.savefig(basename + '.png')
