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
for benchmark_json in data_json:
	name = benchmark_json['name']
	for run_json in benchmark_json['runs']:
		begin = run_json['begin']
		duration = run_json['duration']
		data.append({'name': name, 'begin': begin, 'duration': duration})

fig, ax = plt.subplots()

df = pd.DataFrame(data)
colors = np.where(df['name'] == 'Delivery','r','-')
colors[df['name'] == 'New-Order'] = 'g'
colors[df['name'] == 'Order-Status'] = 'b'
colors[df['name'] == 'Payment'] = 'c'
colors[df['name'] == 'Stock-Level'] = 'm'
df.reset_index().plot(kind='scatter', x='begin', y='duration', c=colors)

handles = [
	mpatches.Patch(color='r', label='Delivery'),
	mpatches.Patch(color='g', label='NewOrder'),
	mpatches.Patch(color='b', label='OrderStatus'),
	mpatches.Patch(color='c', label='Payment'),
	mpatches.Patch(color='m', label='StockLevel')
]
plt.legend(handles=handles)

ax.grid(True)

basename = sys.argv[1].replace('.json', '')
plt.savefig(basename + '.pdf')
plt.savefig(basename + '.png')
