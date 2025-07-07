#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# 1) Read & filter
df = pd.read_csv('data/bloom_filter_results.csv')
df = df[(df.vector_size == 10_000) & (df.distinctiveness == 1)]

# 2) Compute filter_rate
df['filter_rate'] = 1 - (df.hitrate / df.vector_size)

# 3) Assign a unique marker to each filter_size
unique_sizes = sorted(df.filter_size.unique())
marker_list  = ['o','s','^','D','v','*','P','X']  # extend if needed
markers      = {size: marker_list[i % len(marker_list)]
                for i, size in enumerate(unique_sizes)}

# 4) Assign a unique color to each (k, hash_function) pair
df['group'] = df.k.astype(str) + '_h' + df.hash_function.astype(str)
unique_groups = df.group.unique()
cmap = plt.cm.get_cmap('tab10', len(unique_groups))
color_map = {g: cmap(i) for i, g in enumerate(unique_groups)}

# 5) Plot
fig, ax = plt.subplots(figsize=(8,6))
ax.set_xlim(left=0.8)
for _, row in df.iterrows():
    ax.scatter(
        row.filter_rate,
        row.probe_time_us,
        marker=markers[row.filter_size],
        color=color_map[row.group],
        edgecolor='k',
        s=80
    )

ax.set_xlabel('Filter Rate (1 - hitrate / vector_size)')
ax.set_ylabel('Probe Time (µs)')
ax.set_title('Probe Time vs Filter Rate (10 K vectors, distinctiveness=1)')

# 6) Legends
# — Marker legend for filter_size
marker_handles = [
    Line2D([0],[0],
           marker=markers[s],
           color='gray',
           linestyle='',
           label=f'filter_size={s}')
    for s in unique_sizes
]
leg1 = ax.legend(handles=marker_handles, title='Filter Size', loc='upper left')
ax.add_artist(leg1)

# — Color legend for k & hash_function
color_handles = [
    Line2D([0],[0],
           marker='o',
           color=color_map[g],
           linestyle='',
           label=g)
    for g in unique_groups
]
ax.legend(handles=color_handles, title='k_hash', loc='lower left')

plt.tight_layout()
plt.savefig("eval_10K_dist1_1.pdf")
plt.show()
