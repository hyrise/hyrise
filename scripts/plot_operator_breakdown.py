#!/usr/bin/env python3

"""
When called in a folder containing *-PQP.svg files, this script extracts the aggregated operator runtimes from the
table at the bottom right of the graph, parses it, and plots it. While this is slightly hacky, it allows us to retrieve
that information without blowing up the code of the Hyrise core.
"""

import glob
import matplotlib.pyplot as plt
import pandas as pd
import re
import sys
import seaborn as sns
import matplotlib.colors as mplcolors
from matplotlib.colors import LinearSegmentedColormap

if(not len(sys.argv) in [1,2]):
    exit("Call in a folder containing *-PQP.svg files, pass `paper` as an argument to change legend and hatching")
paper_mode = len(sys.argv) == 2 and sys.argv[1] == 'paper'

benchmarks = []

all_operator_breakdowns = {}
for file in sorted(glob.glob('*-PQP.svg')):
    operator_breakdown = {}
    with open(file, 'r') as svg:
        svg_string = svg.read().replace("\n", '|')

        # Find the "total by operator" table using a non-greedy search until the end of the <g> object
        table_string = re.findall(r'Total by operator(.*?)</g>', svg_string)[0]

        # Replace all objects within the table string, also trim newlines (rewritten to |) at the begin and the end
        table_string = re.sub(r'<.*?>', '', table_string)
        table_string = re.sub(r'^\|*', '', table_string)
        table_string = re.sub(r'\|*$', '', table_string)

        row_strings = table_string.split('||')

        # The svg table stores data in a columnar orientation, so we first extract the operator names, then their
        # durations
        operator_names = []
        operator_durations = []

        for operator_name in row_strings[0].split('|'):
            operator_names.append(operator_name.strip())

        # Convert time string to nanoseconds and add to operator_durations
        for operator_duration_str in row_strings[1].split('|'):
            operator_duration = pd.Timedelta(operator_duration_str.replace('µ', 'u')).total_seconds() * 1e9
            operator_durations.append(operator_duration)

        operator_breakdown = dict(zip(operator_names, operator_durations))

        # Ignore the "total" line
        del operator_breakdown['total']

    # Store in all_operator_breakdowns
    all_operator_breakdowns[file.replace('-PQP.svg', '').replace('TPC-H_', 'Q')] = operator_breakdown

# Make operators the columns and order by operator name
df = pd.DataFrame(all_operator_breakdowns).transpose()
df = df.reindex(sorted(df.columns, reverse=True), axis=1)

df = df.fillna(0)

# Calculate share of total execution time (i.e., longer running benchmark items are weighted more)
df.loc["Absolute"] = df.sum() / df.count()

# Normalize data from nanoseconds to percentage of total cost (calculated by dividing the cells value by the total of
# the row it appears in)
df.iloc[:,0:] = df.iloc[:,0:].apply(lambda x: x / x.sum(), axis=1)

# Calculate relative share of operator (i.e., weighing all benchmark items the same) - have to ignore the "Absolute"
# row for that
df.loc["Relative"] = df.head(-1).sum() / df.head(-1).count()

# Print the dataframe for easy access to the raw numbers
print(df)

# Drop all operators that do not exceed 1% in any query
df = df[df > .01].dropna(axis = 'columns', how = 'all')

# Setup colorscheme - using cubehelix, which provides a color mapping that gracefully degrades to grayscale
colors = sns.cubehelix_palette(n_colors=len(df), rot=2, reverse=True, light=.9, dark=.1, hue=1)
cmap = LinearSegmentedColormap.from_list("my_colormap", colors)

# Plot it
ax = df.plot.bar(stacked=True, figsize=(2 + len(df) / 4, 4), colormap=cmap)

if paper_mode:
    # Add hatches in paper mode, where graphs may be printed in grayscale
    # Not used in screen mode, as colors are sufficient there and hatching is ugly
    patterns = ('', '/////', '', '\\\\\\\\\\', '', '/\\/\\/\\/\\/\\', '', '/////', '', '\\\\\\\\\\', '', '/\\/\\/\\/\\/\\')
    hatches = [p for p in patterns for i in range(len(df))]
    for bar, hatch in zip(ax.patches, hatches):
        # Calculate color so that the hatches are visible but not pushy
        hsv = mplcolors.rgb_to_hsv(bar.get_facecolor()[:3])
        hatch_color_hsv = hsv
        hatch_color_hsv[2] = hsv[2] + .2 if hsv[2] < .5 else hsv[2] - .2
        bar.set_edgecolor(mplcolors.hsv_to_rgb(hatch_color_hsv))

        bar.set_hatch(hatch)
        bar.set_linewidth(0)

# Set labels
ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])
ax.set_ylabel('Share of run time\n(Hiding ops <1%)')

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()

if paper_mode:
    # Plot the legend under the graph (good for papers)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width, box.height * 0.8])
    legend = ax.legend(reversed(handles), reversed(labels), loc='lower center', ncol=3, bbox_to_anchor=(0.5, -.45))

    print("Plotting in 'paper' mode (legend below graph, hatching) - remove 'paper' argument to change")
else:
    # Plot the legend to the right of the graph (good for screens)
    legend = ax.legend(reversed(handles), reversed(labels), loc='center left', ncol=1, bbox_to_anchor=(1.0, 0.5))
    print("Plotting in 'screen' mode (legend on right, no hatching) - use 'paper' argument to change")

# Layout and save
plt.tight_layout()
plt.savefig('operator_breakdown.pdf', bbox_extra_artists=(legend,), bbox_inches='tight')
