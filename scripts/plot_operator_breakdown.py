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

if(len(sys.argv) != 1):
    exit("Call without arguments in a folder containing *-PQP.svg files")

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

        # Quick'n dirty conversion from time string to nanoseconds
        for operator_duration_str in row_strings[1].split('|'):
            operator_duration_str = operator_duration_str.replace(' min ', ' * 60 * 1e9 + ')
            operator_duration_str = operator_duration_str.replace(' s ', ' * 1e9 + ')
            operator_duration_str = operator_duration_str.replace(' ms ', ' * 1e6 + ')
            operator_duration_str = operator_duration_str.replace(' µs ', ' * 1e3 + ')
            operator_duration_str = operator_duration_str.replace(' ns ', ' + ')
            operator_duration_str += '0'
            operator_duration = float(eval(operator_duration_str))
            operator_durations.append(operator_duration)

        operator_breakdown = dict(zip(operator_names, operator_durations))

        # Ignore the "total" line
        del operator_breakdown['total']

    # Store in all_operator_breakdowns
    all_operator_breakdowns[file.replace('-PQP.svg', '').replace('TPC-H_', 'Q')] = operator_breakdown

# Make operators the columns and order by operator name
df = pd.DataFrame(all_operator_breakdowns).transpose()
df = df.reindex(sorted(df.columns, reverse=True), axis=1)

df.loc["Total"] = df.sum() / df.count()

# Normalize data from nanoseconds to percentage of total cost (calculated by dividing the cells value by the total of
# the row it appears in)
df.iloc[:,0:] = df.iloc[:,0:].apply(lambda x: x / x.sum(), axis=1)

# Drop all operators that do not exceed 1% in any query
df = df[df > .01].dropna(axis = 'columns', how = 'all')
print(df)

# Plot it
ax = df.plot.bar(stacked=True, figsize=(2 + len(df) / 4, 4))
ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])
ax.set_ylabel('Share of run time\n(Hiding ops <1%)')

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
lgd = ax.legend(reversed(handles), reversed(labels), loc='center left', ncol=1, bbox_to_anchor=(1.0, 0.5))

plt.tight_layout()
plt.savefig('operator_breakdown.pdf', bbox_extra_artists=(lgd,), bbox_inches='tight')
