#!/usr/bin/env python3

"""
When called in a folder containing *-PQP.svg files, this script extracts the aggregated operator runtimes from the
table at the bottom right of the graph, parses it, and plots it. While this is slightly hacky, it allows us to retrieve
that information without blowing up the code of the Hyrise core.
"""

import glob
import math
import matplotlib.pyplot as plt
import pandas as pd
import re
import sys
import seaborn as sns
import matplotlib.colors as mplcolors
import matplotlib.ticker as ticker
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.gridspec import GridSpec


# Determine if a color is rather dark based on the brightness of the HSV color value.
def color_is_dark(rgb_color):
    hatch_color_hsv = mplcolors.rgb_to_hsv(rgb_color)
    return hatch_color_hsv[2] < 0.5


# Calculate hatch color so that the hatches are visible but not pushy
def get_hatch_color(rgb_color):
    hatch_color_hsv = mplcolors.rgb_to_hsv(rgb_color)
    hatch_color_hsv[2] = hatch_color_hsv[2] + 0.2 if color_is_dark(rgb_color) else hatch_color_hsv[2] - 0.2
    return mplcolors.hsv_to_rgb(hatch_color_hsv)


def add_value_labels_to_stacked_plot(ax, format_string):  # adapted from https://stackoverflow.com/a/51535326/1147726
    stack_sum = sum([p.get_height() for p in ax.patches])
    for patch in ax.patches:
        value = patch.get_height()
        if value < (stack_sum * 0.1):  # don't add text labels to tiny bars
            continue
        x = patch.get_x() + patch.get_width() / 2
        y = patch.get_y() + patch.get_height() / 2
        value_str = format_string.format(value)
        patch_color = patch.get_facecolor()[:3]
        ax.text(x, y, value_str, ha="center", va="center", color="white" if color_is_dark(patch_color) else "black")


if len(sys.argv) not in [1, 2] or len(glob.glob("*-PQP.svg")) == 0:
    exit("Call in a folder containing *-PQP.svg files, pass `paper` as an argument to change legend and hatching")
paper_mode = len(sys.argv) == 2 and sys.argv[1] == "paper"

benchmarks = []

all_operator_breakdowns = {}
for file in sorted(glob.glob("*-PQP.svg")):
    operator_breakdown = {}
    with open(file, "r") as svg:
        svg_string = svg.read().replace("\n", "|")

        # Find the "total by operator" table using a non-greedy search until the end of the <g> object
        table_string = re.findall(r"Total by operator(.*?)</g>", svg_string)[0]

        # Replace all objects within the table string, also trim newlines (rewritten to |) at the begin and the end
        table_string = re.sub(r"<.*?>", "", table_string)
        table_string = re.sub(r"^\|*", "", table_string)
        table_string = re.sub(r"\|*$", "", table_string)

        row_strings = table_string.split("||")

        # The svg table stores data in a columnar orientation, so we first extract the operator names, then their
        # durations
        operator_names = []
        operator_durations = []

        for operator_name in row_strings[0].split("|"):
            operator_names.append(operator_name.strip())

        # Convert time string to nanoseconds and add to operator_durations
        for operator_duration_str in row_strings[1].split("|"):
            operator_duration = pd.Timedelta(operator_duration_str.replace("µ", "u")).total_seconds() * 1e9
            operator_durations.append(operator_duration)

        operator_breakdown = dict(zip(operator_names, operator_durations))

        # Ignore the "total" line
        del operator_breakdown["total"]

    # Store in all_operator_breakdowns
    all_operator_breakdowns[file.replace("-PQP.svg", "").replace("TPC-H_", "Q")] = operator_breakdown

# Make operators the columns and order by operator name
df = pd.DataFrame(all_operator_breakdowns).transpose()
df = df.reindex(sorted(df.columns, reverse=True), axis=1)

df = df.fillna(0)

# Obtain copy for normalized plots
df_norm = df.copy()

# Calculate share of total execution time (i.e., longer running benchmark items are weighted more)
df_norm.loc["Total"] = df_norm.sum() / df_norm.count()
# Normalize data from nanoseconds to percentage of total cost (calculated by dividing the cells value by the total of
# the row it appears in)
df_norm.iloc[:, 0:] = df_norm.iloc[:, 0:].apply(lambda x: x / x.sum(), axis=1)

# Print the dataframe for easy access to the raw numbers
print(df_norm)

# Drop all operators that do not exceed 1% in any query
df_norm = df_norm[df_norm >= 0.01].dropna(axis="columns", how="all")
df_norm_queries = df_norm.loc[df_norm.index != "Total"]
df_norm_total = df_norm.loc[["Total"]]

# Setup colorscheme - using cubehelix, which provides a color mapping that gracefully degrades to grayscale
colors = sns.cubehelix_palette(n_colors=len(df_norm), rot=2, reverse=True, light=0.9, dark=0.1, hue=1)
cmap = LinearSegmentedColormap.from_list("my_colormap", colors)

# We want the figure to be slightly wider if more queries are plotted. `added_figure_width` grows sublinearly, using
# sqrt() just happens to work well for the benchmarks that we tested it with.
added_figure_width = int(round(math.sqrt(len(df_norm))))

fig = plt.figure(figsize=(20 + added_figure_width, 6))
plt.subplots(constrained_layout=True)
# Create a grid with two rows (relative and absolute) and two columns (queries and summary). We ensure that each row
# has at least a ratio of 4:1 (queries:summary). This has been manually determined. For TPC-H we get a ratio of 9:1,
# for the Join Order Benchmark it's 14:1.
gs = GridSpec(2, 5 + added_figure_width)

ax_relative_queries = fig.add_subplot(gs[0, :-1])
ax_relative_summary = fig.add_subplot(gs[0, -1])
ax_absolute_queries = fig.add_subplot(gs[1, :-1])
ax_absolute_summary = fig.add_subplot(gs[1, -1])

df_norm_queries.plot.bar(ax=ax_relative_queries, stacked=True, colormap=cmap)
ax_relative_queries.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1.0))
ax_relative_queries.set_ylabel("Share of run time\n(hiding operators <1%)")

df_norm_total.plot.bar(ax=ax_relative_summary, stacked=True, colormap=cmap)
ax_relative_summary.tick_params(axis="x", labelrotation=0)
ax_relative_summary.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1.0))

# Bottom plots with absolute runtimes
df = df / 1e9  # to seconds
df = df[df_norm.columns]  # only show filtered columns of relative chart (>= 1%)

df.plot.bar(ax=ax_absolute_queries, stacked=True, colormap=cmap)
ax_absolute_queries.set_ylabel("Operator run time [s]\n(hiding operators <1%)")
ax_absolute_queries.set_xlabel("Query")

sum_df = df.sum(axis="index").to_frame().T
sum_df.rename(index={0: "Cumulative\nRuntimes [s]"}, inplace=True)

sum_df.plot.bar(ax=ax_absolute_summary, stacked=True, colormap=cmap)
ax_absolute_summary.tick_params(axis="x", labelrotation=0)

# remove legends from subplot as we later add single legend for the whole plot
for axis in [ax_relative_queries, ax_relative_summary, ax_absolute_queries, ax_absolute_summary]:
    axis.legend().remove()

add_value_labels_to_stacked_plot(ax_relative_summary, "{:.0%}")
add_value_labels_to_stacked_plot(ax_absolute_summary, "{:,.1f}")

if paper_mode:
    # Add hatches in paper mode, where graphs may be printed in grayscale
    # Not used in screen mode, as colors are sufficient there and hatching is ugly
    patterns = (
        "",
        "/////",
        "",
        "\\\\\\\\\\",
        "",
        "/\\/\\/\\/\\/\\",
        "",
        "/////",
        "",
        "\\\\\\\\\\",
        "",
        "/\\/\\/\\/\\/\\",
    )
    for axis in [ax_relative_queries, ax_relative_summary, ax_absolute_queries, ax_absolute_summary]:
        column_count = len(axis.get_xticks())
        hatches = [p for p in patterns for i in range(column_count)]
        for bar, hatch in zip(reversed(axis.patches), hatches):
            bar.set_edgecolor(get_hatch_color(bar.get_facecolor()[:3]))
            bar.set_hatch(hatch)
            bar.set_linewidth(0)

handles, labels = ax_relative_queries.get_legend_handles_labels()
fig.legend(
    reversed(handles),
    reversed(labels),
    loc="upper center",
    ncol=10,
)
fig.subplots_adjust(wspace=0.5)  # add a little horizontal margin between the charts (0.2 is the default)
fig.savefig("operator_breakdown.pdf", bbox_inches="tight")
