#!/usr/bin/env python3

# Takes a single benchmark output JSON and plots the duration of item executions as well as
# information about the system utilization (if --metrics is set) and the event log.
#
# If you are seeing "AttributeError: 'NoneType' object has no attribute 'contains_point'",
# your matplotlib version is too old.

import colorsys
import json
import matplotlib.colors as colors
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
import sys


# https://stackoverflow.com/a/49601444/2204581
def lighten_color(input_color, amount=0.5):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    c = input_color
    if input_color in colors.cnames:
        c = colors.cnames[input_color]
    c = colorsys.rgb_to_hls(*colors.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


if len(sys.argv) != 2:
    sys.exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as json_file:
    data_json = json.load(json_file)

# Check if JSON was created with --metrics
has_system_metrics = "system_utilization" in data_json

if has_system_metrics:
    print(
        "Note that the reported memory consumption is also affected by the (relatively high) amount needed to store "
        + "that data. For TPC-H SF 1, this is around 30 MB."
    )
# Build a flat list containing a {name, begin, duration} entry for every benchmark item run
run_durations_list = []
for benchmark_json in data_json["benchmarks"]:
    name = benchmark_json["name"]
    for run_json in benchmark_json["successful_runs"]:
        run_durations_list.append(
            {"name": name, "begin": run_json["begin"] / 1e9, "duration": run_json["duration"], "success": True}
        )
    for run_json in benchmark_json["unsuccessful_runs"]:
        run_durations_list.append(
            {"name": name, "begin": run_json["begin"] / 1e9, "duration": run_json["duration"], "success": False}
        )

# Convert it into a data frame
run_durations_df = pd.DataFrame(run_durations_list).reset_index().sort_values("begin")

# Set the colors
benchmark_names = run_durations_df["name"].unique()
benchmark_names.sort()  # Sort the benchmarks for a deterministic color mapping
name_to_color = {}
prop_cycle = plt.rcParams["axes.prop_cycle"]
default_colors = prop_cycle.by_key()["color"]

# The plotting happens inside this loop. We do it twice, once with all benchmarks in the same graph
# (is_detailed == False) and once where each benchmark has its own subplot
for is_detailed in [False, True]:
    # Calculate the size of the output file (in inches) and the number of subplots
    (figw, figh) = (12, 3 * len(benchmark_names) if is_detailed else 5)
    nrows = len(benchmark_names) if is_detailed else 1

    # If --metrics was used, add space and a subplot for the system metrics
    if has_system_metrics:
        figh += 4
        nrows += 1

    # Create the plot
    fig, axes = plt.subplots(nrows=nrows, sharex=True)
    axes = [axes] if nrows == 1 else axes

    # Add the benchmarks to the plot
    for i, benchmark_name in enumerate(benchmark_names):
        ax = axes[i] if is_detailed else axes[0]

        color = default_colors[i % len(default_colors)]
        name_to_color[benchmark_name] = color
        single_run_color = [colors.to_rgba(lighten_color(color, 0.7), 0.5)]

        # Filter the runs that belong to this benchmark item
        filtered_df = run_durations_df[run_durations_df["name"] == benchmark_name]

        # Plot runs into combined graph
        for success in [True, False]:
            data = filtered_df[filtered_df["success"] == success]
            if not data.empty:
                data.plot(
                    ax=ax,
                    kind="scatter",
                    x="begin",
                    y="duration",
                    color=single_run_color,
                    figsize=(figw, figh),
                    s=5,  # marker size
                    marker=("o" if success else "x"),
                    linewidth=1,
                )

        # Plot rolling average for successful runs, use a window size of 2% of the entire benchmark
        rolling = filtered_df[filtered_df["success"]].copy()
        window_size = max(1, int(len(rolling) * 0.02))
        rolling["rolling"] = rolling.duration.rolling(window_size).mean()
        rolling.plot(ax=ax, x="begin", y="rolling", c=color)

        # Add the labels and the grid
        ax.set_xlabel("Seconds since start")
        ax.set_ylabel("Execution duration [ns]")
        ax.grid(True, alpha=0.3)

        if is_detailed:
            # For the detailed plot, we put the benchmark item's name on top
            ax.set_title(benchmark_name)
            axes[-1].set_title("System Utilization")
            ax.get_legend().remove()
        else:
            # Otherwise, we add a legend
            handles = []
            for name, color in name_to_color.items():
                handles.append(mpatches.Patch(label=name, color=color))
            ax.legend(handles=handles)

    # Next, add the system utilization
    if has_system_metrics:
        system_utilization = pd.DataFrame(data_json["system_utilization"])
        system_utilization["timestamp"] = system_utilization["timestamp"] / 1e9

        # Always plot RSS, only plot allocated_memory if present in JSON (might not be the case if
        # jemalloc is not used)
        y = ["process_RSS"]
        if "allocated_memory" in system_utilization:
            system_utilization["allocated_memory"] = system_utilization["allocated_memory"] / 1e6
            y.append("allocated_memory")

        system_utilization["process_RSS"] = system_utilization["process_RSS"] / 1e6
        system_utilization.plot(ax=axes[-1], x="timestamp", y=y, style=".:")

        axes[-1].set_xlabel("Seconds since start")
        axes[-1].set_ylabel("Memory [MB]")
        axes[-1].grid(True, alpha=0.3)

        axes[-1].yaxis.set_major_locator(plt.MaxNLocator(min_n_ticks=10))

    # Plot log entries
    log_json = data_json["log"]

    # Make the figure larger to accomodate the log entries. Each log entry has a size of
    # log_entry_height and we add another tenth of an inch for spacing
    old_size = fig.get_size_inches()
    log_entry_height = 0.3
    total_log_height = log_entry_height * len(log_json)
    fig.set_size_inches(old_size[0], old_size[1] + total_log_height + 0.1, forward=True)

    # Layout the graphs and move them to the top of the resulting image
    fig.tight_layout(rect=[0, 1 - old_size[1] / fig.get_size_inches()[1], 1, 1])

    # log_y_offset_pixels keeps track of where the next log entry text should be written
    log_y_offset_pixels = total_log_height * fig.dpi
    for log in log_json:
        timestamp = log["timestamp"] / 1e9

        # Convert the x-Axis location of the timestamp (which is data-relative) to an absolute
        # position. Together with log_y_offset_pixels, it describes the location of the text
        # in "figure pixels"
        xyB = (axes[-1].get_xaxis_transform().transform((timestamp, 0))[0], log_y_offset_pixels)

        # Add the line
        line = mpatches.ConnectionPatch(
            xyA=(timestamp, axes[0].get_ylim()[1]),
            xyB=xyB,
            coordsA="data",
            coordsB="figure pixels",
            axesA=axes[0],
            axesB=axes[-1],
            color=(0, 0, 0, 0.2),
            clip_on=False,
            linestyle="dashed",
        )
        fig.add_artist(line)

        # Retrieve the final location of the line's end. If we are in the right half of the image,
        # make the text right-aligned.
        textloc = line.get_path().vertices[-1]
        align = "right" if textloc[0] > 0.5 else "left"
        text = fig.text(
            textloc[0],
            textloc[1],
            log["reporter"] + ": " + log["message"],
            fontsize=9,
            horizontalalignment=align,
            bbox=dict(facecolor="white", edgecolor=(0, 0, 0, 0.2), pad=3),
        )

        # Update log_y_offset_pixels so that the next log entry gets written below
        log_y_offset_pixels -= log_entry_height * fig.dpi

    # Write graph to file
    basename = sys.argv[1].replace(".json", "")
    name = basename + ("_detailed" if is_detailed else "")
    plt.savefig(name + ".pdf")
    plt.savefig(name + ".png")
