#!/usr/bin/env python3

# Compares two Google Benchmark result JSONs (e.g., before and after a change) and plots two charts: absolute runtimes
# side by side and the relative change per benchmark case.
#
# Usage: compare_micro_benchmarks.py before.json after.json
# Produces: <after-basename>_comparison.pdf and <after-basename>_change.pdf

import json
import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as mplticker
import numpy as np


def load(path):
    with open(path) as file:
        data = json.load(file)

    runs = {}
    for benchmark in data["benchmarks"]:
        # Skip aggregates (_mean, _median, ...) so that repetitions are not counted twice.
        if benchmark.get("run_type") == "aggregate":
            continue
        runs[benchmark["name"]] = benchmark["real_time"]
    return runs, data["benchmarks"][0].get("time_unit", "ns")


if len(sys.argv) != 3:
    exit("Usage: " + sys.argv[0] + " before.json after.json")

before, time_unit = load(sys.argv[1])
after, _ = load(sys.argv[2])

names = [name for name in before if name in after]
if not names:
    exit("No benchmark names are present in both files.")

before_times = [before[name] for name in names]
after_times = [after[name] for name in names]
changes = [(after[name] - before[name]) / before[name] * 100 for name in names]

# Strip the common fixture prefix so the labels stay readable.
labels = [name.split("/", 1)[-1] if "/" in name else name for name in names]
positions = np.arange(len(names))

# Chart 1: absolute runtimes, before vs. after.
figure, axis = plt.subplots()
width = 0.4
axis.bar(positions - width / 2, before_times, width, label="before", zorder=3)
axis.bar(positions + width / 2, after_times, width, label="after", zorder=3)
axis.set_ylabel(f"Runtime ({time_unit})")
axis.set_xlabel("Benchmark")
axis.set_xticks(positions)
axis.set_xticklabels(labels, rotation=90)
axis.legend()
plt.grid(axis="y", visible=True, zorder=0, color="black", alpha=0.3)
plt.tight_layout()
basename = sys.argv[2].replace(".json", "")
plt.savefig(basename + "_comparison.pdf")

# Chart 2: relative change. Negative (faster) is good, so color it accordingly.
plt.figure()
figure, axis = plt.subplots()
colors = ["tab:green" if change < 0 else "tab:red" for change in changes]
axis.bar(positions, changes, color=colors, zorder=3)
axis.axhline(0, color="black", linewidth=0.8)
axis.set_ylabel("Change in runtime")
axis.set_xlabel("Benchmark")
axis.set_xticks(positions)
axis.set_xticklabels(labels, rotation=90)
axis.yaxis.set_major_formatter(mplticker.PercentFormatter())
plt.grid(axis="y", visible=True, zorder=0, color="black", alpha=0.3)
plt.tight_layout()
plt.savefig(basename + "_change.pdf")

# Print the same data so the numbers can be pasted into a PR description.
name_width = max(len(label) for label in labels)
print(f"{'benchmark'.ljust(name_width)}  {'before':>12}  {'after':>12}  {'change':>8}")
for label, before_time, after_time, change in zip(labels, before_times, after_times, changes):
    print(f"{label.ljust(name_width)}  {before_time:12.1f}  {after_time:12.1f}  {change:+7.1f}%")