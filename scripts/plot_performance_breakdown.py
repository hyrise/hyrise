#!/usr/bin/env python3

# Given a benchmark result json that was generated with --metrics, this script plots the cost of the different steps
# (parsing, optimization, ...) and the different optimizer rules as two normalized stacked bar charts (one for the
# entire SQL pipeline, one for the optimizer rules).

import json
import math
import matplotlib.pyplot as plt
import matplotlib.ticker as mplticker
import pandas as pd
import sys
from collections import defaultdict


benchmarks = []
rule_benchmarks = []

y_ticks = [x / 5 for x in range(6)]
y_tick_labels = [f"{x:,.0%}" for x in y_ticks]


if len(sys.argv) != 2:
    exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as file:
    data = json.load(file)

for benchmark_json in data["benchmarks"]:
    benchmark = []
    rule_benchmark = []
    benchmark.append(benchmark_json["name"])
    rule_benchmark.append(benchmark_json["name"])

    sum_parse_duration = 0.0
    sum_sql_translation_duration = 0.0
    sum_optimization_duration = 0.0
    sum_optimizer_rule_durations = defaultdict(list)
    sum_lqp_translation_duration = 0.0
    sum_plan_execution_duration = 0.0

    for run in benchmark_json["successful_runs"]:
        if len(run["pipeline_metrics"]) == 0:
            exit("No pipeline metrics found. Did you run the benchmark with --pipeline_metrics?")
        for metrics in run["pipeline_metrics"]:
            sum_parse_duration += metrics["parse_duration"]

            for statement in metrics["statements"]:
                # Plan caching should be switched off for pipeline metrics generation by the BenchmarkRunner. Otherwise,
                # we would not have enough measurements of translation/optimization times to provide meaningful average
                # times.
                assert not statement["query_plan_cache_hit"], "Plan caching must be switched off for metrics tracking."

                sum_sql_translation_duration += statement["sql_translation_duration"]
                sum_optimization_duration += statement["optimization_duration"]
                sum_lqp_translation_duration += statement["lqp_translation_duration"]
                sum_plan_execution_duration += statement["plan_execution_duration"]

                assert statement["optimizer_rule_durations"], "No rule durations found."
                for rule_name, rule_duration in statement["optimizer_rule_durations"].items():
                    sum_optimizer_rule_durations[rule_name] += rule_duration

    benchmark.append(sum_parse_duration / len(benchmark_json["successful_runs"]))
    benchmark.append(sum_sql_translation_duration / len(benchmark_json["successful_runs"]))
    benchmark.append(sum_optimization_duration / len(benchmark_json["successful_runs"]))
    benchmark.append(sum_lqp_translation_duration / len(benchmark_json["successful_runs"]))
    benchmark.append(sum_plan_execution_duration / len(benchmark_json["successful_runs"]))

    benchmarks.append(benchmark)

    for rule_durations in sum_optimizer_rule_durations.values():
        rule_duration = sum(rule_durations)
        rule_benchmark.append(rule_duration / len(benchmark_json["successful_runs"]))
    rule_benchmarks.append(rule_benchmark)

benchmark_df = pd.DataFrame(
    benchmarks, columns=["Benchmark", "Parser", "SQLTranslator", "Optimizer", "LQPTranslator", "Execution"]
)

# Summing up the runtimes from all stages for each query.
total_time = benchmark_df.iloc[:, 1:].apply(lambda x: x.sum(), axis=1)

# Normalize data from nanoseconds to percentage of total cost
benchmark_df.iloc[:, 1:] = benchmark_df.iloc[:, 1:].apply(lambda x: x / x.sum(), axis=1)
print(benchmark_df)
ax = benchmark_df.plot.bar(x="Benchmark", stacked=True, zorder=3)
ax.set_ylabel("Share of Query Runtime")

ax.yaxis.set_major_locator(mplticker.FixedLocator(y_ticks))
ax.set_yticklabels(y_tick_labels)

# We want the figure to be slightly wider if more queries are plotted. `added_figure_width` grows sublinearly, using
# sqrt() just happens to work well for the benchmarks that we tested it with.
wide_layout_threshold = 20
apply_wide_layout = len(benchmark_df) > wide_layout_threshold
added_figure_width = int(round(math.sqrt(len(benchmark_df))))

# Reverse legend so that it matches the stacked bars. If the plot is wider, also distribute the legend better.
handles, labels = ax.get_legend_handles_labels()
legend_columns = 5 if apply_wide_layout else 2
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(0.5, 1.05), loc="lower center", ncols=legend_columns)

# Add total runtime to labels.
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{total_time[label_id]/10**6:.2f} ms")
ax.set_xticklabels(xlabels)

if apply_wide_layout:
    plt.gcf().set_size_inches(wide_layout_threshold + added_figure_width, 4.8)
plt.grid(axis="y", visible=True, zorder=0, color="black")
plt.tight_layout()
basename = sys.argv[1].replace(".json", "")
plt.savefig(basename + "_breakdown.pdf")

rule_benchmark_df = pd.DataFrame(rule_benchmarks, columns=["Benchmark"] + list(sum_optimizer_rule_durations.keys()))
# sort optimizer rules
rule_benchmark_df = rule_benchmark_df.reindex(
    columns=[rule_benchmark_df.columns[0]] + sorted(rule_benchmark_df.columns[1:], key=str.casefold, reverse=True)
)

# Summing up the runtimes from all rules for each query.
optimizer_total_time = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x.sum(), axis=1)
# Normalize data from nanoseconds to percentage of total cost
rule_benchmark_df.iloc[:, 1:] = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x / x.sum(), axis=1)

# Aggregate all rule durations below the threshold.
rule_benchmark_df.insert(0, "Other Rules", 0)
threshold = 0.05
for index, benchmark in rule_benchmark_df[sum_optimizer_rule_durations.keys()].iterrows():
    for rule_name, rule_duration in benchmark.items():
        if rule_duration < threshold:
            rule_benchmark_df.loc[index, "Other Rules"] += rule_duration
            rule_benchmark_df.loc[index, rule_name] = None
rule_benchmark_df.dropna(how="all", axis=1, inplace=True)

plt.figure()
ax = rule_benchmark_df.plot.bar(x="Benchmark", stacked=True, zorder=3)
ax.set_ylabel("Share of Optimizer Runtime")

ax.yaxis.set_major_locator(mplticker.FixedLocator(y_ticks))
ax.set_yticklabels(y_tick_labels)

# Reverse legend so that it matches the stacked bars.
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(0.5, 1.05), loc="lower center", ncols=legend_columns)

# Add total runtime to labels.
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{optimizer_total_time[label_id]/10**6:.2f} ms")
ax.set_xticklabels(xlabels)

if apply_wide_layout:
    plt.gcf().set_size_inches(wide_layout_threshold + added_figure_width, 4.8)
plt.grid(axis="y", visible=True, zorder=0, color="black")
plt.tight_layout()
plt.savefig(basename + "_optimizer_breakdown.pdf")
