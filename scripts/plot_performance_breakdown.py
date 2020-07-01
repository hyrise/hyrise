#!/usr/bin/env python3

# Given a benchmark result json that was generated with --metrics, this script plots the cost of the different
# steps (parsing, optimization, ...) and the different optimizer rules as a two normalized stacked bar charts.

import json
import matplotlib.pyplot as plt
import pandas as pd
import sys

benchmarks = []
rule_benchmarks = []

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
    sum_optimizer_rule_durations = {}
    sum_lqp_translation_duration = 0.0
    sum_plan_execution_duration = 0.0

    for run in benchmark_json["successful_runs"]:
        if len(run["metrics"]) == 0:
            exit("No metrics found. Did you run the benchmark with --metrics?")
        for metrics in run["metrics"]:
            sum_parse_duration += metrics["parse_duration"]

            for statement in metrics["statements"]:
                sum_sql_translation_duration += statement["sql_translation_duration"]
                sum_optimization_duration += statement["optimization_duration"]
                sum_lqp_translation_duration += statement["lqp_translation_duration"]
                sum_plan_execution_duration += statement["plan_execution_duration"]

                if statement["optimizer_rule_durations"]:
                    for rule_name, rule_duration in statement["optimizer_rule_durations"].items():
                        # crop typeid().name down to the actual rule name. This might be compiler dependent
                        rule_name = rule_name[11:-1]
                        if rule_name not in sum_optimizer_rule_durations:
                            sum_optimizer_rule_durations[rule_name] = rule_duration
                        else:
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

# summing up the runtimes from all stages for each query
total_time = benchmark_df.iloc[:, 1:].apply(lambda x: x.sum(), axis=1)

# Normalize data from nanoseconds to percentage of total cost
benchmark_df.iloc[:, 1:] = benchmark_df.iloc[:, 1:].apply(lambda x: x / x.sum(), axis=1)
print(benchmark_df)

plt.figure()
ax = benchmark_df.plot.bar(x="Benchmark", stacked=True)
ax.set_ylabel("Share of query run time")
ax.set_yticklabels(["{:,.0%}".format(x) for x in ax.get_yticks()])

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.0, 1.0))

# Add total runtime to labels
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{total_time[label_id]/10e6:.2f} ms")
ax.set_xticklabels(xlabels)

basename = sys.argv[1].replace(".json", "")
plt.tight_layout()
plt.savefig(basename + "_breakdown.png")

rule_benchmark_df = pd.DataFrame(rule_benchmarks, columns=["Benchmark"] + list(sum_optimizer_rule_durations.keys()))
# sort optimizer rules
rule_benchmark_df = rule_benchmark_df.reindex(
    columns=[rule_benchmark_df.columns[0]] + sorted(rule_benchmark_df.columns[1:], key=str.casefold, reverse=True)
)
# summing up the runtimes from all rules for each query
optimizer_total_time = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x.sum(), axis=1)
# Normalize data from nanoseconds to percentage of total cost
rule_benchmark_df.iloc[:, 1:] = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x / x.sum(), axis=1)

# aggregate all rule durations below the threshold
rule_benchmark_df.insert(0, "Other Rules", 0)
threshold = 0.05
for index, benchmark in rule_benchmark_df[sum_optimizer_rule_durations.keys()].iterrows():
    for rule_name, rule_duration in benchmark.iteritems():
        if rule_duration < threshold:
            rule_benchmark_df.loc[index, "Other Rules"] += rule_duration
            rule_benchmark_df.loc[index, rule_name] = None
rule_benchmark_df.dropna(how="all", axis=1, inplace=True)

plt.figure()
ax = rule_benchmark_df.plot.bar(x="Benchmark", stacked=True)
ax.set_ylabel("Share of optimizer run time")

ax.set_yticklabels(["{:,.0%}".format(x) for x in ax.get_yticks()])

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.0, 1.0))

# Add total runtime to labels
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{optimizer_total_time[label_id]/10e6:.2f} ms")
ax.set_xticklabels(xlabels)

plt.tight_layout()
plt.savefig(basename + "_optimizer_breakdown.png")
