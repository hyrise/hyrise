#!/usr/bin/env python3

# Given a benchmark result json that was generated with --metrics, this script plots the cost of the different steps
# (parsing, optimization, ...) and the different optimizer rules as two normalized stacked bar charts (one for the
# entire SQL pipeline, one for the optimizer rules).

import json
import matplotlib.pyplot as plt
import matplotlib.ticker as mplticker
import numpy as np
import pandas as pd
import sys
from collections import defaultdict


class Statement:
    def __init__(self):
        self.execution_durations = []
        self.optimization_durations = []
        self.sql_translation_durations = []
        self.lqp_translation_durations = []
        self.optimizer_rule_durations = defaultdict(list)

    def mean_execution_duration(self):
        return np.mean(self.execution_durations)

    def mean_optimization_duration(self):
        return np.mean(self.optimization_durations)

    def mean_sql_translation_duration(self):
        return np.mean(self.sql_translation_durations)

    def mean_lqp_translation_duration(self):
        return np.mean(self.lqp_translation_durations)

    def mean_optimizer_rule_durations(self):
        rule_durations = {}
        optimizer_runs = len(self.optimization_durations)
        for rule, durations in self.optimizer_rule_durations.items():
            # Not using np.mean() since each rule can be applied multiple times per statement.
            rule_durations[rule] = sum(durations) / optimizer_runs
        return rule_durations


class Query:
    def __init__(self):
        self.parse_durations = []
        self.statements = []

    def parse_duration(self):
        return np.mean(self.parse_durations)

    def execution_duration(self):
        return sum([statement.mean_execution_duration() for statement in self.statements])

    def optimization_duration(self):
        return sum([statement.mean_optimization_duration() for statement in self.statements])

    def sql_translation_duration(self):
        return sum([statement.mean_sql_translation_duration() for statement in self.statements])

    def lqp_translation_duration(self):
        return sum([statement.mean_lqp_translation_duration() for statement in self.statements])

    def optimizer_rule_durations(self):
        rule_durations = defaultdict(float)
        for statement in self.statements:
            for rule, duration in statement.mean_optimizer_rule_durations().items():
                rule_durations[rule] += duration
        return rule_durations


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
    queries = []

    # Successful runs of benchmark item.
    for run in benchmark_json["successful_runs"]:
        if len(run["metrics"]) == 0:
            exit("No metrics found. Did you run the benchmark with --metrics?")

        if len(queries) == 0:
            queries = [Query() for _ in range(len(run["metrics"]))]

        assert len(queries) == len(run["metrics"])

        # Metrics for each item query, e.g., queries in TPC-C procedures.
        for query_id, metrics in enumerate(run["metrics"]):
            query = queries[query_id]
            query.parse_durations.append(metrics["parse_duration"])
            if len(query.statements) == 0:
                query.statements = [Statement() for _ in range(len(metrics["statements"]))]
            assert len(query.statements) == len(metrics["statements"])

            # Individual statements of each query, e.g., CREATE VIEW view_name, SELECT ... FROM view_name, DROP VIEW view_name
            for statement_id, statement_metrics in enumerate(metrics["statements"]):
                statement = query.statements[statement_id]
                statement.execution_durations.append(statement_metrics["plan_execution_duration"])

                # Cached queries have no meaningful translation and optimization information.
                if statement_metrics["query_plan_cache_hit"]:
                    continue

                statement.sql_translation_durations.append(statement_metrics["sql_translation_duration"])
                statement.optimization_durations.append(statement_metrics["optimization_duration"])
                statement.lqp_translation_durations.append(statement_metrics["lqp_translation_duration"])

                assert statement_metrics[
                    "optimizer_rule_durations"
                ], "Statement was not cached, but optimizer rule metrics are empty."
                for rule_name, rule_durations in statement_metrics["optimizer_rule_durations"].items():
                    statement.optimizer_rule_durations[rule_name] += rule_durations

    # Sum up metrics for all queries of the item.
    benchmark.append(sum([query.parse_duration() for query in queries]))
    benchmark.append(sum([query.sql_translation_duration() for query in queries]))
    benchmark.append(sum([query.optimization_duration() for query in queries]))
    benchmark.append(sum([query.lqp_translation_duration() for query in queries]))
    benchmark.append(sum([query.execution_duration() for query in queries]))

    benchmarks.append(benchmark)

    # Aggregate optimizer rules for all queries of the item.
    optimizer_rule_durations = defaultdict(float)
    for query in queries:
        for rule, duration in query.optimizer_rule_durations().items():
            optimizer_rule_durations[rule] += duration

    for rule_duration in optimizer_rule_durations.values():
        rule_benchmark.append(rule_duration)
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
ax.set_ylabel("Share of Query Runtime")

ax.yaxis.set_major_locator(mplticker.FixedLocator(y_ticks))
ax.set_yticklabels(y_tick_labels)

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(0.5, 1.05), loc="lower center", ncols=2)

# Add total runtime to labels
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{total_time[label_id]/10**6:.2f} ms")
ax.set_xticklabels(xlabels)

basename = sys.argv[1].replace(".json", "")
plt.tight_layout()
plt.savefig(basename + "_breakdown.pdf")

rule_benchmark_df = pd.DataFrame(rule_benchmarks, columns=["Benchmark"] + list(optimizer_rule_durations.keys()))
# sort optimizer rules
rule_benchmark_df = rule_benchmark_df.reindex(
    columns=[rule_benchmark_df.columns[0]] + sorted(rule_benchmark_df.columns[1:], key=str.casefold, reverse=True)
)
print(rule_benchmark_df)

# summing up the runtimes from all rules for each query
optimizer_total_time = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x.sum(), axis=1)
# Normalize data from nanoseconds to percentage of total cost
rule_benchmark_df.iloc[:, 1:] = rule_benchmark_df.iloc[:, 1:].apply(lambda x: x / x.sum(), axis=1)

# aggregate all rule durations below the threshold
rule_benchmark_df.insert(0, "Other Rules", 0)
threshold = 0.05
for index, benchmark in rule_benchmark_df[optimizer_rule_durations.keys()].iterrows():
    for rule_name, rule_duration in benchmark.items():
        if rule_duration < threshold:
            rule_benchmark_df.loc[index, "Other Rules"] += rule_duration
            rule_benchmark_df.loc[index, rule_name] = None
rule_benchmark_df.dropna(how="all", axis=1, inplace=True)

plt.figure()
ax = rule_benchmark_df.plot.bar(x="Benchmark", stacked=True)
ax.set_ylabel("Share of Optimizer Runtime")

ax.yaxis.set_major_locator(mplticker.FixedLocator(y_ticks))
ax.set_yticklabels(y_tick_labels)

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(0.5, 1.05), loc="lower center", ncols=2)

# Add total runtime to labels
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + "\n" + r"$\emptyset$ " + f"{optimizer_total_time[label_id]/10**6:.2f} ms")
ax.set_xticklabels(xlabels)

plt.tight_layout()
plt.savefig(basename + "_optimizer_breakdown.pdf")
