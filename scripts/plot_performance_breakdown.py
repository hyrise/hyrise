#!/usr/bin/env python3

# Given a benchmark result json that was generated with --sql_metrics, this script plots the cost of the different
# steps (parsing, optimization, ...) as a normalized stacked bar chart.

import json
import matplotlib.pyplot as plt
import pandas as pd
import sys

benchmarks = []

if(len(sys.argv) != 2):
    exit("Usage: " + sys.argv[0] + " benchmark.json")

with open(sys.argv[1]) as file:
    data = json.load(file)

for benchmark_json in data['benchmarks']:
    benchmark = []
    benchmark.append(benchmark_json['name'])

    sum_parse_duration = 0.0
    sum_sql_translation_duration = 0.0
    sum_optimization_duration = 0.0
    sum_lqp_translation_duration = 0.0
    sum_plan_execution_duration = 0.0

    for run in benchmark_json['successful_runs']:
        if len(run['metrics']) == 0:
            exit("No metrics found. Did you run the benchmark with --sql_metrics?")
        for metrics in run['metrics']:
            sum_parse_duration += metrics['parse_duration']

            for statement in metrics['statements']:
                sum_sql_translation_duration += statement['sql_translation_duration']
                sum_optimization_duration += statement['optimization_duration']
                sum_lqp_translation_duration += statement['lqp_translation_duration']
                sum_plan_execution_duration += statement['plan_execution_duration']

    benchmark.append(sum_parse_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_sql_translation_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_optimization_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_lqp_translation_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_plan_execution_duration / len(benchmark_json['successful_runs']))

    benchmarks.append(benchmark)

df = pd.DataFrame(benchmarks, columns=['Benchmark', 'Parser', 'SQLTranslator', 'Optimizer', 'LQPTranslator', 'Execution'])

# Normalize data from nanoseconds to percentage of total cost
df.iloc[:,1:] = df.iloc[:,1:].apply(lambda x: x / x.sum(), axis=1)
print(df)

ax = df.plot.bar(x='Benchmark', stacked=True)
ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])
ax.set_ylabel('Share of query run time')

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.0, 1.0))

basename = sys.argv[1].replace('.json', '')
plt.tight_layout()
plt.savefig(basename + '_breakdown.png')
