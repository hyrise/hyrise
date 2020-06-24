#!/usr/bin/env python3

# Given a benchmark result json that was generated with --sql_metrics, this script plots the cost of the different
# steps (parsing, optimization, ...) as a normalized stacked bar chart.

import json
import matplotlib.pyplot as plt
import pandas as pd
import argparse

benchmarks = []
rule_benchmarks = []

parser = argparse.ArgumentParser(description='''Given a benchmark result json that was generated with --sql_metrics,
                                this script plots the cost of the different steps (parsing, optimization, ...) as a
                                normalized stacked bar chart.''')
parser.add_argument('filename', action='store', help='Benchmark json filename')
parser.add_argument('--plot_rules', dest='plot_rules', action='store_true', help='Plot performance breakdown for each optimizer rule')
options = parser.parse_args()

with open(options.filename) as file:
    data = json.load(file)

for benchmark_json in data['benchmarks']:
    benchmark = []
    rule_benchmark = []
    benchmark.append(benchmark_json['name'])
    rule_benchmark.append(benchmark_json['name'])

    sum_parse_duration = 0.0
    sum_sql_translation_duration = 0.0
    sum_optimization_duration = 0.0
    sum_optimizer_rule_durations = {}
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
                if statement['optimizer_rule_durations']:
                    for rule_name, rule_duration in statement['optimizer_rule_durations'].items():
                        if rule_name not in sum_optimizer_rule_durations:
                            sum_optimizer_rule_durations[rule_name] = rule_duration
                        else:
                            sum_optimizer_rule_durations[rule_name] += rule_duration
                sum_lqp_translation_duration += statement['lqp_translation_duration']
                sum_plan_execution_duration += statement['plan_execution_duration']

    benchmark.append(sum_parse_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_sql_translation_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_optimization_duration / len(benchmark_json['successful_runs']))
    if options.plot_rules:
        for rule_duration in sum_optimizer_rule_durations.values():
            rule_benchmark.append(rule_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_lqp_translation_duration / len(benchmark_json['successful_runs']))
    benchmark.append(sum_plan_execution_duration / len(benchmark_json['successful_runs']))

    benchmarks.append(benchmark)
    rule_benchmarks.append(rule_benchmark)

column_names = ['Benchmark', 'Parser', 'SQLTranslator', 'Optimizer', 'LQPTranslator', 'Execution']
benchmark_df = pd.DataFrame(benchmarks, columns=column_names)

# summing up the runtimes from all stages for each query
total_time = benchmark_df.iloc[:,1:].apply(lambda x: x.sum(), axis=1)

# Normalize data from nanoseconds to percentage of total cost
benchmark_df.iloc[:,1:] = benchmark_df.iloc[:,1:].apply(lambda x: x / x.sum(), axis=1)

if options.plot_rules:
    column_names = ['Benchmark'] + list(sum_optimizer_rule_durations.keys())
    rule_benchmark_df = pd.DataFrame(rule_benchmarks, columns=column_names)
    # sort optimizer rules
    rule_benchmark_df = rule_benchmark_df.reindex(columns= [rule_benchmark_df.columns[0]]
                                                         + sorted(rule_benchmark_df.columns[1:], 
                                                                  key=str.casefold, 
                                                                  reverse=True))
    # summing up the runtimes from all rules for each query
    optimizer_total_time = rule_benchmark_df.iloc[:,1:].apply(lambda x: x.sum(), axis=1)
    # Normalize data from nanoseconds to percentage of total cost
    rule_benchmark_df.iloc[:,1:] = rule_benchmark_df.iloc[:,1:].apply(lambda x: x / x.sum(), axis=1)
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
ax = benchmark_df.plot.bar(x='Benchmark', stacked=True)
ax.set_ylabel('Share of query run time')

ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])

# Reverse legend so that it matches the stacked bars
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.0, 1.0))

# Add total runtime to labels
xlabels = ax.get_xticklabels()
for label_id, label in enumerate(xlabels):
    label.set_text(label.get_text() + '\n' + r'$\emptyset$ ' + f'{total_time[label_id]/10e6:.2f} ms')
ax.set_xticklabels(xlabels)

basename = options.filename.replace('.json', '')
plt.tight_layout()
plt.savefig(basename + '_breakdown.png')

if options.plot_rules:
    plt.figure()
    ax = rule_benchmark_df.plot.bar(x='Benchmark', stacked=True)
    ax.set_ylabel('Share of optimizer run time')
    
    ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])

    # Reverse legend so that it matches the stacked bars
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.0, 1.0))

    # Add total runtime to labels
    xlabels = ax.get_xticklabels()
    for label_id, label in enumerate(xlabels):
        label.set_text(label.get_text() + '\n' + r'$\emptyset$ ' + f'{optimizer_total_time[label_id]/10e6:.2f} ms')
    ax.set_xticklabels(xlabels)

    basename = options.filename.replace('.json', '')
    plt.tight_layout()
    plt.savefig(basename + '_optimizer_breakdown.png')


