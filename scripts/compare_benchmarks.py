#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

p_value_significance_threshold = 0.001
min_iterations = 10
min_runtime_ns = 59 * 1000 * 1000 * 1000

def format_percentage_change(diff):
    select_color = lambda value, color: color if abs(value) > 0.05 else 'white'

    diff -= 1  # adapt to show change in percent
    if diff < 0.0:
        return colored("{0:.0%}".format(diff), select_color(diff, 'red'))
    else:
        return colored("+{0:.0%}".format(diff), select_color(diff, 'green'))

def format_factor_change(change):
    select_color = lambda value, color: color if abs(value) - 1 > 0.05 else 'white'

    if change < 0.0:
        return colored("⬇ {:.4f}".format(abs(change)), select_color(change, 'red'))
    else:
        return colored("⬆ {:.4f}".format(abs(change)), select_color(change, 'green'))

def geometric_mean(values):
    product = 1
    for value in values:
        product *= value

    return pow(product, 1 / len(values))

def calculate_factor_change(old, new):
    if old == 0 and new == 0:
        return float('nan')

    old_abs = abs(old)
    new_abs = abs(new)
    multiplier = -1 if old_abs > new_abs else 1
    return multiplier * (max(old_abs, new_abs) / min(old_abs, new_abs))

def get_iteration_durations(iterations):
    # Sum up the parsing/optimization/execution/... durations of all statement of a query iteration
    # to a single entry in the result list.

    iteration_durations = []
    for iteration in iterations:
        iteration_duration = 0
        for statement in iteration["statements"]:
            iteration_duration += statement["sql_translation_duration"] + statement["optimization_duration"] + \
                                  statement["lqp_translation_duration"] + statement["plan_execution_duration"]
        iteration_duration += iteration["parse_duration"]

        iteration_durations.append(iteration_duration)

    return iteration_durations

def calculate_and_format_p_value(old, new):
    old_iteration_durations = get_iteration_durations(old["metrics"])
    new_iteration_durations = get_iteration_durations(new["metrics"])

    p_value = ttest_ind(array('d', old_iteration_durations), array('d', new_iteration_durations))[1]
    is_significant = p_value < p_value_significance_threshold

    notes = ""
    old_runtime = sum(runtime for runtime in old_iteration_durations)
    new_runtime = sum(runtime for runtime in new_iteration_durations)
    if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
        is_significant = False
        notes += "(run time too short) "

    if (len(old_iteration_durations) < min_iterations or len(new_iteration_durations) < min_iterations):
        is_significant = False
        notes += "(not enough runs) "

    color = 'green' if is_significant else 'white'
    return colored(notes + "{0:.4f}".format(p_value), color)


if(len(sys.argv) != 3):
    print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
    exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

changes = []

table_data = []
table_data.append(["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change [%]", "change factor", "p-value (significant if <" + str(p_value_significance_threshold) + ")"])

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
    name = old['name']
    if old['name'] != new['name']:
        name += ' -> ' + new['name']

    items_per_second_old = float(old['items_per_second'])
    items_per_second_new = float(new['items_per_second'])
    if items_per_second_old > 0.0:
        diff = items_per_second_new / items_per_second_old
        changes.append(diff)
        factor_change = calculate_factor_change(items_per_second_old, items_per_second_new)
    else:
        diff = float('nan')

    diff_formatted = format_percentage_change(diff)
    change_formatted = format_factor_change(factor_change)
    p_value_formatted = calculate_and_format_p_value(old, new)

    table_data.append([name, str(old['items_per_second']), str(len(old['metrics'])), str(new['items_per_second']), str(len(new['metrics'])), diff_formatted, change_formatted, p_value_formatted])

overall_change = calculate_factor_change(sum(run['items_per_second'] for run in old_data['benchmarks']),
                                         sum(run['items_per_second'] for run in new_data['benchmarks']))

table_data.append(['average', '', '', '', '', format_percentage_change(geometric_mean(changes)), format_factor_change(overall_change), ''])

table = AsciiTable(table_data)
table.justify_columns[6] = 'right'

print("")
print(table.table)
print("")
