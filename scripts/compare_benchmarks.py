#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

p_value_significance_threshold = 0.001
min_iterations = 10
min_runtime_ns = 59 * 1000 * 1000 * 1000

def format_diff(diff):
    select_color = lambda value, color: color if abs(value) > 0.05 else 'white'

    diff -= 1  # adapt to show change in percent
    if diff < 0.0:
        return colored("{0:.0%}".format(diff), select_color(diff, 'red'))
    else:
        return colored("+{0:.0%}".format(diff), select_color(diff, 'green'))

def geometric_mean(values):
    product = 1
    for value in values:
        product *= value

    return product**(1 / float(len(values)))

def get_iteration_durations(iterations):
    # Sum up the parsing/optimization/execution/... durations of all statement of a query iteration
    # to a single entry in the result list.

    iteration_durations = []
    for iteration in iterations:
        iteration_duration = 0
        iteration_duration += iteration

        iteration_durations.append(iteration_duration)

    return iteration_durations

def calculate_and_format_p_value(old, new):
    p_value = ttest_ind(array('d', old["durations"]), array('d', new["durations"]))[1]
    is_significant = p_value < p_value_significance_threshold

    notes = ""
    old_runtime = sum(runtime for runtime in old["durations"])
    new_runtime = sum(runtime for runtime in new["durations"])
    if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
        is_significant = False
        notes += "(run time too short) "

    if (len(old["durations"]) < min_iterations or len(new["durations"]) < min_iterations):
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

diffs = []

table_data = []
table_data.append(["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change [%]", "p-value (significant if <" + str(p_value_significance_threshold) + ")"])

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
    name = old['name']
    if old['name'] != new['name']:
        name += ' -> ' + new['name']

    if float(old['items_per_second']) > 0.0:
        diff = float(new['items_per_second']) / float(old['items_per_second'])
        diffs.append(diff)
    else:
        diff = float('nan')

    diff_formatted = format_diff(diff)
    p_value_formatted = calculate_and_format_p_value(old, new)

    table_data.append([name, str(old['items_per_second']), str(len(old['metrics'])), str(new['items_per_second']), str(len(new['metrics'])), diff_formatted, p_value_formatted])

table_data.append(['geometric mean', '', '', '', '', format_diff(geometric_mean(diffs)), ''])

table = AsciiTable(table_data)
table.justify_columns[6] = 'right'

print("")
print(table.table)
print("")
