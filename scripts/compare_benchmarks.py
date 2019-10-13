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

def format_diff(diff, inverse_colors = False):
    select_color = lambda value, color: color if abs(value) > 0.05 else 'white'

    diff -= 1  # adapt to show change in percent
    if diff < 0.0:
        color = 'green' if inverse_colors else 'red'
        return colored("{0:.0%}".format(diff), select_color(diff, color))
    else:
        color = 'red' if inverse_colors else 'green'
        return colored("+{0:.0%}".format(diff), select_color(diff, color))

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
    old_durations = [run["duration"] for run in old["successful_runs"]]
    new_durations = [run["duration"] for run in new["successful_runs"]]

    p_value = ttest_ind(array('d', old_durations), array('d', new_durations))[1]
    is_significant = p_value < p_value_significance_threshold

    notes = ""
    old_runtime = sum(runtime for runtime in old_durations)
    new_runtime = sum(runtime for runtime in new_durations)
    if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
        is_significant = False
        return "(run time too short)"
    elif (len(old_durations) < min_iterations or len(new_durations) < min_iterations):
        is_significant = False
        return "(not enough runs)"
    else:
        color = 'green' if is_significant else 'white'
        return colored("{0:.4f}".format(p_value), color)


if (not len(sys.argv) in [3, 4]):
    exit("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json [--github]")

# Format the output as a diff (prepending - and +) so that Github shows colors
github_format = bool(len(sys.argv) == 4 and sys.argv[3] == '--github')

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

if old_data['context']['benchmark_mode'] != new_data['context']['benchmark_mode']:
    exit("Benchmark runs with different modes (ordered/shuffled) are not comparable")

diffs = []

table_data = []
table_data.append(["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change [%]", "p-value"])

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

    table_data.append([name, str(old['items_per_second']), str(len(old['successful_runs'])), str(new['items_per_second']), str(len(new['successful_runs'])), diff_formatted, p_value_formatted])

    if (len(old['unsuccessful_runs']) > 0 or len(new['unsuccessful_runs']) > 0):
        old_unsuccessful_per_second = float(len(old['unsuccessful_runs'])) / (old['duration'] / 1e9)
        new_unsuccessful_per_second = float(len(new['unsuccessful_runs'])) / (new['duration'] / 1e9)

        if len(old['unsuccessful_runs']) > 0:
            diff_unsuccessful = float(new_unsuccessful_per_second / old_unsuccessful_per_second)
        else:
            diff_unsuccessful = float('nan')

        unsuccessful_info = [
            '  unsucc.:',
            ' ' + str(old_unsuccessful_per_second),
            ' ' + str(len(old['unsuccessful_runs'])),
            ' ' + str(new_unsuccessful_per_second),
            ' ' + str(len(new['unsuccessful_runs'])),
            ' ' + format_diff(diff_unsuccessful, True)
        ]

        unsuccessful_info_colored = [colored(text, attrs=['dark']) for text in unsuccessful_info]
        table_data.append(unsuccessful_info_colored)

table_data.append(['geometric mean', '', '', '', '', format_diff(geometric_mean(diffs)), ''])

table = AsciiTable(table_data)
table.justify_columns[6] = 'right'

# If github_format is set, format the output in the style of a diff file where added lines (starting with +) are
# colored green, removed lines (starting with -) are red, and others (starting with an empty space) are black.
# Because terminaltables (unsurprisingly) does not support this hack, we need to post-process the result string,
# searching for the control codes that define text to be formatted as green or red.

result = str(table.table)
if github_format:
    new_result = '```diff\n'
    green_control_sequence = colored('', 'green')[0:5]
    red_control_sequence = colored('', 'red')[0:5]

    for line in result.splitlines():
        if green_control_sequence in line:
            new_result += '+'
        elif red_control_sequence in line:
            new_result += '-'
        else:
            new_result += ' '

        new_result += line + '\n'
    new_result += '```'
    result = new_result

print("")
print(result)
print("")
