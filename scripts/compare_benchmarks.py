#!/usr/bin/env python3

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
    diff -= 1  # adapt to show change in percent
    if diff < 0.0:
        return f"{diff:.0%}"
    else:
        return f"+{diff:.0%}"

def color_diff(diff, inverse_colors = False):
    select_color = lambda value, color: color if abs(value - 1) > 0.05 else 'white'

    diff_str = format_diff(diff)
    color = 'green' if (diff_str[0] == '+') != (inverse_colors) else 'red'

    return colored(format_diff(diff), select_color(diff, color))

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
        global add_note_for_insufficient_pvalue_runs
        add_note_for_insufficient_pvalue_runs = True
        return colored('˅', 'yellow', attrs=['bold'])
    else:
        if is_significant:
            return colored(f"{p_value:.4f}", 'white')
        else:
            return colored(f"{p_value:.4f}", 'yellow', attrs=['bold'])

def print_context_overview(old_config, new_config):
    ignore_difference_for = ['GIT-HASH', 'date']
    lines = [["Parameter", sys.argv[1], sys.argv[2]]]
    for (old_key, old_value), (new_key, new_value) in zip(old_config['context'].items(), new_config['context'].items()):
        color = 'white'
        note = ' '
        if old_value != new_value and old_key not in ignore_difference_for:
            color = 'red'
            note = '≠'
        lines.append([colored(note + old_key, color), old_value, new_value])

    table = AsciiTable(lines)
    table.title = 'Configuration Overview'
    print(table.table)


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

diffs_latency = []
diffs_throughput = []
total_runtime_old = 0
total_runtime_new = 0

add_note_for_capped_runs = False
add_note_for_insufficient_pvalue_runs = False

print_context_overview(old_data, new_data)

table_data = []
# $latency and $thrghpt will be replaced later with a title spanning two columns
table_data.append(["Item", "", "$latency", "", "Change", "", "$thrghpt", "", "Change", "p-value"])
table_data.append(["", "", "old", "new", "", "", "old", "new", "", ""])

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
    name = old['name']
    if old['name'] != new['name']:
        name += ' -> ' + new['name']

    # TODO Check if avg_real_time_per_iteration is correctly calculated for multi-client benchmarks
    total_runtime_old += old['avg_real_time_per_iteration']
    total_runtime_new += new['avg_real_time_per_iteration']

    if float(old['avg_real_time_per_iteration']) > 0.0:
        diff_latency = float(new['avg_real_time_per_iteration']) / float(old['avg_real_time_per_iteration'])
    else:
        diff_latency = float('nan')

    if float(old['items_per_second']) > 0.0:
        diff_throughput = float(new['items_per_second']) / float(old['items_per_second'])
        diffs_throughput.append(diff_throughput)
    else:
        diff_throughput = float('nan')

    diff_throughput_formatted = color_diff(diff_throughput)
    diff_latency_formatted = color_diff(diff_latency, True)
    p_value_formatted = calculate_and_format_p_value(old, new)

    if (old_data['context']['max_runs'] > 0 or new_data['context']['max_runs'] > 0) and \
       (old['iterations'] >= old_data['context']['max_runs'] or new['iterations'] >= new_data['context']['max_runs']):
        note = colored('˄', 'yellow', attrs=['bold'])
        add_note_for_capped_runs = True
    else:
        note = ' '

    # Note, we use a width of 7/8 for printing to ensure that we can later savely replace the latency/throughput marker
    # and everything still fits nicely.
    table_data.append([name, '', f'{(old["avg_real_time_per_iteration"] / 1e6):>7.1f}',
                      f'{(new["avg_real_time_per_iteration"] / 1e6):>7.1f}', diff_latency_formatted + note, '',
                      f'{old["items_per_second"]:>8.2f}', f'{new["items_per_second"]:>8.2f}',
                      diff_throughput_formatted + note, p_value_formatted])

    if (len(old['unsuccessful_runs']) > 0 or len(new['unsuccessful_runs']) > 0):
        old_unsuccessful_per_second = float(len(old['unsuccessful_runs'])) / (old['duration'] / 1e9)
        new_unsuccessful_per_second = float(len(new['unsuccessful_runs'])) / (new['duration'] / 1e9)

        if len(old['unsuccessful_runs']) > 0:
            diff_unsuccessful = float(new_unsuccessful_per_second / old_unsuccessful_per_second)
        else:
            diff_unsuccessful = float('nan')

        unsuccessful_info = [
            '   unsucc.:', ''
            f'{old_unsuccessful_per_second:>.2f}',
            f'{new_unsuccessful_per_second:>.2f}',
            color_diff(diff_unsuccessful) + ' '
        ]

        unsuccessful_info_colored = [colored(text, attrs=['dark']) for text in unsuccessful_info]
        table_data.append(unsuccessful_info_colored)

table_data.append(['Sum', '', f'{(total_runtime_old / 1e6):>7.1f}', f'{(total_runtime_new / 1e6):>7.1f}',
                   color_diff(total_runtime_new / total_runtime_old, True) + ' '])
table_data.append(['Geomean', '' , '', '', '', '', '', '', color_diff(geometric_mean(diffs_throughput)) + ' '])

table = AsciiTable(table_data)
for column_index in range(1, 10): # all columns justified to right, except for item name
    table.justify_columns[column_index] = 'right'

result = str(table.table)

new_result = ''
lines = result.splitlines()

# TODO: replace properly
" Latency (ms/iter)"
" Throughput (iter/s)"

# Narrow separation column TODO: multiple times
separation_columns = []
header_strings = lines[4].split('|') # use a result line without empty columns here
for column_id, text in enumerate(header_strings):
    # find empty columns
    # ignore first and last as this is "outside" of the actual table
    if text.strip() == "" and column_id > 0 and column_id < len(header_strings)-1:
        separation_columns.append(column_id)

if len(separation_columns) > 0:
    for sep_column_id in separation_columns:
        for line_id, line in enumerate(lines):
            separator = '|' if line[0] == '|' else '+'
            splits = line.split(separator)
            new_splits = splits[:sep_column_id] + [''] + splits[sep_column_id+1:]
            lines[line_id] = separator.join(new_splits)

# Span throughput/latency header columns
for (placeholder, final) in [('$thrghpt', 'Throughput (iter/s)'), ('$latency', 'Latency (ms/iter)')]:
    header_strings = lines[1].split('|')
    for column_id, text in enumerate(header_strings):
        if placeholder in text:
            title_column = header_strings[column_id]
            unit_column = header_strings[column_id + 1]
            previous_length = len(title_column) + len(unit_column) + 1
            new_title = f' {final} '.ljust(previous_length,' ')
            lines[1] = '|'.join(header_strings[:column_id] + [new_title] + header_strings[column_id+2:])

 # swap second line of header with automatically added separator
lines[2], lines[3] = lines[3], lines[2]
for (line_number, line) in enumerate(lines):
    if line_number == len(table_data):
        # Add another separation between benchmark items and aggregates
        new_result += lines[-1] + "\n"

    new_result += line + "\n"

if add_note_for_capped_runs or add_note_for_insufficient_pvalue_runs:
    first_column_width = len(lines[1].split('|')[1])
    width_for_note = len(lines[0]) - first_column_width - 5 # 5 for seperators and spaces
    if add_note_for_capped_runs:
        note = '˄' + f' Execution stopped at {new_data["context"]["max_runs"]} runs'
        new_result += '|' + (' Notes '.rjust(first_column_width, ' ')) +  '|| ' + note.ljust(width_for_note, ' ') + '|\n'
    if add_note_for_insufficient_pvalue_runs:
        note = '˅' + ' Insufficient number of runs for p-value calculation'
        new_result += '|' + (' ' * first_column_width) + '|| ' + note.ljust(width_for_note, ' ') + '|\n'
    new_result += lines[-1] + "\n"

result = new_result


# If github_format is set, format the output in the style of a diff file where added lines (starting with +) are
# colored green, removed lines (starting with -) are red, and others (starting with an empty space) are black.
# Because terminaltables (unsurprisingly) does not support this hack, we need to post-process the result string,
# searching for the control codes that define text to be formatted as green or red.

if github_format:
    new_result = '```diff\n'
    green_control_sequence = colored('', 'green')[0:5]
    red_control_sequence = colored('', 'red')[0:5]

    for line in result.splitlines():
        if green_control_sequence + '+' in line:
            new_result += '+'
        elif red_control_sequence + '-' in line:
            new_result += '-'
        else:
            new_result += ' '

        new_result += line + '\n'
    new_result += '```'
    result = new_result

print("")
print(result)
print("")
