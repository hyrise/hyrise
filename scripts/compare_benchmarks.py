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
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green')
	else:
		return colored("{0:.0%}".format(diff), 'red')

def calculate_and_format_p_value(old, new):
	p_value = ttest_ind(array('d', old['iteration_durations']), array('d', new['iteration_durations']))[1]
	is_significant = p_value < p_value_significance_threshold

	notes = ""
	old_runtime = sum(runtime for runtime in old['iteration_durations'])
	new_runtime = sum(runtime for runtime in new['iteration_durations'])
	if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
		is_significant = False
		notes += "(run time too short) "

	if (len(old['iteration_durations']) < min_iterations or len(new['iteration_durations']) < min_iterations):
		is_significant = False
		notes += "(not enough runs) "

	color = 'green' if is_significant else 'white'
	return colored(notes + "{0:.4f}".format(p_value), color)

def print_usage():
	print("Usage: " + sys.argv[0] + " [--ignore-naming]" + " benchmark1.json benchmark2.json")
	exit()

if len(sys.argv) < 3 or len(sys.argv) > 4:
	print_usage()

if (sys.argv[-2][-5:] != ".json" and sys.argv[-2][-4:] != ".txt") or (sys.argv[-1][-5:] != ".json" and sys.argv[-1][-4:] != ".txt"):
	print_usage()

p_ignore_naming = False

if len(sys.argv) > 3:
	parameters = iter(sys.argv[1:-2])
	try:
		while True:
			p = parameters.next()
			if p == "--ignore-naming":
				p_ignore_naming = True
			else:
				print_usage()
	except StopIteration:
		pass

with open(sys.argv[-2]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[-1]) as new_file:
    new_data = json.load(new_file)

table_data = []
table_data.append(["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change", "p-value (significant if <" + str(p_value_significance_threshold) + ")"])

average_diff_sum = 0.0

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
	if old['name'] != new['name'] and not p_ignore_naming:
		print("Benchmark name mismatch")
		exit()
	if float(old['items_per_second']) > 0.0:
		diff = float(new['items_per_second']) / float(old['items_per_second']) - 1
		average_diff_sum += diff
	else:
		diff = float('nan')

	diff_formatted = format_diff(diff)
	p_value_formatted = calculate_and_format_p_value(old, new)

	if p_ignore_naming:
		naming = "{} -> {}".format(old['name'], new['name'])
	else:
		naming = old['name']

	table_data.append([naming, str(old['items_per_second']), str(old['iterations']), str(new['items_per_second']), str(new['iterations']), diff_formatted, p_value_formatted])

table_data.append(['average', '', '', '', '', format_diff(average_diff_sum / len(old_data['benchmarks'])), ''])

table = AsciiTable(table_data)
table.justify_columns[6] = 'right'

print("")
print(table.table)
print("")
