#!/usr/bin/env python

import json
import sys
from beautifultable import BeautifulTable
from termcolor import colored
from scipy.stats import ttest_ind, combine_pvalues
from array import array

def format_diff(diff):
	if diff >= 0:
		return colored('+' + "{0:.0%}".format(diff), 'green')
	else:
		return colored("{0:.0%}".format(diff), 'red')

def format_p_value(p_value):
	if p_value < 0.05:
		return colored("{0:.5}".format(p_value), 'green')
	else:
		return colored("{0:.5}".format(p_value), 'red')

if(len(sys.argv) != 3):
	print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
	exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

table = BeautifulTable(default_alignment = BeautifulTable.ALIGN_LEFT, max_width=100)
table.column_headers = ["Benchmark", "prev. iter/s", "new iter/s", "change", "p-value"]
table.column_widths = [10, 10, 10, 20, 20]
table.width_exceed_policy = BeautifulTable.WEP_ELLIPSIS
table.row_separator_char = ''
table.top_border_char = ''
table.bottom_border_char = ''
table.left_border_char =''
table.right_border_char =''

average_diff_sum = 0.0
p_values = []

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
	if old['name'] != new['name']:
		print("Benchmark name mismatch")
		exit()
	diff = float(new['items_per_second']) / float(old['items_per_second']) - 1
	old_rsd = "{0:.0%}".format(old['real_time_per_iteration_relative_standard_deviation'])
	new_rsd = "{0:.0%}".format(new['real_time_per_iteration_relative_standard_deviation'])
	average_diff_sum += diff
	diff_formatted = format_diff(diff)

	p_value = ttest_ind(array('d', old['iteration_durations']), array('d', new['iteration_durations']))[1]
	p_value_formatted = format_p_value(p_value)
	p_values.append(p_value)


	table.append_row([old['name'], str(old['items_per_second']), str(new['items_per_second']), diff_formatted, p_value_formatted])

p_values_combined = combine_pvalues(p_values)[1]
p_values_combined_formatted = format_p_value(p_values_combined)
table.append_row(['average', '', '', format_diff(average_diff_sum / len(old_data['benchmarks'])), p_values_combined_formatted])

print("")
print(table)
print("")
