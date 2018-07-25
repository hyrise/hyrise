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



if(len(sys.argv) != 3):
	print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
	exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

table = BeautifulTable(default_alignment = BeautifulTable.ALIGN_LEFT)
table.column_headers = ["Benchmark", "prev. iter/s", "new iter/s", "change", "p-value"]
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
	average_diff_sum += diff
	diff_formatted = format_diff(diff)

	p_value = ttest_ind(array('d', old['iteration_durations']), array('d', new['iteration_durations']))[1]
	p_values.append(p_value)

	table.append_row([old['name'], str(old['items_per_second']), str(new['items_per_second']), diff_formatted, str(p_value)])

table.append_row(['average', '', '', format_diff(average_diff_sum / len(old_data['benchmarks'])), combine_pvalues(p_values)[1]])

print("")
print(table)
print("")
