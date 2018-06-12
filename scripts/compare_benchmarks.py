import json
import sys
from beautifultable import BeautifulTable
from termcolor import colored

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
table.column_headers = ["Benchmark", "prev. iter/s", "new iter/s", "change"]
table.row_separator_char = ''
table.top_border_char = ''
table.bottom_border_char = ''
table.left_border_char =''
table.right_border_char =''

average_diff_sum = 0.0

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
	if old['name'] != new['name']:
		print("Benchmark name mismatch")
		exit()
	diff = float(new['items_per_second']) / float(old['items_per_second']) - 1
	average_diff_sum += diff
	diff_formatted = format_diff(diff)
	table.append_row([old['name'], old['items_per_second'], new['items_per_second'], diff_formatted])

table.append_row(['average', '', '', format_diff(average_diff_sum / len(old_data['benchmarks']))])

print("")
print(table)
print("")
