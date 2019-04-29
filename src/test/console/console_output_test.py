#!/usr/local/bin/python3
# change

import os
import sys
import time

try:
	import pexpect
except ImportError as e:
	print("Pexpect is not installed.")
	print("Please run 'pip install --user -r requirements.txt' in the Hyrise root directory.")
	sys.exit(1)

def main():
	if len(sys.argv) == 1:
		print ("Usage: ./console_output_test.py <absolute_console_path>")
		return

	console_path = sys.argv[1]
	delimiter = "hyrise/"
	hyrise_path = console_path.split(delimiter)[0] + delimiter
	
	console = pexpect.spawn(console_path, timeout=10)

	# test print
	console.sendline("print test")
	console.expect("Exception thrown while loading table:")
	
	# test load
	console.sendline("load " + hyrise_path + "resources/test_data/tbl/10_ints.tbl test")
	console.expect('Loading .*tbl/10_ints.tbl into table "test"')
	console.expect('Encoding "test" using Unencoded')

	# test sql statement
	console.sendline("select sum(a) from test")
	console.expect("786")
	console.expect("1 rows total")

	# test visualize
	console.sendline("visualize")
	print(console.before)
	print(console.after)
	time.wait(2000)
	# console.expect("iTerm2")
	with open(hyrise_path + '.pqp.dot') as f: s = f.read()

	if len(f) <= 5:
		return 1

	# test tpch generation
	console.sendline("generate_tpch 0.001")
	console.expect("Generating all TPCH tables \(this might take a while\) ...")

	# test tpch tables
	console.sendline("select * from nation")
	console.expect("25 rows total")

	return

if __name__ == '__main__':
	main()