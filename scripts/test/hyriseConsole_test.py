#!/usr/bin/python

import os
import sys
import pexpect
import time

def initialize():
	if len(sys.argv) == 1:
		print ("Usage: ./scripts/test/hyriseConsole_test.py <build_dir>")
		sys.exit(1)

	if not os.path.isdir("resources/test_data/tbl"):
		print ("Cannot find resources/test_data/tbl. Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)

	env = {"HYRISE_DISABLE_ITERM_CHECK": "1"}
	console_path = sys.argv[1]
	console = pexpect.spawn(console_path + "/hyriseConsole", env=env, timeout=15, dimensions=(200, 64))
	return console

def close_console(console):
	time.sleep(1)
	console.close()

def check_exit_status(console):
	if console.exitstatus == None:
		sys.exit(console.signalstatus)

def main():
	console = initialize()

	# Test print command
	console.sendline("print test")
	console.expect("Exception thrown while loading table:")
	
	# Test load command
	console.sendline("load resources/test_data/tbl/10_ints.tbl test")
	console.expect('Loading .*tbl/10_ints.tbl into table "test"')
	console.expect('Encoding "test" using Unencoded')

	# Test SQL statement
	console.sendline("select sum(a) from test")
	console.expect("786")
	console.expect("Execution info:")

	# Test visualize command
	console.sendline("visualize")
	console.expect("Currently, only iTerm2 can print the visualization inline. You can find the plan at")

	with open('.pqp.dot') as f: s = f.readlines()

	if len(s) < 6:
		sys.exit(1)

	# Test TPCH generation
	console.sendline("generate_tpch 0.001")
	console.expect("Generating tables done")

	# Test TPCH tables
	console.sendline("select * from nation")
	console.expect("25 rows total")

	# Test exit command
	console.sendline("exit")
	console.expect("Bye.")

	close_console(console)
	check_exit_status(console)

if __name__ == '__main__':
	main()
