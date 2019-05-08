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

	console_path = sys.argv[1]
	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))
	return console, console_path

def close_console(console):
	time.sleep(1)
	console.close()

def check_exit_status(console):
	if console.exitstatus == None:
		sys.exit(console.signalstatus)

def main():
	console, console_path = initialize()

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

	# Test exit command
	console.sendline("exit")
	console.expect("Bye.")

	close_console(console)
	check_exit_status(console)

	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))
	console.sendline("load resources/test_data/tbl/10_ints.tbl test")
	console.sendline("select sum(a) from test")

	# Test visualize command
	console.sendline("visualize")

	# The emulated terminal freezes because of is_iterm2.sh in visualize command.
	# Therefore, we don't exit cleanly.
	close_console(console)

	timeout = 0
	while not os.path.exists('.pqp.dot') and timeout < 10:
		time.sleep(1)
		timeout += 1

	with open('.pqp.dot') as f: s = f.readlines()

	timeout = 0
	while len(s) < 6 and timeout < 10:
		with open('.pqp.dot') as f: s = f.readlines()
		timeout += 1

	# Start new console because is_iterm2.sh called by visualize command freezes emulated terminal
	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))

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
