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

	build_dir = sys.argv[1]
	console = pexpect.spawn(build_dir + "/hyriseConsole", timeout=60, dimensions=(200, 64))
	return console

def close_console(console):
	console.expect(pexpect.EOF)
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

	console.sendline("load resources/test_data/bin/float.bin test_bin")
	console.expect('Loading .*bin/float.bin into table "test_bin"')
	console.expect('Encoding "test_bin" using Unencoded')	

	# Test SQL statement
	console.sendline("select sum(a) from test")
	console.expect("786")
	console.expect("Execution info:")

	# Test TPCH generation
	console.sendline("generate_tpch     0.01   7")
	console.expect("Generating tables done")

	# Test TPCH tables
	console.sendline("select * from nation")
	console.expect("25 rows total")

	# Test correct chunk size (25 nations, independent of scale factor, with a maximum chunk size of 7 result in 4 chunks)
	console.sendline("print nation")
	console.expect("=== Chunk 3 ===")

	# Test exit command
	console.sendline("exit")
	console.expect("Bye.")

	close_console(console)
	check_exit_status(console)

if __name__ == '__main__':
	main()
