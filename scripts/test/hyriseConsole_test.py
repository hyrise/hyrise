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
	build_dir = sys.argv[1]
	lib_suffix = ""

	if sys.platform.startswith("linux"):
		lib_suffix = ".so"
	elif sys.platform.startswith("darwin"):
		lib_suffix = ".dylib"

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

	# Reload table with a specified encoding and check meta tables for applied encoding
	console.sendline("load resources/test_data/bin/float.bin test_bin RunLength")
	console.expect('Loading .*bin/float.bin into table "test_bin"')
	console.expect('Table "test_bin" already existed. Replacing it.')
	console.expect('Encoding "test_bin" using RunLength')
	console.sendline("select encoding_type from meta_segments where table_name='test_bin' and chunk_id=0 and column_id=0")
	console.expect('RunLength')

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

	# Test meta table modification
	console.sendline("insert into meta_settings values ('foo', 'bar', 'baz')")
	console.expect("Invalid input error: Cannot insert into meta_settings")
	console.sendline("select * from meta_plugins")
	console.expect("0 rows total")
	console.sendline("insert into meta_plugins values ('" + build_dir + "/lib/libhyriseTestPlugin" + lib_suffix + "')")
	console.sendline("select * from meta_plugins")
	console.expect("hyriseTestPlugin")

	# Test exit command
	console.sendline("exit")
	console.expect("Bye.")

	close_console(console)
	check_exit_status(console)

if __name__ == '__main__':
	main()
