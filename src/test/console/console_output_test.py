#!/usr/local/bin/python3
# change

import os
import sys
import time
import pexpect

def main():
	if len(sys.argv) == 1:
		print ("Usage: ./console_output_test.py <absolute_console_path>")
		return

	console_path = sys.argv[1]
	delimiter = "hyrise/"
	hyrise_path = console_path.split(delimiter)[0] + delimiter
	
	console = pexpect.spawn(console_path, timeout=5, dimensions=(1920,1080))
	console.logfile = open('mylogfilename', 'wb')

	# Test print command
	console.sendline("print test")
	console.expect("Exception thrown while loading table:")
	
	# Test load command
	console.sendline("load " + hyrise_path + "resources/test_data/tbl/10_ints.tbl test")
	console.expect('Loading .*tbl/10_ints.tbl into table "test"')
	console.expect('Encoding "test" using Unencoded')

	# Test sql statement
	console.sendline("select sum(a) from test")
	console.expect("786")
	console.expect("1 rows total")

	# Test TPCH generation
	console.sendline("generate_tpch 0.001")
	console.expect("Generating tables done")

	# Test TPCH tables
	console.sendline("select * from nation")
	console.expect("25 rows total")

	console.logfile.close()

	return

if __name__ == '__main__':
	main()