#!/usr/bin/python

import os
import sys
import pexpect
import time

def check_exit_status(console):
	print(console.exitstatus, console.signalstatus)
	if console.exitstatus:
		sys.exit(console.exitstatus)
	else:
		sys.exit(console.signalstatus)

def main():
	if len(sys.argv) == 1:
		print ("Usage: ./scripts/test/hyriseConsole_test.py <build_dir>")
		sys.exit(1)

	if not os.path.isdir("resources/test_data/tbl"):
		print ("Cannot find resources/test_data/tbl. Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)
 

	console_path = sys.argv[1]	
	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))

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

	console.sendline("exit")
	console.expect("Bye.")
	time.sleep(1)
	console.close()

	check_exit_status(console)
	#print(os.WIFEXITED(console.status), os.WEXITSTATUS(console.status),os.WIFSIGNALED(console.status), os.WTERMSIG(console.status), os.WIFSTOPPED(console.status), os.WSTOPSIG(console.status))	

	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))
	console.logfile = open('logfile', 'wb')

	# Test visualize command
	console.sendline("visualize")

	while not os.path.exists('.pqp.dot'):
		print("test")
		time.sleep(1)

	with open('.pqp.dot') as f: s = f.read()

	while len(s) < 6:
		print("test2")
		with open('.pqp.dot') as f: s = f.read()

	console.logfile.close()

	# Start new console because is_iterm2.sh called by visualize command freezes emulated terminal
	console = pexpect.spawn(console_path + "/hyriseConsole", timeout=15, dimensions=(200, 64))

	# Test TPCH generation
	console.sendline("generate_tpch 0.001")
	console.expect("Generating tables done")

	# Test TPCH tables
	console.sendline("select * from nation")
	console.expect("25 rows total")

	check_exit_status(console)

if __name__ == '__main__':
	main()
