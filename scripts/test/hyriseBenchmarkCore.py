import os
import sys
import pexpect
import glob

def close_benchmark(benchmark):
	benchmark.expect(pexpect.EOF, timeout=None)
	benchmark.close()

def check_exit_status(benchmark):
	if benchmark.exitstatus == None:
		sys.exit(benchmark.signalstatus)

def initialize(arguments, benchmark_name):
	if len(sys.argv) == 1:
		print ("Usage: ./scripts/test/" + benchmark_name + "_test.py <build_dir>")
		sys.exit(1)

	if "--table_path" in arguments and not os.path.isdir(arguments["--table_path"].replace("'", "")):
		print ("Cannot find " + arguments["--table_path"] + ". Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)

	if "--query_path" in arguments and not os.path.isdir(arguments["--query_path"].replace("'", "")):
		print ("Cannot find " + arguments["--query_path"] + ". Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)

	build_dir = sys.argv[1]

	concat_arguments = ' '.join(['='.join(map(str, x)) for x in arguments.items()])
	print(concat_arguments)

	benchmark = pexpect.spawn(build_dir + "/" + benchmark_name + " " + concat_arguments, maxread=1000000, timeout=None, dimensions=(200, 64))
	benchmark.logfile = sys.stdout
	return benchmark

# This benchmark tests a binary with two different sets of arguments.
# After the first test, it checks if binary tables and an output file were created.
# After the second test, it checks if all queries were successfully verified with sqlite.
def run_benchmark(arguments1, arguments2, benchmark_name):
	return_error = False

	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments1, benchmark_name)
	close_benchmark(benchmark)
	check_exit_status(benchmark)

	if arguments1["--cache_binary_tables"] == "true" and not glob.glob(arguments1["--table_path"].replace("'", '') + "*.bin"):
		print ("Cannot find binary tables in " + arguments1["--table_path"])
		sys.exit(1)

	if "--output" in arguments1 and not os.path.isfile(arguments1["--output"].replace("'", "")):
		print ("Cannot find output file " + arguments1["--output"])
		sys.exit(1)

	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments2, benchmark_name)
	benchmark.expect("-> Executed")

	if benchmark.before.count('Verification failed'):
		return_error = True

	close_benchmark(benchmark)
	check_exit_status(benchmark)

	if return_error:
		sys.exit(1)
