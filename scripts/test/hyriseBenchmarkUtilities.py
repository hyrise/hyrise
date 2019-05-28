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

def initialize(arguments, benchmark_name, additional_directory):
	if len(sys.argv) == 1:
		print ("Usage: ./scripts/test/" + benchmark_name + "_test.py <build_dir>")
		sys.exit(1)

	if additional_directory and not os.path.isdir(additional_directory):
		print ("Cannot find" + additional_directory + ". Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)

	build_dir = sys.argv[1]

	concat_arguments = ' '.join(['='.join(map(str, x)) for x in arguments.items()])
	print(concat_arguments)

	benchmark = pexpect.spawn(build_dir + "/" + benchmark_name + " " + concat_arguments, maxread=1000000,timeout=None, dimensions=(200, 64))
	benchmark.logfile = sys.stdout
	return benchmark

# This benchmark tests a binary with two different sets of arguments.
# After the first test, it checks if binary tables were created.
# After the second test, it checks if all benchmarks were successfully verified.
def run_benchmark(arguments1, arguments2, benchmark_name, additional_directory=None):
	return_error = False
	
	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments1, benchmark_name, additional_directory)
	close_benchmark(benchmark)
	check_exit_status(benchmark)

	if arguments1["--cache_binary_tables"] == "true" and not glob.glob(arguments1["--table_path"].replace("'", '') + "*.bin"):
		print(arguments1["--table_path"].replace("'", '') + "*.bin")
		sys.exit(1)

	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments2, benchmark_name, additional_directory)
	benchmark.expect("-> Executed")

	if benchmark.before.count('Verification failed'):
		return_error = True

	close_benchmark(benchmark)
	check_exit_status(benchmark)

	if return_error:
		sys.exit(1)
