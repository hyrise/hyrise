#!/usr/bin/python

import os
import sys
import pexpect

arguments1 = {}
arguments1["--table_path"] = "'imdb_data/'"
arguments1["--query_path"] = "'third_party/join-order-benchmark/'"
arguments1["--queries"] = "'21c,22b,23c,24a'"
arguments1["--excluded_queries"] = "'fkindexes.sql, schema.sql'"
arguments1["--time"] = "1"
arguments1["--runs"] = "100"
arguments1["--mode"] = "'PermutedQuerySet'"
arguments1["--encoding"] = "'lz4'"
arguments1["--compression"] = "'SIMD-BP128'"
arguments1["--scheduler"] = "true"
arguments1["--clients"] = "4"
arguments1["--mvcc"] = "true"
arguments1["--visualize"] = "true"
arguments1["--cache_binary_tables"] = "true"

arguments2 = {}
arguments2["--table_path"] = "'imdb_data/'"
arguments2["--query_path"] = "'third_party/join-order-benchmark/'"
arguments2["--queries"] = "'3b'"
arguments2["--excluded_queries"] = "'fkindexes.sql, schema.sql'"
arguments2["--time"] = "1"
arguments2["--runs"] = "100"
arguments2["--warmup"] = "1"
arguments2["--compression"] = "'Fixed-size byte-aligned'"
arguments2["--verify"] = "true"

def initialize(arguments):
	if len(sys.argv) == 1:
		print ("Usage: ./scripts/test/hyriseBenchmarkFileBased_test.py <build_dir>")
		sys.exit(1)

	if not os.path.isdir("third_party/join-order-benchmark"):
		print ("Cannot find third_party/join-order-benchmark. Are you running the test suite from the main folder of the Hyrise repository?")
		sys.exit(1)

	build_dir = sys.argv[1]

	concat_arguments = ' '.join(['='.join(map(str, x)) for x in arguments.items()])

	benchmark = pexpect.spawn(build_dir + "/hyriseBenchmarkFileBased " + concat_arguments, maxread=1000000,timeout=None, dimensions=(200, 64))
	benchmark.logfile = sys.stdout
	return benchmark

def close_benchmark(benchmark):
	benchmark.expect(pexpect.EOF, timeout=None)
	benchmark.close()

def check_exit_status(benchmark):
	if benchmark.exitstatus == None:
		sys.exit(benchmark.signalstatus)

def main():
	return_error = False
	
	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments1)
	close_benchmark(benchmark)
	check_exit_status(benchmark)

	os.system("rm -rf imdb_data/*.bin")

	benchmark = initialize(arguments2)
	benchmark.expect("-> Executed")

	if benchmark.before.count('Verification failed'):
		return_error = True

	close_benchmark(benchmark)
	check_exit_status(benchmark)

	if return_error:
		sys.exit(1)

if __name__ == '__main__':
	main()
