#!/usr/bin/python

from hyriseBenchmarkCore import *

def main():

	arguments1 = {}
	arguments1["--scale"] = ".01"
	arguments1["--use_prepared_statements"] = "true"
	arguments1["--queries"] = "'1,13,19'"
	arguments1["--time"] = "1"
	arguments1["--runs"] = "100"
	arguments1["--output"] = "'json_output.txt'"
	arguments1["--mode"] = "'PermutedQuerySet'"
	arguments1["--compression"] = "'Fixed-size byte-aligned'"
	arguments1["--scheduler"] = "true"
	arguments1["--clients"] = "4"
	arguments1["--mvcc"] = "true"
	arguments1["--visualize"] = "true"
	arguments1["--cache_binary_tables"] = "false"

	arguments2 = {}
	arguments2["--scale"] = ".005"
	arguments2["--queries"] = "'2,4,6'"
	arguments2["--time"] = "1"
	arguments2["--runs"] = "100"
	arguments2["--warmup"] = "1"
	arguments2["--encoding"] = "'LZ4'"
	arguments2["--compression"] = "'SIMD-BP128'"
	arguments2["--verify"] = "true"

	run_benchmark(arguments1, arguments2, "hyriseBenchmarkTPCH")

if __name__ == '__main__':
	main()
