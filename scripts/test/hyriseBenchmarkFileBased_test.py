#!/usr/bin/python

from hyriseBenchmarkCore import *

def main():

	arguments1 = {}
	arguments1["--table_path"] = "'resources/test_data/tbl/file_based/'"
	arguments1["--query_path"] = "'resources/test_data/queries/file_based/'"
	arguments1["--queries"] = "'select_statement'"
	arguments1["--time"] = "1"
	arguments1["--runs"] = "100"
	arguments1["--output"] = "'json_output.txt'"
	arguments1["--mode"] = "'PermutedQuerySet'"
	arguments1["--encoding"] = "'Unencoded'"
	arguments1["--scheduler"] = "true"
	arguments1["--clients"] = "4"
	arguments1["--mvcc"] = "true"
	arguments1["--visualize"] = "true"
	arguments1["--cache_binary_tables"] = "true"

	arguments2 = {}
	arguments2["--table_path"] = "'resources/test_data/tbl/file_based/'"
	arguments2["--query_path"] = "'resources/test_data/queries/file_based/'"
	arguments2["--queries"] = "'select_statement'"
	arguments2["--time"] = "1"
	arguments2["--runs"] = "100"
	arguments2["--warmup"] = "1"
	arguments2["--encoding"] = "'LZ4'"
	arguments2["--compression"] = "'SIMD-BP128'"
	arguments2["--verify"] = "true"

	run_benchmark(arguments1, arguments2, "hyriseBenchmarkFileBased")

if __name__ == '__main__':
	main()
