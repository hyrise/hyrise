#!/usr/bin/python

from hyriseBenchmarkUtilities import *

def main():

	arguments1 = {}
	arguments1["--table_path"] = "'resources/test_data/imdb_sample/'"
	arguments1["--queries"] = "'21c,22b,23c,24a'"
	arguments1["--time"] = "1"
	arguments1["--runs"] = "100"
	arguments1["--mode"] = "'PermutedQuerySet'"
	arguments1["--encoding"] = "'Unencoded'"
	arguments1["--scheduler"] = "true"
	arguments1["--clients"] = "4"
	arguments1["--mvcc"] = "true"
	arguments1["--visualize"] = "true"
	arguments1["--cache_binary_tables"] = "true"

	arguments2 = {}
	arguments2["--table_path"] = "'resources/test_data/imdb_sample/'"
	arguments2["--queries"] = "'3b'"
	arguments2["--time"] = "1"
	arguments2["--runs"] = "100"
	arguments2["--warmup"] = "1"
	arguments2["--encoding"] = "'LZ4'"
	arguments2["--compression"] = "'Fixed-size byte-aligned'"
	arguments2["--verify"] = "true"

	run_benchmark(arguments1, arguments2, "hyriseBenchmarkJoinOrder", "third_party/join-order-benchmark")

if __name__ == '__main__':
	main()
