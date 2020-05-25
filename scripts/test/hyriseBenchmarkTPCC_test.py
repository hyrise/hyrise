#!/usr/bin/env python3

from hyriseBenchmarkCore import *
from compareBenchmarkScriptTest import *

COMPARE_BENCHMARKS_PATH = f'{sys.argv[1]}/../scripts/compare_benchmarks.py'

def main():

  # Not explicitly setting all parameters and not testing all lines of the output. Many are tested in the TPCH test
  # and we want to avoid duplication. First test single-threaded, then multi-threaded, followed by a third run for
  # compare_benchmark script tests.

  output_filename_1 = "tpcc_output_1.json"

  arguments = {}
  arguments["--scale"] = "2"
  arguments["--time"] = "30"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)

  benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
  benchmark.expect_exact("TPC-C scale factor (number of warehouses) is 2")
  benchmark.expect_exact("Consistency checks passed")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  arguments = {}
  arguments["--scale"] = "1"
  arguments["--time"] = "60"
  arguments["--consistency_checks"] = "true"
  arguments["--scheduler"] = "true"
  arguments["--clients"] = "10"
  arguments["--output"] = output_filename_1

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)

  benchmark.expect_exact(f"Writing benchmark results to '{output_filename_1}'")
  benchmark.expect_exact("Running in multi-threaded mode using all available cores")
  benchmark.expect_exact("10 simulated clients are scheduling items in parallel")
  benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
  benchmark.expect_exact("TPC-C scale factor (number of warehouses) is 1")
  benchmark.expect_exact("Results for Delivery")
  benchmark.expect_exact("-> Executed")
  benchmark.expect_exact("Results for New-Order")
  benchmark.expect_exact("-> Executed")
  benchmark.expect_exact("Results for Order-Status")
  benchmark.expect_exact("-> Executed")
  benchmark.expect_exact("Results for Payment")
  benchmark.expect_exact("-> Executed")
  benchmark.expect_exact("Results for Stock-Level")
  benchmark.expect_exact("-> Executed")
  benchmark.expect_exact("Consistency checks passed")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  output_filename_2 = "tpcc_output_2.json"

  arguments = {}
  arguments["--scale"] = "1"
  arguments["--time"] = "30"
  arguments["--clients"] = "1"
  arguments["--output"] = output_filename_2

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)
  benchmark.expect_exact(f"Writing benchmark results to '{output_filename_2}'")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  CompareBenchmarkScriptTest(COMPARE_BENCHMARKS_PATH, output_filename_1, output_filename_2).run()


if __name__ == '__main__':
  main()
