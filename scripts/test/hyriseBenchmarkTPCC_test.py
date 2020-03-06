#!/usr/bin/python

from hyriseBenchmarkCore import *

def main():

  # Not explicitly setting all parameters and not testing all lines of the output. Many are tested in the TPCH test
  # and we want to avoid duplication. First test single-threaded, then multi-threaded.

  return_error = False

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

  if benchmark.before.count('Verification failed'):
    return_error = True

  arguments = {}
  arguments["--scale"] = "1"
  arguments["--time"] = "60"
  arguments["--consistency_checks"] = "true"
  arguments["--scheduler"] = "true"
  arguments["--clients"] = "10"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)

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

  if benchmark.before.count('Verification failed'):
    return_error = True

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
