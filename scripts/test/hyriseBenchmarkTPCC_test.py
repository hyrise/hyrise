#!/usr/bin/python

from hyriseBenchmarkCore import *

def main():

  # Not explicitly setting all parameters and not testing all lines of the output. Many are tested in the TPCH test
  # and we want to avoid duplication.

  return_error = False

  arguments = {}
  arguments["--scale"] = "2"
  arguments["--time"] = "30"
  arguments["--runs"] = "100"
  arguments["--output"] = "'json_output.txt'"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)

  benchmark.expect("Running benchmark in 'Shuffled' mode")
  benchmark.expect("TPC-C scale factor (number of warehouses) is 2")
  benchmark.expect("Results for Delivery")
  benchmark.expect("Results for New-Order")
  benchmark.expect("Results for Order-Status")
  benchmark.expect("Results for Payment")
  benchmark.expect("Results for Stock-Level")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if benchmark.before.count('Verification failed'):
    return_error = True

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
