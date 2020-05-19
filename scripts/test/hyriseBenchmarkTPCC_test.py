#!/usr/bin/env python3

from hyriseBenchmarkCore import *

def main():

  # Not explicitly setting all parameters and not testing all lines of the output. Many are tested in the TPCH test
  # and we want to avoid duplication. First test single-threaded, then multi-threaded.

  return_error = False
  output_filename = "'json_output_tpcc.txt'"

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
  arguments["--output"] = output_filename

  benchmark = initialize(arguments, "hyriseBenchmarkTPCC", True)

  benchmark.expect_exact(f"Writing benchmark results to {output_filename}")
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

  # Test that the output of the TPC-C benchmark does not cause crashes in the compare_benchmarks.py script.
  benchmark_comparison = pexpect.spawn(f"scripts/compare_benchmarks.py {output_filename} {output_filename}", maxread=1000000, timeout=2, dimensions=(200, 64))
  benchmark_comparison.expect_exact(["warmup_duration", "Latency (ms/iter)", "New-Order", "Geomean", "Sum"])
  close_benchmark(benchmark_comparison)
  check_exit_status(benchmark_comparison)

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
