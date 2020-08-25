#!/usr/bin/env python3

from compareBenchmarkScriptTest import CompareBenchmarkScriptTest
from hyriseBenchmarkCore import close_benchmark, check_exit_status, initialize, run_benchmark


def main():
    build_dir = initialize()
    compare_benchmarks_path = f"{build_dir}/../scripts/compare_benchmarks.py"
    output_filename_1 = f"{build_dir}/tpcc_output_1.json"

    # Not explicitly setting all parameters and not testing all lines of the output. Many are tested in the TPCH test
    # and we want to avoid duplication. First TPC-C execution is single-threaded, second is multi-threaded. The third
    # execution is done to run the compare_benchmark script tests.
    arguments = {}
    arguments["--scale"] = "2"
    arguments["--time"] = "30"
    arguments["--verify"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCC", True)

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

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCC", True)

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

    output_filename_2 = f"{build_dir}/tpcc_output_2.json"

    arguments = {}
    arguments["--scale"] = "1"
    arguments["--time"] = "30"
    arguments["--clients"] = "1"
    arguments["--output"] = output_filename_2

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCC", True)
    benchmark.expect_exact(f"Writing benchmark results to '{output_filename_2}'")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    CompareBenchmarkScriptTest(compare_benchmarks_path, output_filename_1, output_filename_2).run()


if __name__ == "__main__":
    main()
