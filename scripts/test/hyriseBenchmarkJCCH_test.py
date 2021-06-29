#!/usr/bin/env python3

from hyriseBenchmarkCore import close_benchmark, check_exit_status, initialize, run_benchmark


def main():
    build_dir = initialize()

    # Run JCC-H and validate its output using pexpect and check if all queries were successfully verified with sqlite.
    arguments = {}
    arguments["--scale"] = ".01"
    arguments["--chunk_size"] = "10000"
    arguments["--queries"] = "'2,4,6'"
    arguments["--time"] = "10"
    arguments["--runs"] = "100"
    arguments["--warmup"] = "10"
    arguments["--encoding"] = "'LZ4'"
    arguments["--compression"] = "'SIMD-BP128'"
    arguments["--indexes"] = "false"
    arguments["--scheduler"] = "true"
    arguments["--clients"] = "4"
    arguments["--jcch"] = "skewed"
    arguments["--verify"] = "true"
    arguments["--dont_cache_binary_tables"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCH", True)

    benchmark.expect_exact("Running in multi-threaded mode using all available cores")
    benchmark.expect_exact("4 simulated clients are scheduling items in parallel")
    benchmark.expect_exact("Running benchmark in 'Ordered' mode")
    benchmark.expect_exact("Encoding is 'LZ4'")
    benchmark.expect_exact("Chunk size is 10000")
    benchmark.expect_exact("Max runs per item is 100")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("Warmup duration per item is 10 seconds")
    benchmark.expect_exact("Benchmarking Queries: [ 2, 4, 6 ]")
    benchmark.expect_exact("JCC-H scale factor is 0.01")
    benchmark.expect_exact("Using prepared statements: no")
    benchmark.expect_exact("Using JCC-H dbgen from")
    benchmark.expect_exact("JCC-H query parameters are skewed")
    benchmark.expect_exact("calling external qgen")
    benchmark.expect_exact("Multi-threaded Topology:")

    close_benchmark(benchmark)
    check_exit_status(benchmark)


if __name__ == "__main__":
    main()
