#!/usr/bin/env python3

from hyriseBenchmarkCore import close_benchmark, check_exit_status, initialize, run_benchmark


def main():
    build_dir = initialize()

    # Run SSB, validate its output using pexpect, and check if all queries were successfully verified with sqlite.
    arguments = {}
    arguments["--queries"] = "'1.1,1.2,2.2,3.3'"
    arguments["--scale"] = "0.01"
    arguments["--time"] = "10"
    arguments["--runs"] = "20"
    arguments["--mode"] = "'Ordered'"
    arguments["--encoding"] = "'Unencoded'"
    arguments["--clients"] = "1"
    arguments["--scheduler"] = "false"
    arguments["--verify"] = "true"

    # Binary tables would be written into the table_path. In CI, this path is shared by different targets that are
    # potentially executed concurrently. This sometimes led to issues with corrupted binary files.
    arguments["--dont_cache_binary_tables"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkStarSchema", True)

    benchmark.expect_exact("- Running in single-threaded mode")
    benchmark.expect_exact("- Data preparation will use all available cores")
    benchmark.expect_exact("- 1 simulated client is scheduling items")
    benchmark.expect_exact("- Running benchmark in 'Ordered' mode")
    benchmark.expect_exact("- Encoding is 'Unencoded'")
    benchmark.expect_exact("- Chunk size is 65535")
    benchmark.expect_exact("- Max runs per item is 20")
    benchmark.expect_exact("- Max duration per item is 10 seconds")
    benchmark.expect_exact("- No warmup runs are performed")
    benchmark.expect_exact(
        "- Automatically verifying results with SQLite. This will make the performance numbers invalid."
    )
    benchmark.expect_exact("- Not caching tables as binary files")
    benchmark.expect_exact("- Not tracking SQL metrics")
    benchmark.expect_exact("- Running subset of queries: 1.1,1.2,2.2,3.3")
    benchmark.expect_exact("- SSB scale factor is 0.01")
    benchmark.expect_exact("- Using SSB dbgen")

    close_benchmark(benchmark)
    check_exit_status(benchmark)


if __name__ == "__main__":
    main()
