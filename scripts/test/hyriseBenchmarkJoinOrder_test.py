#!/usr/bin/env python3

import json
import os
import sys

from hyriseBenchmarkCore import close_benchmark, check_exit_status, check_json, initialize, run_benchmark


# This test runs the binary hyriseBenchmarkJoinOrder with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():
    build_dir = initialize()

    return_error = False

    arguments = {}
    arguments["--table_path"] = "'resources/test_data/imdb_sample/'"
    arguments["--queries"] = "'21c,22b,23c,24a'"
    arguments["--time"] = "10"
    arguments["--runs"] = "100"
    arguments["--output"] = "'json_output.txt'"
    arguments["--mode"] = "'Shuffled'"
    arguments["--encoding"] = "'Unencoded'"
    arguments["--clients"] = "1"
    arguments["--scheduler"] = "false"

    # Binary tables would be written into the table_path. In CI, this path is shared by different targets that are
    # potentially executed concurrently. This sometimes led to issues with corrupted binary files.
    arguments["--dont_cache_binary_tables"] = "true"

    os.system(f'rm -rf {arguments["--table_path"]}/*.bin')

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkJoinOrder", True)

    benchmark.expect_exact("Writing benchmark results to 'json_output.txt'")
    benchmark.expect_exact("Running in single-threaded mode")
    benchmark.expect_exact("1 simulated client is scheduling items")
    benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
    benchmark.expect_exact("Encoding is 'Unencoded'")
    benchmark.expect_exact("Max runs per item is 100")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("No warmup runs are performed")
    benchmark.expect_exact("Not caching tables as binary files")
    benchmark.expect_exact("Retrieving the IMDB dataset.")
    benchmark.expect_exact("IMDB setup already complete, no setup action required")
    benchmark.expect_exact("Benchmarking queries from third_party/join-order-benchmark")
    benchmark.expect_exact("Running on tables from resources/test_data/imdb_sample/")
    benchmark.expect_exact("Running subset of queries: 21c,22b,23c,24a")
    benchmark.expect_exact("-> Executed")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    if not os.path.isfile(arguments["--output"].replace("'", "")):
        print("ERROR: Cannot find output file " + arguments["--output"])
        return_error = True

    with open(arguments["--output"].replace("'", "")) as f:
        output = json.load(f)

    return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
    for i in range(0, 4):
        return_error = check_json(
            output["benchmarks"][i]["name"],
            arguments["--queries"].replace("'", "").split(",")[i],
            "Query doesn't match with JSON:",
            return_error,
        )
    return_error = check_json(
        output["context"]["max_duration"],
        int(arguments["--time"]) * 1e9,
        "Max duration doesn't match with JSON:",
        return_error,
    )
    return_error = check_json(
        output["context"]["max_runs"], int(arguments["--runs"]), "Max runs don't match with JSON:", return_error
    )
    return_error = check_json(
        output["context"]["benchmark_mode"],
        arguments["--mode"].replace("'", ""),
        "Benchmark mode doesn't match with JSON:",
        return_error,
    )
    return_error = check_json(
        output["context"]["encoding"]["default"]["encoding"],
        arguments["--encoding"].replace("'", ""),
        "Encoding doesn't match with JSON:",
        return_error,
    )
    return_error = check_json(
        str(output["context"]["using_scheduler"]).lower(),
        arguments["--scheduler"],
        "Scheduler doesn't match with JSON:",
        return_error,
    )
    return_error = check_json(
        output["context"]["clients"], int(arguments["--clients"]), "Client count doesn't match with JSON:", return_error
    )

    os.system(f'rm -rf {arguments["--table_path"]}/*.bin')

    arguments = {}
    arguments["--table_path"] = "'resources/test_data/imdb_sample/'"
    arguments["--time"] = "10"
    arguments["--runs"] = "2"
    arguments["--warmup"] = "2"
    arguments["--encoding"] = "'LZ4'"
    arguments["--compression"] = "'Bit-packing'"
    arguments["--scheduler"] = "true"
    arguments["--clients"] = "4"
    arguments["--chunk_size"] = "100000"
    arguments["--verify"] = "true"
    arguments["--dont_cache_binary_tables"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkJoinOrder", True)

    benchmark.expect_exact("Running in multi-threaded mode using all available cores")
    benchmark.expect_exact("4 simulated clients are scheduling items in parallel")
    benchmark.expect_exact("Running benchmark in 'Ordered' mode")
    benchmark.expect_exact("Encoding is 'LZ4'")
    benchmark.expect_exact("Chunk size is 100000")
    benchmark.expect_exact("Max runs per item is 2")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("Warmup duration per item is 2 seconds")
    benchmark.expect_exact(
        "Automatically verifying results with SQLite. This will make the performance numbers invalid."
    )
    benchmark.expect_exact("Benchmarking queries from third_party/join-order-benchmark")
    benchmark.expect_exact("Running on tables from resources/test_data/imdb_sample/")
    benchmark.expect_exact("Multi-threaded Topology:")
    benchmark.expect_exact("- Warming up for 10a")
    benchmark.expect_exact("- Benchmarking 10a")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    if return_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
