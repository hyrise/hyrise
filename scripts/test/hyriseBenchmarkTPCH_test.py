#!/usr/bin/env python3

import json
import os
import sys

from compareBenchmarkScriptTest import CompareBenchmarkScriptTest
from hyriseBenchmarkCore import close_benchmark, check_exit_status, check_json, initialize, run_benchmark


def main():
    build_dir = initialize()
    compare_benchmarks_path = f"{build_dir}/../scripts/compare_benchmarks.py"
    output_filename = f"{build_dir}/tpch_output.json"

    return_error = False

    # First, run TPC-H and validate it using pexpect. After this run, check if an output file was created and if it
    # matches the arguments.
    arguments = {}
    arguments["--scale"] = ".01"
    arguments["--use_prepared_statements"] = "true"
    arguments["--queries"] = "'1,13,19'"
    arguments["--time"] = "10"
    arguments["--runs"] = "-1"
    arguments["--mode"] = "'Shuffled'"
    arguments["--encoding"] = "'Dictionary'"
    arguments["--compression"] = "'Fixed-size byte-aligned'"
    arguments["--indexes"] = "true"
    arguments["--scheduler"] = "false"
    arguments["--clients"] = "1"
    arguments["--dont_cache_binary_tables"] = "true"
    arguments["--output"] = output_filename

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCH", True)

    benchmark.expect_exact(f"Writing benchmark results to '{output_filename}'")
    benchmark.expect_exact("Running in single-threaded mode")
    benchmark.expect_exact("1 simulated client is scheduling items")
    benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
    benchmark.expect_exact("Encoding is 'Dictionary'")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("No warmup runs are performed")
    benchmark.expect_exact("Not caching tables as binary files")
    benchmark.expect_exact("Benchmarking Queries: [ 1, 13, 19 ]")
    benchmark.expect_exact("TPC-H scale factor is 0.01")
    benchmark.expect_exact("Using prepared statements: yes")
    benchmark.expect_exact("Creating index on customer [ c_custkey ]")
    benchmark.expect_exact("Preparing queries")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    if not os.path.isfile(arguments["--output"].replace("'", "")):
        print("ERROR: Cannot find output file " + arguments["--output"])
        return_error = True

    with open(arguments["--output"].replace("'", "")) as f:
        output = json.load(f)

    return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
    return_error = check_json(
        output["context"]["scale_factor"],
        float(arguments["--scale"]),
        "Scale factor doesn't match with JSON:",
        return_error,
        0.001,
    )
    for i in range(0, 3):
        return_error = check_json(
            output["benchmarks"][i]["name"].replace("TPC-H 0", "").replace("TPC-H ", ""),
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
        output["context"]["encoding"]["default"]["compression"],
        arguments["--compression"].replace("'", ""),
        "Compression doesn't match with JSON:",
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

    CompareBenchmarkScriptTest(compare_benchmarks_path, output_filename, output_filename).run()

    # Run TPC-H and validate its output using pexpect and check if all queries were successfully verified with sqlite.
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
    benchmark.expect_exact("TPC-H scale factor is 0.01")
    benchmark.expect_exact("Using prepared statements: no")
    benchmark.expect_exact("Multi-threaded Topology:")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    # Run TPC-H and create query plan visualizations. Test that pruning works end-to-end, that is from the command line
    # parameter all the way to the visualizer.
    arguments = {}
    arguments["--scale"] = ".01"
    arguments["--chunk_size"] = "10000"
    arguments["--queries"] = "'6'"
    arguments["--runs"] = "1"
    arguments["--visualize"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCH", True)

    benchmark.expect_exact("Visualizing the plans into SVG files. This will make the performance numbers invalid.")
    benchmark.expect_exact("Chunk size is 10000")
    benchmark.expect_exact("Benchmarking Queries: [ 6 ]")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    visualization_file = "TPC-H_06-PQP.svg"
    if not os.path.isfile(visualization_file):
        print("ERROR: Cannot find visualization file " + visualization_file)
        sys.exit(1)
    with open(visualization_file) as f:
        # Check whether the (a) the GetTable node exists and (b) the chunk count is correct for the given scale factor
        if "/7 chunk(s)" not in f.read():
            print("ERROR: Did not find expected pruning information in the visualization file")
            sys.exit(1)

    if return_error:
        sys.exit(1)

    # The next two TPC-H runs are executed to create output files with which we check the output of the
    # compare_benchmark.py script.
    output_filename_1 = f"{build_dir}/tpch_output_1.json"
    output_filename_2 = f"{build_dir}/tpch_output_2.json"

    arguments = {}
    arguments["--scale"] = ".01"
    arguments["--chunk_size"] = "10000"
    arguments["--queries"] = "'2,6,15'"
    arguments["--runs"] = "10"
    arguments["--output"] = output_filename_1
    arguments["--dont_cache_binary_tables"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCH", True)
    benchmark.expect_exact(f"Writing benchmark results to '{output_filename_1}'")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    arguments["--output"] = output_filename_2
    arguments["--scheduler"] = True
    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkTPCH", True)
    benchmark.expect_exact(f"Writing benchmark results to '{output_filename_2}'")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    CompareBenchmarkScriptTest(compare_benchmarks_path, output_filename_1, output_filename_2).run()


if __name__ == "__main__":
    main()
