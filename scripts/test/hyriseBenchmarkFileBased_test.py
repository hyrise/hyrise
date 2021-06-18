#!/usr/bin/env python3

import json
import os
import sys

from compareBenchmarkScriptTest import CompareBenchmarkScriptTest
from hyriseBenchmarkCore import close_benchmark, check_exit_status, check_json, initialize, run_benchmark


# This test runs the binary hyriseBenchmarkFileBased with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():
    build_dir = initialize()
    compare_benchmarks_path = f"{build_dir}/../scripts/compare_benchmarks.py"
    output_filename_1 = f"{build_dir}/file_based_output_1.json"

    return_error = False

    arguments = {}
    arguments["--table_path"] = "'resources/test_data/tbl/file_based/'"
    arguments["--query_path"] = "'resources/test_data/queries/file_based/'"
    arguments["--queries"] = "'select_statement'"
    arguments["--time"] = "10"
    arguments["--runs"] = "100"
    arguments["--output"] = output_filename_1
    arguments["--mode"] = "'Shuffled'"
    arguments["--encoding"] = "'Unencoded'"
    arguments["--scheduler"] = "false"
    arguments["--clients"] = "1"

    # Binary tables would be written into the table_path. In CI, this path is shared by different targets that are
    # potentially executed concurrently. This sometimes led to issues with corrupted binary files.
    arguments["--dont_cache_binary_tables"] = "true"

    os.system(f'rm -rf {arguments["--table_path"]}/*.bin')

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkFileBased", True)

    benchmark.expect_exact(f"Writing benchmark results to '{output_filename_1}'")
    benchmark.expect_exact("Running in single-threaded mode")
    benchmark.expect_exact("1 simulated client is scheduling items")
    benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
    benchmark.expect_exact("Encoding is 'Unencoded'")
    benchmark.expect_exact("Max runs per item is 100")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("No warmup runs are performed")
    benchmark.expect_exact("Not caching tables as binary files")
    benchmark.expect_exact("Benchmarking queries from resources/test_data/queries/file_based/")
    benchmark.expect_exact("Running on tables from resources/test_data/tbl/file_based/")
    benchmark.expect_exact("Running subset of queries: select_statement")
    benchmark.expect_exact("-> Executed")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    output_filename_2 = f"{build_dir}/file_based_output_1.json"

    # Second run for compare_benchmark.py test
    arguments["--output"] = output_filename_2
    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkFileBased", True)
    benchmark.expect_exact(f"Writing benchmark results to '{output_filename_2}'")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    CompareBenchmarkScriptTest(compare_benchmarks_path, output_filename_1, output_filename_2).run()

    if not os.path.isfile(arguments["--output"].replace("'", "")):
        print("ERROR: Cannot find output file " + arguments["--output"])
        return_error = True

    with open(arguments["--output"].replace("'", "")) as f:
        output = json.load(f)

    return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
    return_error = check_json(
        output["benchmarks"][0]["name"],
        arguments["--queries"].replace("'", "").split(",")[0],
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

    arguments = {}
    arguments["--table_path"] = "'resources/test_data/tbl/file_based/'"
    arguments["--query_path"] = "'resources/test_data/queries/file_based/'"
    arguments["--queries"] = "'select_statement'"
    arguments["--time"] = "10"
    arguments["--runs"] = "100"
    arguments["--warmup"] = "5"
    arguments["--encoding"] = "'LZ4'"
    arguments["--compression"] = "'Bit-packing'"
    arguments["--scheduler"] = "true"
    arguments["--clients"] = "4"
    arguments["--verify"] = "true"
    arguments["--dont_cache_binary_tables"] = "true"

    benchmark = run_benchmark(build_dir, arguments, "hyriseBenchmarkFileBased", True)

    benchmark.expect_exact("Running in multi-threaded mode using all available cores")
    benchmark.expect_exact("4 simulated clients are scheduling items in parallel")
    benchmark.expect_exact("Running benchmark in 'Ordered' mode")
    benchmark.expect_exact("Encoding is 'LZ4'")
    benchmark.expect_exact("Max runs per item is 100")
    benchmark.expect_exact("Max duration per item is 10 seconds")
    benchmark.expect_exact("Warmup duration per item is 5 seconds")
    benchmark.expect_exact(
        "Automatically verifying results with SQLite. This will make the performance numbers invalid."
    )
    benchmark.expect_exact("Benchmarking queries from resources/test_data/queries/file_based/")
    benchmark.expect_exact("Running on tables from resources/test_data/tbl/file_based/")
    benchmark.expect_exact("Running subset of queries: select_statement")
    benchmark.expect_exact("Multi-threaded Topology:")

    close_benchmark(benchmark)
    check_exit_status(benchmark)

    if return_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
