#!/usr/bin/env python3

from hyriseBenchmarkCore import initialize, run_benchmark


def main():
    build_dir = initialize()

    arguments = {}
    arguments["--benchmark_data"] = "tpc-h:0.01"

    benchmark = run_benchmark(build_dir, arguments, "hyriseServer", True)

    benchmark.expect_exact("Loading/Generating tables", timeout=2)
    benchmark.expect_exact("Encoding 'lineitem'", timeout=10)
    benchmark.expect_exact("Server started at 0.0.0.0 and port 5432", timeout=10)

    # Not using close_benchmark() here, as a server is started and a timeout of None would wait forever
    benchmark.close()


if __name__ == "__main__":
    main()
