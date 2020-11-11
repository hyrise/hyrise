#!/usr/bin/env python3

import os
import sys
import pexpect


def close_benchmark(benchmark):
    benchmark.expect(pexpect.EOF, timeout=None)
    benchmark.close()


def check_exit_status(benchmark):
    if benchmark.exitstatus != 0 or benchmark.signalstatus is not None:
        print(
            "Benchmark failed with exit status "
            + str(benchmark.exitstatus)
            + " and signal status "
            + str(benchmark.signalstatus)
        )
        sys.exit(1)


def check_json(json, argument, error, return_error, difference=None):
    if type(json) is not float:
        if json != argument:
            print("ERROR: " + error + " " + str(json) + " " + str(argument))
            return True
    else:
        if abs(json - argument) > difference:
            print(f"ERROR: {error} {json} {argument} {difference}")
            return True
    return return_error


def initialize():
    if len(sys.argv) == 1:
        print(f"Usage: ./scripts/test/{os.path.basename(sys.argv[0])} <build_dir>")
        sys.exit(1)

    build_dir = sys.argv[1]
    return build_dir


def run_benchmark(build_dir, arguments, benchmark_name, verbose):
    if "--table_path" in arguments and not os.path.isdir(arguments["--table_path"].replace("'", "")):
        print(
            "Cannot find "
            + arguments["--table_path"]
            + ". Are you running the test suite from the main folder of the Hyrise repository?"
        )
        sys.exit(1)

    if "--query_path" in arguments and not os.path.isdir(arguments["--query_path"].replace("'", "")):
        print(
            "Cannot find "
            + arguments["--query_path"]
            + ". Are you running the test suite from the main folder of the Hyrise repository?"
        )
        sys.exit(1)

    concat_arguments = " ".join(["=".join(map(str, x)) for x in arguments.items()])

    benchmark = pexpect.spawn(
        build_dir + "/" + benchmark_name + " " + concat_arguments, maxread=1000000, timeout=1000, dimensions=(200, 64)
    )
    if verbose:
        benchmark.logfile = sys.stdout.buffer
    return benchmark
