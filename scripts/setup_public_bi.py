#!/usr/bin/python3

# This script is meant to be called by hyriseBenchmarkJoinOrder, but nothing stops you from calling it yourself.
# It downloads the IMDB used by the JoinOrderBenchmark and unzips it. We do this in Python and not in C++ because
# downloading and unzipping is straight forward in Python.

import argparse
import hashlib
import os
import sys
import urllib.request
import re
import bz2


def clean_up(table_dir, including_tables=False):
    if os.path.exists(table_dir):
        delete_extension = ".csv" if including_tables else ".bz2"
        files_to_delete = [x for x in os.listdir(table_dir) if x.endswith(delete_extension)]
        for file_name in files_to_delete:
            os.remove(os.path.join(table_dir, file_name))


def tables_are_setup(table_dir, tables_per_benchmark):
    for table_name in tables_per_benchmark.values():
        if not os.path.exists(os.path.join(table_dir, f"{table_name}.csv")):
            return False
    return True


def queries_are_setup(query_dir, queries_per_benchmark):
    for benchmark, queries in queries_per_benchmark.items():
        for query in queries:
            if not os.path.exists(os.path.join(query_dir, f"{benchmark}.{query}")):
                return False
    return True


def parse_args():
    ap = argparse.ArgumentParser(description="Setup data and queries for the Public BI Benchmark")
    ap.add_argument("benchmark_source", type=str, help="Path to the Public BI Benchmark repository")
    ap.add_argument("data_dir", type=str, help="Directory where tables and queries will be located")
    return ap.parse_args()


def main(data_dir, benchmark_source):

    tables_per_benchmark = dict()
    queries_per_benchmark = dict()

    benchmark_dir = os.path.join(benchmark_source, "benchmark")
    benchmarks = sorted(os.listdir(benchmark_dir))

    for benchmark in benchmarks:
        current_benchmark_dir = os.path.join(benchmark_dir, benchmark)
        tables_per_benchmark[benchmark] = [
            table_name[: -len(".table.sql")] for table_name in os.listdir(os.path.join(current_benchmark_dir, "tables"))
        ]
        queries_per_benchmark[benchmark] = os.listdir(os.path.join(current_benchmark_dir, "queries"))

    print("- Retrieving the Public BI dataset.")
    num_files = sum([len(tables) for tables in tables_per_benchmark.values()])
    table_dir = os.path.join(data_dir, "tables")
    query_dir = os.path.join(data_dir, "queries")
    abort = (False, 0)

    if tables_are_setup(table_dir, tables_per_benchmark) or True:
        print("- Public BI tables already complete, no setup action required")
    else:
        print(f"- Downloading {num_files} tables")

        url_file_name = "data-urls.txt"
        if not os.path.isdir(table_dir):
            os.makedirs(table_dir)
        for benchmark in benchmarks:
            if abort[0]:
                break
            table_name_regex = re.compile(f"(?<={benchmark}/).+(?=\\.csv.bz2)")
            with open(os.path.join(benchmark_dir, benchmark, url_file_name)) as url_file:
                for line in url_file:
                    table_url = line.strip()
                    url = urllib.request.urlopen(table_url)

                    meta = url.info()
                    table_name_match = table_name_regex.search(table_url)
                    assert table_name_match is not None
                    table_name = table_name_match.group(0)

                    if "Content-Length" in meta:
                        file_size = int(meta["Content-Length"])
                    else:
                        print(f"- Aborting. Could not retrieve the file size for {benchmark}/{table_name}")
                        abort = (True, 1)
                        break

                    table_path = os.path.join(table_dir, f"{table_name}.csv.bz2")
                    if os.path.exists(table_path) and os.path.getsize(table_path) == file_size:
                        print(f" - Skipping: {table_name} (already downloaded)")
                    else:
                        with open(table_path, "wb") as target_file:
                            print(" - Downloading: %s (%.2f MB)" % (table_name, file_size / 1000**2))

                            already_retrieved = 0
                            block_size = 8192
                            try:
                                while True:
                                    buffer = url.read(block_size)
                                    if not buffer:
                                        break

                                    already_retrieved += len(buffer)
                                    target_file.write(buffer)
                                    status = r" - Retrieved %3.2f%% of the data" % (
                                        already_retrieved * 100.0 / file_size
                                    )
                                    status = status + chr(8) * (len(status) + 1)
                                    print(status, end="\r")
                            except Exception:
                                print("- Aborting. Something went wrong during the download. Cleaning up.")
                                abort = (True, 2)
                                break
                        print()
                    print(" - Decompressing the file...")
                    try:
                        with bz2.open(table_path) as compressed_file:
                            with open(table_path[: -len(".bz2")], "w") as decompressed_file:
                                decompressed_file.write(compressed_file.read().decode())
                    except Exception:
                        print("- Aborting. Something went wrong during decompression. Cleaning up.")
                        abort = (True, 3)

    print("- Preparing the Public BI queries.")
    if queries_are_setup(query_dir, queries_per_benchmark):
        print("- Public BI queries already complete, no setup action required")
    else:
        if not os.path.isdir(query_dir):
            os.makedirs(query_dir)
        for benchmark, queries in queries_per_benchmark.items():
            for query in queries:
                source_path = os.path.join(benchmark_dir, benchmark, "queries", query)
                target_path = os.path.join(query_dir, f"{benchmark}.{query}")
                with open(source_path) as source_file:
                    with open(target_path, "w") as target_file:
                        target_file.write(source_file.read())

    if not abort[0]:
        clean_up(table_dir)
    else:
        clean_up(table_dir, including_tables=True)
        sys.exit(abort[1])

    print(f"- {os.path.basename(__file__)} ran sucessfully.")


if __name__ == "__main__":
    args = parse_args()
    main(args.data_dir, args.benchmark_source)
