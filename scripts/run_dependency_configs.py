#!/usr/bin/python3

import argparse
import os
import shutil
import json
from subprocess import Popen


def parse_args():
    ap = argparse.ArgumentParser(description="Creates plots from a benchmark output file")

    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--force-delete",
        "-d",
        action="store_true",
        help="Delete cached tables",
    )
    return ap.parse_args()


def main(output_path, force_delete):
    pwd = os.getcwd()
    if not pwd.endswith("hyrise"):
        print("Did you call the script from project root?")
        return
    build_dir = os.path.join(pwd, "cmake-build-release")
    config_file = "dependency_config.json"
    dep_mining_plugin_path = os.path.join(build_dir, "lib", "libhyriseDependencyMiningPlugin.so")
    config_path = os.path.join(build_dir, config_file)
    benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkJoinOrder"]
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    configs = {
        "all_off": [False for _ in range(4)],
        "only_dgr": [True, False, False, False],
        "only_jts": [False, True, False, False],
        "only_join2pred": [False, False, True, False],
        "only_join_elim": [False, False, False, True],
        "all_on": [True for _ in range(4)],
        "dgr_jts_join2pred": [True, True, True, False],
    }

    if (force_delete):
        print("Clear cached tables")
        cached_table_dirs = ["imdb_data", "tpch_cached_tables", "tpcds_cached_tables"]
        cached_dir_prefix = ".."
        for cached_table_dir in cached_table_dirs:
            cached_table_path = os.path.join(cached_dir_prefix, cached_table_dir)
            if not os.path.isdir(cached_table_path):
                continue
            shutil.rmtree(cached_table_path)

    print("Build executables")
    os.chdir(build_dir)
    all_benchmark_string = " ".join(benchmarks)
    build_command = f"ninja {all_benchmark_string} hyriseDependencyMiningPlugin"
    with Popen(build_command, shell=True) as p:
        p.wait()
    os.chdir("..")

    for config_name, config in configs.items():
        print(f"\n{'=' * 20}\n{config_name.upper()}\n{'=' * 20}")
        assert len(config) == 4
        config_contents = dict()
        config_contents["groupby_reduction"] = config[0]
        config_contents["join_to_semi"] = config[1]
        config_contents["join_to_predicate"] = config[2]
        config_contents["join_elimination"] = config[3]
        config_contents["preset_constraints"] = False

        with open(config_path, "w") as f:
            json.dump(config_contents, f, indent=4)

        for benchmark in benchmarks:
            print(f"\nRunning {benchmark} for {config_name}..")
            benchmark_path = os.path.join(build_dir, benchmark)
            results_path = os.path.join(output_path, f"{benchmark}_{config_name}.json")
            log_path = os.path.join(output_path, f"{benchmark}_{config_name}.log")

            exec_command = f"({benchmark_path} -r 100 -w 1 -o {results_path} --dep_mining_plugin {dep_mining_plugin_path} --dep_config {config_path} 2>&1 ) | tee {log_path}"

            with Popen(exec_command, shell=True) as p:
                p.wait()

        os.remove(config_path)


if __name__ == "__main__":
    args = parse_args()
    main(args.output_path, args.force_delete)
