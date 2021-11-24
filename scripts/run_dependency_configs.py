#!/usr/bin/python3

import argparse
import os
import shutil
import json
from subprocess import Popen, PIPE


class DependencyUsageConfig:
    def __init__(self, groupby_reduction, join_to_semi, join_to_predicate, join_elimination, preset_constraints=False):
        self.groupby_reduction = groupby_reduction
        self.join_to_semi = join_to_semi
        self.join_to_predicate = join_to_predicate
        self.join_elimination = join_elimination
        self.preset_constraints = preset_constraints

    def to_json(self, file_path):
        with open(file_path, "w") as f:
            json.dump(vars(self), f, indent=4)


def parse_args():
    ap = argparse.ArgumentParser(description="Runs benchmarks for pre-defined dependency usage configurations")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--force-delete",
        "-d",
        action="store_true",
        help="Delete cached tables",
    )
    ap.add_argument(
        "--build_dir",
        type=str,
        default="cmake-build-release",
        help="Build directory",
    )
    ap.add_argument(
        "--commit",
        "-c",
        type=str,
        default=None,
        help="Dedicated commit",
    )
    return ap.parse_args()


def main(output_path, force_delete, build_dir, commit):
    pwd = os.getcwd()
    if not os.path.isdir(build_dir):
        print(f"Could not find build directory {build_dir}\nDid you call the script from project root?")
        return
    config_file = "dependency_config.json"
    dep_mining_plugin_path = os.path.join(os.path.abspath(build_dir), "lib", "libhyriseDependencyMiningPlugin.so")
    config_path = os.path.join(build_dir, config_file)
    benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkJoinOrder"]
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    configs = {
        "all_off": DependencyUsageConfig(False, False, False, False),
        "only_dgr": DependencyUsageConfig(True, False, False, False),
        "only_jts": DependencyUsageConfig(False, True, False, False),
        "only_join2pred": DependencyUsageConfig(False, False, True, False),
        "only_join_elim": DependencyUsageConfig(False, False, False, True),
        "all_on": DependencyUsageConfig(True, True, True, True),
        "dgr_jts_join2pred": DependencyUsageConfig(True, True, True, False),
    }

    scale_factors = [1, 10, 100]

    if force_delete:
        print("Clear cached tables")
        cached_table_dirs = ["imdb_data", "tpch_cached_tables", "tpcds_cached_tables"]
        cached_dir_prefix = ".."
        for cached_table_dir in cached_table_dirs:
            cached_table_path = os.path.join(cached_dir_prefix, cached_table_dir)
            if not os.path.isdir(cached_table_path):
                continue
            shutil.rmtree(cached_table_path)

    if commit:
        print(f"Checkout {commit}")
        with Popen(f"git rev-parse {commit} | head -n 1", shell=True, stdout=PIPE) as p:
            p.wait()
            commit_ref = p.stdout.read().decode().strip()
        with Popen(f"git checkout {commit_ref}", shell=True) as p:
            p.wait()

    print("Build executables")
    os.chdir(build_dir)
    all_benchmark_string = " ".join(benchmarks)
    build_command = f"ninja {all_benchmark_string} hyriseDependencyMiningPlugin"
    with Popen(build_command, shell=True) as p:
        p.wait()
    os.chdir(pwd)

    for config_name, config in configs.items():
        print(f"\n{'=' * 20}\n{config_name.upper()}\n{'=' * 20}")
        config.to_json(config_path)

        for benchmark in benchmarks:
            benchmark_path = os.path.join(build_dir, benchmark)

            for scale_factor in scale_factors:
                if benchmark != "hyriseBenchmarkTPCH" and scale_factor == 0.01:
                    continue
                if benchmark == "hyriseBenchmarkJoinOrder" and scale_factor != 10:
                    continue

                print(f"\nRunning {benchmark} for {config_name} with SF {scale_factor}..")
                sf_flag = f"-s {scale_factor}" if benchmark != "hyriseBenchmarkJoinOrder" else ""
                sf_printable = str(scale_factor).replace(".", "")
                sf_extension = f"_s-{sf_printable}" if benchmark != "hyriseBenchmarkJoinOrder" else ""
                base_file_name = f"{benchmark}_{config_name}{sf_extension}"
                results_path = os.path.join(output_path, f"{base_file_name}.json")
                log_path = os.path.join(output_path, f"{base_file_name}.log")
                run_time = max(60, scale_factor * 6)
                exec_command = (
                    f"({benchmark_path} -r 100 -w 1 {sf_flag} -t {run_time} -o {results_path} --dep_mining_plugin "
                    + f"{dep_mining_plugin_path} --dep_config {config_path} 2>&1 ) | tee {log_path}"
                )

                with Popen(exec_command, shell=True) as p:
                    p.wait()

        os.remove(config_path)
    print(n_r)

if __name__ == "__main__":
    args = parse_args()
    main(args.output_path, args.force_delete, args.build_dir, args.commit)
