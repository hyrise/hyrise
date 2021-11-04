#!/usr/bin/python3

import os
import shutil
from subprocess import Popen


def main():
    # exec_path = "../scripts/visualize_commit.sh"
    exec_path = "../scripts/benchmark_commit.sh"

    commits = {
        "all_off": "55d303015834dfc8413f1a48bf57af1d8fe78657",
        "only_dgr": "6eef22f15efa07b1a5032d6dc4b1a9ea6162ee58",
        "only_jts": "c9855d0af46b0900607cd875a20023c7cd85fce3",
        "only_join2pred": "9537dbc9c90bdd5458f30d34df564b1805b6b7ec",
        "only_join_elim": "f9ffe63c58764f45a42a976b7fb4c0c9efd6ef6f",
        "all_on": "ccbeffa5d344df3c4fc4849cf6d3a8db4a7dbcd7",
        "dgr_jts_join2pred": "f52fa0ab4f77a15c3b041bff5073b6dbb42501b5",
    }

    cached_table_dirs = ["imdb_data", "tpch_cached_tables", "tpcds_cached_tables"]
    cached_dir_prefix = ".."
    for cached_table_dir in cached_table_dirs:
        cached_table_path = os.path.join(cached_dir_prefix, cached_table_dir)
        if not os.path.isdir(cached_table_path):
            continue
        if cached_table_dir != "imdb_data":
            shutil.rmtree(cached_table_path)
        else:
            files_to_delete = [file_name for file_name in os.listdir(cached_table_path) if file_name.endswith(".bin")]
            for file_name in files_to_delete:
                os.remove(os.path.join(cached_table_path, file_name))

    for config, commit in commits.items():
        print(f"\n{config}\t{commit}")
        command = f"numactl -N 1 -m 1 {exec_path} {commit} {config}"
        with Popen(command, shell=True) as p:
            p.wait()


if __name__ == "__main__":
    main()
