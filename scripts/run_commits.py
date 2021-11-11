#!/usr/bin/python3

import os
import shutil
from subprocess import Popen


def main():
    # exec_path = "../scripts/visualize_commit.sh"
    exec_path = "../scripts/benchmark_commit.sh"

    commits = {
        "all_off": "ef2446f53c6434e0325dacb4ff24ef772960f492",
        "only_dgr": "18aab54e15309378aa6fd37f066d76f11fbaede0",
        "only_jts": "22fcb70812a2bfc5f5f8c8c766c1424d275e3262",
        "only_join2pred": "07740a25f529ab3b6ab42474650df32d4e3b9c64",
        "only_join_elim": "7ec3a4ce0de47ce262627a8a0852ee6adce3458b",
        "all_on": "8f316723ea1bedeff0d5b9579253bc3e9c8a33a2",
        "dgr_jts_join2pred": "10d0fa21634e630dda8c3d18838946d52f6daefa",
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
