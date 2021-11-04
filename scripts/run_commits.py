#!/usr/bin/python3

import os
import shutil
from subprocess import Popen


def main():
    # exec_path = "../scripts/visualize_commit.sh"
    exec_path = "../scripts/benchmark_commit.sh"

    commits = {
        "all_off": "7285db4980bf9aff7b61e9b7a325b37314787f2d",
        "only_dgr": "afff5a7d2dd89618c4556ed09cf1a72244dc6483",
        "only_jts": "4911dc1a2b4356961152965cbf630bf821ca3a07",
        "only_join2pred": "f9447c3074f36ce6c7caa0a5f2aac10da4178fc9",
        "only_join_elim": "e10ff4bbf9407414859da21678103c4907ff520b",
        "all_on": "5cf066ed918015492f78dde5129c1a9e6fe48c8e",
        "dgr_jts_join2pred": "99a294a95e1d6227c72fa87181fc21e3dee353b4",
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
