#!/usr/bin/python3

import os
import shutil
from subprocess import Popen


def main():
    # exec_path = "../scripts/visualize_commit.sh"
    exec_path = "../scripts/benchmark_commit.sh"

    commits = {
        "all_off": "b404a332600828bb47344470ab6b3cce3550ff6c",
        "only_dgr": "95d90f88419b7a4a6a71c9e7bce722359f48f80c",
        "only_jts": "cbfe249ce9eccdc4b4bc67559fb0d2e03f45c012",
        "only_join2pred": "f89e3d04701835fa172a75c0af7e435579d5a7eb",
        "only_join_elim": "2dc15124f1f605663ef6a9b0ffd4a461098a73d6",
        "all_on": "d9e8c3f4f47be0c5a501f2e4125025a9bfb993a1",
        "dgr_jts_join2pred": "9b5f88481bec252bbceb06b4680a0c7f2f94a755",
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
