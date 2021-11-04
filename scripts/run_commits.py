#!/usr/bin/python3

import os
import shutil
from subprocess import Popen


def main():
    # exec_path = "../scripts/visualize_commit.sh"
    exec_path = "../scripts/benchmark_commit.sh"

    commits = {
        "all_off": "f4d9326910e5b35a8002cac29f6a587841a6feb5",
        "only_dgr": "6f74487cb8f9249c785706f959252bd765a78e6c",
        "only_jts": "8c4056246a10f4731e2b66d5c194604d3ea6c062",
        "only_join2pred": "d1ae4ded3457bc01b5b8ca7271be1dbf1e82d61e",
        "only_join_elim": "95b137541cd01090c195ea2d0309c8f25bc00b38",
        "all_on": "79cf1841ac54bfa2da036a46c14f1116175da487",
        "dgr_jts_join2pred": "ccb5064ce1cb2c54249246d9cdd07ec5259315b4",
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
