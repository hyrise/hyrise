#!/usr/bin/env python3

import argparse
import json
import matplotlib
import multiprocessing
import os
import socket
import sys

from datetime import datetime
from pathlib import Path


MAX_CORE_COUNT = multiprocessing.cpu_count()

# We provide a numabinding and cores (to limit to physical cores instead of all node cores).
# for each config (three vars):
#   - run benchmark \in {TPCH,DS,JOB,SSB}
#     - shuffled and non-shuffled

def get_parser():
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action="store_true", help="Print log messages.")
  parser.add_argument("-m", "--master", action="store_true", help="Assume master branch is checked out.")
  parser.add_argument(
    "-c",
    "--cores",
    action="store",
    type=int,
    help="Cores to be used for the benchmarks.",
  )
  parser.add_argument(
    "-b",
    "--build_dir",
    action="store",
    type=str,
    default="rel",
    help="Directly with CMake release build."
  )

  return parser


if __name__ == "__main__":
  parser = get_parser()
  args, _ = parser.parse_known_args()
  build_dir = args.build_dir

  hostname = socket.gethostname()

  os.makedirs(hostname, exist_ok=True)

  if args.master:
    os.system("git checkout master")
  else:
    os.system("git checkout martin/perf/num_groups")

  os.system("git submodule update --recursive --init")
  os.system(f"ninja -C {build_dir}")

  runtime = 1200 if not args.verbose else 5
  runs = 50 if not args.verbose else 1
  core_count = args.cores if args.cores else MAX_CORE_COUNT

  num_groups_min_factors = [0.1, 0.25, 0.5] if not args.master else [17]
  num_groups_max_factors = [1.0, 1.5, 2.0] if not args.master else [17]
  upper_limit_queue_size_factor = [2, 4, 8] if not args.master else [17]

  for NUM_GROUPS_MIN_FACTOR in num_groups_min_factors:
    for NUM_GROUPS_MAX_FACTOR in num_groups_max_factors:
      for UPPER_LIMIT_QUEUE_SIZE_FACTOR in upper_limit_queue_size_factor:
        for benchmark in ["TPCH", "TPCDS", "JoinOrder", "StarSchema"]:
          for mode in ["Shuffled", "Ordered"]:
            client_count = 1 if mode == "Ordered" else core_count
            run_limit = f"--runs={runs}" if mode == "Ordered" else f"--time={runtime}"
            output_filename = f'./{hostname}/{"master" if args.master else "branch"}__{benchmark}__min_{NUM_GROUPS_MIN_FACTOR}__max_{NUM_GROUPS_MAX_FACTOR}__limit_{UPPER_LIMIT_QUEUE_SIZE_FACTOR}__clients_{client_count}__cores_{core_count}.json'
            scale = "" if benchmark == "JoinOrder" else ("--scale=1" if args.verbose else "--scale=10")

            if Path(output_filename).exists():
              print(f"Skipping as result JSON already exists ({output_filename}).")
              continue

            try:
              os.system(f"NUM_GROUPS_MIN_FACTOR={NUM_GROUPS_MIN_FACTOR} NUM_GROUPS_MAX_FACTOR={NUM_GROUPS_MAX_FACTOR} UPPER_LIMIT_QUEUE_SIZE_FACTOR={UPPER_LIMIT_QUEUE_SIZE_FACTOR} ./{build_dir}/hyriseBenchmark{benchmark} --mode={mode} --clients={client_count} --cores={core_count} --scheduler -o {output_filename} {run_limit} --warmup=1 {scale}")
            except KeyboardInterrupt:
              print("An error occurred.")
              sys.exit()
