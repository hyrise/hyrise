#!/usr/bin/env python3

import argparse
import atexit
import csv
import json
import pandas as pd
import psycopg2
import random
import subprocess
import sys
import static_tpch_queries
import time

from pathlib import Path

WARMUP_RUNS = 1
MEASUREMENT_RUNS = 5

PLUGIN_FILETYPE = '.so' if sys.platform == "linux" else '.dylib'

random.seed(17)

def cleanup():
  global hyrise_server_process
  if hyrise_server_process:
    print("Shutting down...")
    hyrise_server_process.kill()
    while hyrise_server_process.poll() is None:
      time.sleep(1)
atexit.register(cleanup)

parser = argparse.ArgumentParser()
parser.add_argument("--hyrise_path", required=True, type=str)
parser.add_argument("--scale_factor", required=True, type=float)
parser.add_argument("--query_run_count", required=True, type=int)
parser.add_argument("--query_set", required=True, choices=["JCCH_EVAL_SET", "ALL_TPCH", "JCCH_VARIANTS", "RANDOM_VARIANTS"], type=str)
parser.add_argument("--query_set_size", type=int)

parser.add_argument('--debug', action='store_true')
parser.add_argument('--skip_server_start', action='store_true')

args = parser.parse_args()

assert Path(args.hyrise_path).exists()

# -- Skipping for now due to some clang17 issues and parallel work on other branches.
subprocess.run(["ninja", "-C", args.hyrise_path, "hyriseServer", "hyriseDataLoadingPlugin"])

eval_query_set = [7, 8, 17, 20]
query_sets = set()

if args.query_set == "JCCH_EVAL_SET":
  query_sets.add(("MAIN", "ORIGINAL", tuple(eval_query_set)))

elif args.query_set == "ALL_TPCH":
  # too many queries to plot ... and the results are not nice (yet)
  query_sets.add(("TPC-H", "ORIGINAL", tuple(range(1, 23))))

elif args.query_set == "JCCH_VARIANTS":
  assert args.query_set_size is not None, "--query_set_size needs to be set for query set selection."

  # with four queries, we will not have more than 24 sets (minus 1 for the original one)
  while len(query_sets) < min(args.query_set_size, 23):
    queries_copy = eval_query_set.copy()
    random.shuffle(queries_copy)  # I don't like the sample() work-around for non-in-place shuffle.

    if queries_copy == eval_query_set:
      continue  # only permutations distinct from "original".

    query_sets.add(("MAIN", "PERMUTATION", tuple(queries_copy)))

elif args.query_set == "RANDOM_VARIANTS":
  assert args.query_set_size is not None, "--query_set_size needs to be set for query set selection."

  # cheap and expensive queries
  # expensive ones are [1, 9, 10, 13, 18], cheap ones are [2, 6, 17, 22]
  query_sets.add(("RANDOM", "SLOW", (9, 18, 13, 1)))
  query_sets.add(("RANDOM", "FAST", (6, 2, 22, 17)))
  while len(query_sets) < (args.query_set_size / 2):
    query_set = set()
    while len(query_set) < 4:
      query_set.add(random.choice([1, 9, 10, 13, 18, 2, 6, 17, 22]))
    query_sets.add(("RANDOM", "FAST_SLOW", tuple(query_set)))

  # real random ones
  while len(query_sets) < args.query_set_size:
    query_set = set()
    while len(query_set) < 4:
      query_set.add(random.randint(1, 22))
    query_sets.add(("RANDOM", "RANDOM", tuple(query_set)))

# query_sets = set()
# query_sets.add(("X", "X", tuple([1, 17, 13])))

# No particular reason to sort, it's just easier to debug and with server restarts, we should not run into
# caching issues.
# query_sets_sorted = sorted(query_sets, key=lambda x: x[0] + x[1] + "".join([str(y) for y in x[2]]))

# Taking intermediate results sucks when queries are sorted. Shuffling them now.
query_sets_sorted = list(query_sets)
random.shuffle(query_sets_sorted)

df = pd.DataFrame()
measurements = []
measurement_id = 0
plugin_is_loaded = False

query_set_id = 0
for query_set_name, query_set_kind, query_set in query_sets_sorted:
  print(f"####\n#### SF {args.scale_factor} - Query set: {query_set} - Query runs: {args.query_run_count} ####\n####")
  for server_config_name, data_command, load_plugin in [
                                                        ("DATA_LOADING", "", True),
                                                        ("DEFAULT", f"--benchmark_data=TPC-H:{args.scale_factor}", False)
                                                        ]:

    attempt_id = 0
    run_id = 0
    while run_id < MEASUREMENT_RUNS:
      attempt_id += 1
      if attempt_id > 15:
        print(f"Too many unsuccessful attempts to run {query_set=}.")
        break

      global hyrise_server_process
      hyrise_server_process = None
      try:
        server_start_time = time.time()
        if not args.skip_server_start:
          command = [f"./{args.hyrise_path}/hyriseServer", data_command]
          print(f"#### Starting server (attempt {attempt_id}: ", " ".join(command))
          hyrise_server_process = subprocess.Popen(command,
                                                   env={"RADIX_CLUSTER_FACTOR": str(1.0),
                                                        "SCALE_FACTOR": str(args.scale_factor)},
                                                   stdout=subprocess.PIPE,
                                                   bufsize=0, universal_newlines=True)

          print("Waiting for Hyrise to start: ", end="")
          
          for line in iter(hyrise_server_process.stdout.readline, ""):
            if line:
              if 'Server started at' in line:
                print("done.")
                break
            if time.time() - min(1200, 300 * args.scale_factor) > server_start_time:
              print("Error: time out during server start")
              sys.exit()

        server_start_duration = time.time() - server_start_time

        connection = psycopg2.connect("host=localhost sslmode='disable' gssencmode='disable'")
        connection.autocommit = True
        cursor = connection.cursor()

        if load_plugin and (not args.skip_server_start or not plugin_is_loaded):
          cursor.execute(f"INSERT INTO meta_plugins(name) VALUES('./{args.hyrise_path}/lib/libhyriseDataLoadingPlugin{PLUGIN_FILETYPE}');")
          plugin_is_loaded = True

        wrote_initial_measurement = False
        for query_id, query in enumerate(query_set):
          print(f"Executing query #{query}: ", end="", flush=True)

          query_sql = static_tpch_queries.queries[query - 1]
          for warmup_run in range(WARMUP_RUNS):
            print(" warm", end="", flush=True)
            cursor.execute(query_sql)
            print("up ", end="", flush=True)

          query_start = time.time()
          for query_run in range(args.query_run_count):
            cursor.execute(query_sql)
            print(".", end="", flush=True)

          print("")
          query_duration = time.time() - query_start
          time_passed = time.time() - server_start_time
          print(f"Query #{query}: {query_duration}s - Time passed: {time_passed}", flush=True)

          if not wrote_initial_measurement:
            # Only append when we end up here (i.e., a query succeeded). With failing queries, we end up with many
            # initial lines otherwise.
            measurements.append({"MEASUREMENT_ID": measurement_id, "QUERY_SET_ID": query_set_id, 
                                 "QUERY_SET": ",".join([str(q) for q in query_set]), "QUERY_SET_KIND": query_set_kind,
                                 "QUERY_ID": 0, "RUN_ID": run_id, "SCALE_FACTOR": args.scale_factor,
                                 "SERVER_CONFIG": server_config_name, "WARMUP_RUNS": WARMUP_RUNS, "QUERY_EXECUTIONS": args.query_run_count,
                                 "QUERY_RUNTIME_S": 0, "TIME_PASSED_S": 0})
            wrote_initial_measurement = True

          measurements.append({"MEASUREMENT_ID": measurement_id, "QUERY_SET_ID": query_set_id, 
                               "QUERY_SET": ",".join([str(q) for q in query_set]), "QUERY_SET_KIND": query_set_kind,
                               "QUERY_ID": query_id+1, "RUN_ID": run_id, "SCALE_FACTOR": args.scale_factor,
                               "SERVER_CONFIG": server_config_name, "WARMUP_RUNS": WARMUP_RUNS, "QUERY_EXECUTIONS": args.query_run_count,
                               "QUERY_RUNTIME_S": query_duration, "TIME_PASSED_S": time_passed})

        print(f"Server start time: {server_start_duration}s - Time passed: {time_passed}", flush=True)
        df = pd.concat([df, pd.DataFrame(measurements)], ignore_index=True)
        df.to_csv(f"data_loading__results_sf{int(args.scale_factor)}__{args.query_run_count}runs__set_{args.query_set.replace('_', '').upper()}__{args.query_set_size}perms.csv", index=None)

        run_id += 1
      except Exception as e:
        print("ERROR: ", e)

      if not args.skip_server_start:
        cleanup()

  query_set_id += 1
