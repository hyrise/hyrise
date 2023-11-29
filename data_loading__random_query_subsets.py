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

JCCH_SET_VARIANTS = 1
ADDITIONAL_QUERY_SET_VARIANTS = 70
WARMUP_RUNS = 1
QUERY_RUNS = [1, 10, 100]
MEASUREMENT_RUNS = 5

# JCCH_SET_VARIANTS = 2
# ADDITIONAL_QUERY_SET_VARIANTS = 3
# WARMUP_RUNS = 1
# QUERY_RUNS = [2, 6]
# MEASUREMENT_RUNS = 5

PLUGIN_FILETYPE = '.so' if sys.platform == "linux" else '.dylib'

FILENAME_TIMESTAMP = int(time.time())

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
parser.add_argument("--scale_factor", type=float)
parser.add_argument('--debug', action='store_true')
parser.add_argument('--process_result_files', action='store_true')
parser.add_argument('--all_queries', action='store_true')
parser.add_argument('--skip_server_start', action='store_true')

args = parser.parse_args()

assert Path(args.hyrise_path).exists()

subprocess.run(["ninja", "-C", args.hyrise_path, "hyriseServer", "hyriseDataLoadingPlugin"])

scale_factors = [args.scale_factor] if args.scale_factor else [10.0, 50.0]
jcch_set = ("MAIN", "ORIGINAL", (7, 8, 17, 20))
query_sets = set()
query_sets.add(jcch_set)
# query_sets.add(("TPC-H", "ORIGINAL", tuple(range(1, 23))))  # too many queries to plot ... and the results are not nice (yet)

# add variants of JCC-H set
while len(query_sets) < JCCH_SET_VARIANTS:
  queries_copy = list(jcch_set[2])
  random.shuffle(queries_copy)  # I don't like the sample() work-around for in-place shuffle.

  if tuple(queries_copy) in [(7, 8, 17, 20), tuple(range(1, 23))]:
    continue  # not nice
  query_sets.add(("MAIN", "PERMUTATION", tuple(queries_copy)))

while (len(query_sets) - JCCH_SET_VARIANTS) < ADDITIONAL_QUERY_SET_VARIANTS:
  query_set = set()
  while len(query_set) < 4:
    query_set.add(random.randint(1, 22))
  query_sets.add(("RANDOM", "RANDOM", tuple(query_set)))

query_sets_sorted = sorted(query_sets, key=lambda x: x[0] + x[1] + "".join([str(y) for y in x[2]]))
# query_sets_sorted = sorted(query_sets, key=lambda x: x[0] + x[1] + "".join([str(y) for y in x[2]]), reverse=True)

if args.debug:
  if not args.scale_factor:
    scale_factors = [0.1, 1.0]

df = pd.DataFrame()
measurements = []
measurement_id = 0
for scale_factor in scale_factors:
  for query_runs in QUERY_RUNS:
    query_set_id = 0
    for query_set_name, query_set_kind, query_set in query_sets_sorted:
      print(f"####\n#### SF {scale_factor} - Query set: {query_set} - Query runs: {query_runs} ####\n####")
      for server_config_name, data_command, load_plugin in [
                                                            ("DATA_LOADING", "", True),
                                                            ("DEFAULT", f"--benchmark_data=TPC-H:{scale_factor}", False)
                                                            ]:

        run_id = 0
        while run_id < MEASUREMENT_RUNS:
          global hyrise_server_process
          hyrise_server_process = None
          try:
            server_start_time = time.time()
            if not args.skip_server_start:
              command = [f"./{args.hyrise_path}/hyriseServer", data_command]
              print(f"#### Starting server: ", " ".join(command))
              hyrise_server_process = subprocess.Popen(command,
                                                       env={"RADIX_CLUSTER_FACTOR": str(1.0),
                                                            "SCALE_FACTOR": str(scale_factor)},
                                                       stdout=subprocess.PIPE,
                                                       bufsize=0, universal_newlines=True)

              print("Waiting for Hyrise to start: ", end="")
              
              for line in iter(hyrise_server_process.stdout.readline, ""):
                if line:
                  if 'Server started at' in line:
                    print("done.")
                    break
                if time.time() - min(1200, 300 * scale_factor) > server_start_time:
                  print("Error: time out during server start")
                  sys.exit()
            server_start_duration = time.time() - server_start_time

            connection = psycopg2.connect("host=localhost sslmode='disable' gssencmode='disable'")
            connection.autocommit = True
            cursor = connection.cursor()

            if load_plugin:
              cursor.execute(f"INSERT INTO meta_plugins(name) VALUES('./{args.hyrise_path}/lib/libhyriseDataLoadingPlugin{PLUGIN_FILETYPE}');")

            measurements.append({"MEASUREMENT_ID": measurement_id, "QUERY_SET_ID": query_set_id, 
                                   "QUERY_SET": ",".join([str(q) for q in query_set]), "QUERY_SET_KIND": query_set_kind,
                                   "QUERY_ID": 0, "RUN_ID": run_id, "SCALE_FACTOR": scale_factor,
                                   "SERVER_CONFIG": server_config_name, "WARMUP_RUNS": WARMUP_RUNS, "QUERY_EXECUTIONS": query_runs,
                                   "QUERY_RUNTIME_S": 0, "TIME_PASSED_S": 0})
            for query_id, query in enumerate(query_set):
              print(f"Executing query #{query}: ", end="", flush=True)
              
              query_sql = static_tpch_queries.queries[query - 1]
              for warmup_run in range(WARMUP_RUNS):
                print(" warm", end="", flush=True)
                cursor.execute(query_sql)
                print("up ", end="", flush=True)

              query_start = time.time()
              for query_run in range(query_runs):
                cursor.execute(query_sql)
                print(".", end="", flush=True)

              print("")
              query_duration = time.time() - query_start
              time_passed = time.time() - server_start_time
              print(f"Query #{query}: {query_duration}s - Time passed: {time_passed}", flush=True)
              measurements.append({"MEASUREMENT_ID": measurement_id, "QUERY_SET_ID": query_set_id, 
                                   "QUERY_SET": ",".join([str(q) for q in query_set]), "QUERY_SET_KIND": query_set_kind,
                                   "QUERY_ID": query_id+1, "RUN_ID": run_id, "SCALE_FACTOR": scale_factor,
                                   "SERVER_CONFIG": server_config_name, "WARMUP_RUNS": WARMUP_RUNS, "QUERY_EXECUTIONS": query_runs,
                                   "QUERY_RUNTIME_S": query_duration, "TIME_PASSED_S": time_passed})

            print(f"Server start time: {server_start_duration}s - Time passed: {time.time() - server_start_time}", flush=True)
            df = pd.concat([df, pd.DataFrame(measurements)], ignore_index=True)
            df.to_csv(f"data_loading__random_query_subsets_{FILENAME_TIMESTAMP}.csv", index=None)

            hyrise_server_process.kill()
            while hyrise_server_process.poll() is None:
              time.sleep(1)
          except Exception as e:
            print("ERROR: ", e)

          run_id += 1

      query_set_id += 1
    measurement_id += 1
