#!/usr/bin/env python3

import argparse
import csv
import json
import pandas as pd
import subprocess
import sys

from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--hyrise_path", required=True, type=str)
parser.add_argument("--scale_factor", type=float)
parser.add_argument('--debug', action='store_true')
parser.add_argument('--process_result_files', action='store_true')
parser.add_argument('--all_queries', action='store_true')

args = parser.parse_args()

main_df = pd.DataFrame()

def write_csv_file(input_file, main_df, scheduler_mode_name, scale_factor, benchmark_name, radix_cluster_factor):
  rows = []
  with open(input_file) as file:
    result_json = json.load(file)
    for query in result_json["benchmarks"]:
      for run in query["successful_runs"]:
        rows.append({"BENCHMARK": benchmark_name, "SCALE_FACTOR": scale_factor, "SCHEDULER_MODE": scheduler_mode_name,
        						 "NAME": query["name"], "RADIX_CLUSTER_FACTOR": radix_cluster_factor, "RUNTIME_NS": run["duration"]})

  df = pd.DataFrame(rows)
  df.to_csv(Path(input_file).stem + ".csv", index=None, quoting=csv.QUOTE_NONNUMERIC)
  merged_df = pd.concat([main_df, df], ignore_index=True)
  return merged_df


assert Path(args.hyrise_path).exists()

subprocess.run(["ninja", "-C", args.hyrise_path, "hyriseBenchmarkTPCH"])


scale_factors = [args.scale_factor] if args.scale_factor else [10.0, 50.0]
scheduler_modes = [("mt", "--scheduler"), ("st", "")]
scheduler_modes = [("mt", "--scheduler")]
queries = "7,8,17,20"
max_query_runtime = 7200

if args.debug:
  scheduler_modes = [("mt", "--scheduler")]
  max_query_runtime = 300
  if not args.scale_factor:
    scale_factors = [args.scale_factor] if args.scale_factor else [0.1, 1.0]

if args.all_queries:
	queries = "1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22"

for scale_factor in scale_factors:
  for benchmark_name, benchmark_shortname, benchmark_command in [("TPC-H", "tpch", ""),
                                                                 ("JCC-H (normal)", "jcch_normal", "--jcch=normal"),
                                                                 ("JCC-H (skewed)", "jcch_skewed", "--jcch=skewed")]:
    for scheduler_mode_name, scheduler_mode_command in scheduler_modes:
      for radix_cluster_factor in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 7.0, 8.0]:
	      print(f"####\n#### SF {scale_factor} - {benchmark_name} - Radix cluster factor: {radix_cluster_factor}\n####", file=sys.stderr)
	      result_filename = f"{benchmark_shortname}__sf{int(scale_factor)}_{scheduler_mode_name}__radix_{str(radix_cluster_factor).replace('.', '_')}.json"

	      if args.process_result_files:
	        if not Path(result_filename).exists():
	          print(f"File '{result_filename}' does not exist.", file=sys.stderr)
	          continue

	        main_df = write_csv_file(result_filename, main_df, scheduler_mode_name, scale_factor, benchmark_name, radix_cluster_factor)
	        continue

	      command = [f"./{args.hyrise_path}/hyriseBenchmarkTPCH", "-s", f"{scale_factor}", scheduler_mode_command,
	                 "-q", queries,
	                 "-o", result_filename,
	                 benchmark_command,
	                 "-t", str(max_query_runtime),
	                 "-r", "51",
	                 "-w", "1",
	                 "--cores", "64"]
	      print(f"Running command (radix configuration: {radix_cluster_factor}:\n\t", " ".join(command))
	      subprocess.run(command, env={"RADIX_CLUSTER_FACTOR": str(radix_cluster_factor)})


main_df.to_csv("data_loading_main.csv", index=None, quoting=csv.QUOTE_NONNUMERIC)

subprocess.run(["Rscript", "data_integration__showcase_plot.R"])
