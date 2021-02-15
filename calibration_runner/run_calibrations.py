import sys
import os
from subprocess import Popen
import numpy as np
import math
import json
import pandas as pd

from tpch_benchmark import TPCHBenchmark
from tpcds_benchmark import TPCDSBenchmark
from calibration_benchmark import CalibrationBenchmark

AVAILABLE_BENCHMARKS = {
  "tpch": TPCHBenchmark(),
  "tpcds": TPCDSBenchmark(),
  "calibration": CalibrationBenchmark(),
}


def run_benchmark(benchmark, config_name, chunk_size):
  # write clustering config into the config json
  sort_order = benchmark.sort_orders()[config_name]
  with open ("clustering_config.json", "w") as clustering_config_file:
    clustering_config_file.write(json.dumps(sort_order) + "\n")

  # ensure that the results directory exists
  #if not os.path.exists(benchmark.result_path()):
  #  os.makedirs(benchmark.result_path())

  process_env = os.environ.copy()
  process_env["CLUSTERING_ALGORITHM"] = "DisjointClusters"
  #process_env["CLUSTERING_ALGORITHM"] = "Partitioner"

  p = Popen(
            [benchmark.exec_path(), "--benchmark", benchmark.name(), "--dont_cache_binary_tables", "--time", str(benchmark.time()), "--runs", str(benchmark.max_runs()), "--scale", str(benchmark.scale()), "--chunk_size", str(chunk_size)],
            env=process_env,
            stdout=sys.stdout,
            stdin=sys.stdin,
            stderr=sys.stderr
        )
  p.wait()

  #visualization_path = f"visualizations/final/{benchmark.name()}/sf{benchmark.scale()}-3d-corrected/{config_name}_{chunk_size}"
  #if not os.path.exists(visualization_path):
  #  os.makedirs(visualization_path)


  # modify aggregate ordering information
  data_path = f"data/{benchmark.name()}"

  correlations = {
    'lineitem': {
      'l_orderkey': ['l_shipdate', 'l_receiptdate'],
      'l_receiptdate': ['l_shipdate', 'l_orderkey'],
      'l_shipdate': ['l_orderkey', 'l_receiptdate'],
    }
  }

  with open(f"{data_path}/aggregates.csv.json") as aggregate_json:
    column_information = json.load(aggregate_json)
  column_names = list(map(lambda x: x['name'], column_information['columns']))

  aggregates = pd.read_csv(f"{data_path}/aggregates.csv", names=column_names)
  INPUT_COLUMN_SORTED_COLUMN = 'INPUT_COLUMN_SORTED'
  if benchmark.name() != "calibration":
    GROUP_COLUMNS = 'GROUP_COLUMNS'
    GROUP_COLUMN_NAMES = 'GROUP_COLUMN_NAMES'
    aggregates = aggregates[aggregates.apply(lambda x: not (x[GROUP_COLUMNS] == 1 and pd.isnull(x[GROUP_COLUMN_NAMES])), axis=1)]
    aggregates[INPUT_COLUMN_SORTED_COLUMN] = aggregates.apply(actual_aggregate_ordering_information, args=(sort_order, correlations,), axis=1)
  else:
    aggregates[INPUT_COLUMN_SORTED_COLUMN] = 0.5 # we only generate such aggregates at the moment
  aggregates.to_csv(f"{data_path}/aggregates.csv", header=False, index=False)

  #visualization_file_pattern = benchmark.visualization_pattern()
  if benchmark.name() != "calibration":
    import glob
    import shutil
    target_path = f"{data_path}/sf{benchmark.scale()}-runs{benchmark.max_runs()}/{config_name}"

    if not os.path.exists(target_path):
      os.makedirs(target_path)

    for file in glob.glob(data_path + "/*.csv*"):
      shutil.move(file, target_path + '/' + os.path.basename(file))

    shutil.copyfile('clustering_config.json', f'{target_path}/clustering_config.json')


def actual_aggregate_ordering_information(row, sort_order, correlations):
  group_columns = row['GROUP_COLUMNS']
  group_column_names = row['GROUP_COLUMN_NAMES']
  input_column_sorted = row['INPUT_COLUMN_SORTED']
  if int(group_columns) != 1:
    return 0
  assert len(group_column_names) > 0 and ',' not in group_column_names

  if input_column_sorted == 0:
    return 0
  assert input_column_sorted == 1, "INPUT_COLUMN_SORTED is neither 0 nor 1: " + str(input_column_sorted)


  def correlates(group_column, clustering_columns, correlations):
    correlated_columns = correlations.get(group_column, {})
    for column in correlated_columns:
      if column in clustering_columns:
        return True
    return False

  if 'lineitem' in sort_order:
    clustering_columns = list(map(lambda x: x[0], sort_order['lineitem']))
    if group_column_names in clustering_columns:
      return 1
    elif correlates(group_column_names, clustering_columns, correlations['lineitem']):
      return 0.5

  return 0

def build_sort_order_string(sort_order_per_table):
  return json.dumps(sort_order_per_table)

  table_entries = []
  for table, sort_order in sort_order_per_table.items():
    table_entries.append(table + ":" + ",".join(sort_order))
  return ";".join(table_entries)

def benchmarks_to_run():
  benchmark_names = sys.argv[1:]
  if not len(benchmark_names) > 0:
    print("Error: you need to provide at least one benchmark name")
    exit(1)

  for name in benchmark_names:
    if not name.lower() in AVAILABLE_BENCHMARKS:
      print(f"Error: unknown benchmark: {name}")
      exit(1)

  return [AVAILABLE_BENCHMARKS[name] for name in benchmark_names]

def main():
  benchmarks = benchmarks_to_run()
  for benchmark in benchmarks:
    benchmark_name = benchmark.name()
    print(f"Running benchmarks for {benchmark_name.upper()}")
    num_benchmarks = len(benchmark.sort_orders())
    print(f"Found {num_benchmarks} configurations for {len(benchmark.chunk_sizes())} chunk size(s)")
    for chunk_size in benchmark.chunk_sizes():
      
      # using cached tables is deactivated for now
      # different chunk sizes need different caches, hidden behind symlinks      
      #os.system(f"rm -f {benchmark_name}_cached_tables")
      #os.system(f"ln -s {benchmark_name}_cached_tables_cs{chunk_size} {benchmark_name}_cached_tables")      
      for run_id, config_name in enumerate(benchmark.sort_orders()):
        print(f"Running benchmark {run_id + 1} out of {num_benchmarks}")           
        run_benchmark(benchmark, config_name, chunk_size)


if __name__ == "__main__":
  main()