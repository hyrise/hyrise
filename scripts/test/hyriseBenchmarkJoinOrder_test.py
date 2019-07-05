#!/usr/bin/python

from hyriseBenchmarkCore import *

# This test runs the binary hyriseBenchmarkJoinOrder with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():

  return_error = False

  arguments = {}
  arguments["--table_path"] = "'resources/test_data/imdb_sample/'"
  arguments["--queries"] = "'21c,22b,23c,24a'"
  arguments["--time"] = "10"
  arguments["--runs"] = "100"
  arguments["--output"] = "'json_output.txt'"
  arguments["--mode"] = "'Shuffled'"
  arguments["--encoding"] = "'Unencoded'"
  arguments["--clients"] = "1"
  arguments["--cache_binary_tables"] = "true"
  arguments["--scheduler"] = "false"

  os.system("rm -rf " + arguments["--table_path"] + "/*.bin")

  benchmark = initialize(arguments, "hyriseBenchmarkJoinOrder", True)

  benchmark.expect("Writing benchmark results to 'json_output.txt'")
  benchmark.expect("Running in single-threaded mode")
  benchmark.expect("1 simulated clients are scheduling items in parallel")
  benchmark.expect("Running benchmark in 'Shuffled' mode")
  benchmark.expect("Encoding is 'Unencoded'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per item is 100")
  benchmark.expect("Max duration per item is 10 seconds")
  benchmark.expect("No warmup runs are performed")
  benchmark.expect("Caching tables as binary files")
  benchmark.expect("Retrieving the IMDB dataset.")
  benchmark.expect("IMDB setup already complete, no setup action required")
  benchmark.expect("Benchmarking queries from third_party/join-order-benchmark")
  benchmark.expect("Running on tables from resources/test_data/imdb_sample/")
  benchmark.expect("Running subset of queries: 21c,22b,23c,24a")
  benchmark.expect("-> Executed")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if benchmark.before.count('Verification failed'):
    return_error = True

  if not os.path.isfile(arguments["--output"].replace("'", "")):
    print ("ERROR: Cannot find output file " + arguments["--output"])
    return_error = True

  with open(arguments["--output"].replace("'", '')) as f:
    output = json.load(f)

  return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
  for i in xrange(0,4):
    return_error = check_json(output["benchmarks"][i]["name"], arguments["--queries"].replace("'", '').split(',')[i], "Query doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_duration"], int(arguments["--time"]) * 1e9, "Max duration doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_runs"], int(arguments["--runs"]), "Max runs don't match with JSON:", return_error)
  return_error = check_json(output["context"]["benchmark_mode"], arguments["--mode"].replace("'", ''), "Benchmark mode doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["encoding"]["default"]["encoding"], arguments["--encoding"].replace("'", ''), "Encoding doesn't match with JSON:", return_error)
  return_error = check_json(str(output["context"]["using_scheduler"]).lower(), arguments["--scheduler"], "Scheduler doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["clients"], int(arguments["--clients"]), "Client count doesn't match with JSON:", return_error)

  if not glob.glob(arguments["--table_path"].replace("'", '') + "*.bin"):
    print ("ERROR: Cannot find binary tables in " + arguments["--table_path"])
    return_error = True

  os.system("rm -rf " + arguments["--table_path"] + "/*.bin")

  arguments = {}
  arguments["--table_path"] = "'resources/test_data/imdb_sample/'"
  arguments["--queries"] = "'3b'"
  arguments["--time"] = "10"
  arguments["--runs"] = "100"
  arguments["--warmup"] = "10"
  arguments["--encoding"] = "'LZ4'"
  arguments["--compression"] = "'Fixed-size byte-aligned'"
  arguments["--scheduler"] = "true"
  arguments["--clients"] = "4"
  arguments["--visualize"] = "true"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkJoinOrder", True)

  benchmark.expect("Running in multi-threaded mode using all available cores")
  benchmark.expect("4 simulated clients are scheduling items in parallel")
  benchmark.expect("Running benchmark in 'Ordered' mode")
  benchmark.expect("Visualizing the plans into SVG files. This will make the performance numbers invalid.")
  benchmark.expect("Encoding is 'LZ4'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per item is 100")
  benchmark.expect("Max duration per item is 10 seconds")
  benchmark.expect("Warmup duration per item is 10 seconds")
  benchmark.expect("Automatically verifying results with SQLite. This will make the performance numbers invalid.")
  benchmark.expect("Not caching tables as binary files")
  benchmark.expect("Benchmarking queries from third_party/join-order-benchmark")
  benchmark.expect("Running on tables from resources/test_data/imdb_sample/")
  benchmark.expect("Running subset of queries: 3b")
  benchmark.expect("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
