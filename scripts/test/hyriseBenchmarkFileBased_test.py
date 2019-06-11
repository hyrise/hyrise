#!/usr/bin/python

from hyriseBenchmarkCore import *

# This test runs the binary hyriseBenchmarkFileBased with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():

  return_error = False

  arguments1 = {}
  arguments1["--table_path"] = "'resources/test_data/tbl/file_based/'"
  arguments1["--query_path"] = "'resources/test_data/queries/file_based/'"
  arguments1["--queries"] = "'select_statement'"
  arguments1["--time"] = "1"
  arguments1["--runs"] = "100"
  arguments1["--output"] = "'json_output.txt'"
  arguments1["--mode"] = "'PermutedQuerySet'"
  arguments1["--encoding"] = "'Unencoded'"
  arguments1["--scheduler"] = "true"
  arguments1["--clients"] = "4"
  arguments1["--mvcc"] = "true"
  arguments1["--visualize"] = "true"
  arguments1["--cache_binary_tables"] = "true"

  arguments2 = {}
  arguments2["--table_path"] = "'resources/test_data/tbl/file_based/'"
  arguments2["--query_path"] = "'resources/test_data/queries/file_based/'"
  arguments2["--queries"] = "'select_statement'"
  arguments2["--time"] = "1"
  arguments2["--runs"] = "100"
  arguments2["--warmup"] = "1"
  arguments2["--encoding"] = "'LZ4'"
  arguments2["--compression"] = "'SIMD-BP128'"
  arguments2["--verify"] = "true"

  os.system("rm -rf " + arguments2["--table_path"] + "/*.bin")

  benchmark = initialize(arguments1, "hyriseBenchmarkFileBased")

  benchmark.expect("Writing benchmark results to 'json_output.txt'")
  benchmark.expect("MVCC is enabled")
  benchmark.expect("Running in multi-threaded mode using all available cores")
  benchmark.expect("4 simulated clients are scheduling queries in parallel")
  benchmark.expect("Running benchmark in 'PermutedQuerySet' mode")
  benchmark.expect("Visualization is on")
  benchmark.expect("Encoding is 'Unencoded'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per query is 100")
  benchmark.expect("Max duration per query is 1 seconds")
  benchmark.expect("No warmup runs are performed")
  benchmark.expect("Caching tables as binary files")
  benchmark.expect("Benchmarking queries from resources/test_data/queries/file_based/")
  benchmark.expect("Running on tables from resources/test_data/tbl/file_based/")
  benchmark.expect("Running subset of queries: select_statement")
  benchmark.expect("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if not glob.glob(arguments1["--table_path"].replace("'", '') + "*.bin"):
    print ("Cannot find binary tables in " + arguments1["--table_path"])
    return_error = True

  if not os.path.isfile(arguments1["--output"].replace("'", "")):
    print ("Cannot find output file " + arguments1["--output"])
    return_error = True

  with open(arguments1["--output"].replace("'", '')) as f:
    output = json.load(f)

  if output["summary"]["table_size_in_bytes"] == 0:
    return_error = True
    print("ERROR: Table size is zero.")
  if output["benchmarks"][0]["name"] != arguments1["--queries"].replace("'", ''):
    return_error = True
    print("ERROR: Queries dont match with JSON:", output["benchmarks"][0]["name"], arguments1["--queries"].replace("'", ''))
  if output["context"]["max_duration"] != int(arguments1["--time"]) * 1e9:
    return_error = True
    print("ERROR: Max duration doesn't match with JSON:", output["context"]["max_duration"], int(arguments1["--time"]) * 1e9)
  if output["context"]["max_runs"] != int(arguments1["--runs"]):
    return_error = True
    print("ERROR: Max runs don't match with JSON:", output["context"]["max_runs"], int(arguments1["--runs"]))
  if output["context"]["benchmark_mode"] != arguments1["--mode"].replace("'", ''):
    return_error = True
    print("ERROR: Benchmark mode doesn't match with JSON:", output["context"]["benchmark_mode"], arguments1["--mode"].replace("'", ''))
  if output["context"]["encoding"]["default"]["encoding"] != arguments1["--encoding"].replace("'", ''):
    return_error = True
    print("ERROR: Encoding doesn't match with JSON:", output["context"]["encoding"]["default"]["encoding"], arguments1["--encoding"].replace("'", ''))
  if output["context"]["using_scheduler"] != bool(arguments1["--scheduler"]):
    return_error = True
    print("ERROR: Scheduler doesn't match with JSON:", output["context"]["using_scheduler"], bool(arguments1["--scheduler"]))
  if output["context"]["clients"] != int(arguments1["--clients"]):
    return_error = True
    print("ERROR: Client count doesn't match with JSON:", output["context"]["clients"], int(arguments1["--clients"]))
  if output["context"]["using_mvcc"] != bool(arguments1["--mvcc"]):
    return_error = True
    print("ERROR: MVCC doesn't match with JSON:", output["context"]["using_mvcc"], bool(arguments1["--mvcc"]))
  if output["context"]["using_visualization"] != bool(arguments1["--visualize"]):
    return_error = True
    print("ERROR: Visualization doesn't match with JSON:", output["context"]["using_visualization"], bool(arguments1["--visualize"]))

  os.system("rm -rf " + arguments1["--table_path"] + "/*.bin")

  benchmark = initialize(arguments2, "hyriseBenchmarkFileBased")

  benchmark.expect("Writing benchmark results to stdout")
  benchmark.expect("MVCC is disabled")
  benchmark.expect("Running in single-threaded mode")
  benchmark.expect("1 simulated clients are scheduling queries in parallel")
  benchmark.expect("Running benchmark in 'IndividualQueries' mode")
  benchmark.expect("Visualization is off")
  benchmark.expect("Encoding is 'LZ4'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per query is 100")
  benchmark.expect("Max duration per query is 1 seconds")
  benchmark.expect("Warmup duration per query is 1 seconds")
  benchmark.expect("Automatically verifying results with SQLite. This will make the performance numbers invalid.")
  benchmark.expect("Not caching tables as binary files")
  benchmark.expect("Benchmarking queries from resources/test_data/queries/file_based/")
  benchmark.expect("Running on tables from resources/test_data/tbl/file_based/")
  benchmark.expect("Running subset of queries: select_statement")

  benchmark.expect("-> Executed")

  if benchmark.before.count('Verification failed'):
    return_error = True

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
