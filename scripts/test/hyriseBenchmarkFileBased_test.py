#!/usr/bin/python

from hyriseBenchmarkCore import *

# This test runs the binary hyriseBenchmarkFileBased with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():

  return_error = False

  arguments = {}
  arguments["--table_path"] = "'resources/test_data/tbl/file_based/'"
  arguments["--query_path"] = "'resources/test_data/queries/file_based/'"
  arguments["--queries"] = "'select_statement'"
  arguments["--time"] = "10"
  arguments["--runs"] = "100"
  arguments["--output"] = "'json_output.txt'"
  arguments["--mode"] = "'Shuffled'"
  arguments["--encoding"] = "'Unencoded'"
  arguments["--scheduler"] = "false"
  arguments["--clients"] = "1"
  arguments["--cache_binary_tables"] = "true"

  os.system("rm -rf " + arguments["--table_path"] + "/*.bin")

  benchmark = initialize(arguments, "hyriseBenchmarkFileBased", True)

  benchmark.expect_exact("Writing benchmark results to 'json_output.txt'")
  benchmark.expect_exact("Running in single-threaded mode")
  benchmark.expect_exact("1 simulated clients are scheduling items in parallel")
  benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
  benchmark.expect_exact("Encoding is 'Unencoded'")
  benchmark.expect_exact("Chunk size is 100000")
  benchmark.expect_exact("Max runs per item is 100")
  benchmark.expect_exact("Max duration per item is 10 seconds")
  benchmark.expect_exact("No warmup runs are performed")
  benchmark.expect_exact("Caching tables as binary files")
  benchmark.expect_exact("Benchmarking queries from resources/test_data/queries/file_based/")
  benchmark.expect_exact("Running on tables from resources/test_data/tbl/file_based/")
  benchmark.expect_exact("Running subset of queries: select_statement")
  benchmark.expect_exact("-> Executed")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if not glob.glob(arguments["--table_path"].replace("'", '') + "*.bin"):
    print ("ERROR: Cannot find binary tables in " + arguments["--table_path"])
    return_error = True

  os.system("rm -rf " + arguments["--table_path"] + "/*.bin")
  
  if not os.path.isfile(arguments["--output"].replace("'", "")):
    print ("ERROR: Cannot find output file " + arguments["--output"])
    return_error = True

  with open(arguments["--output"].replace("'", '')) as f:
    output = json.load(f)

  return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
  return_error = check_json(output["benchmarks"][0]["name"], arguments["--queries"].replace("'", '').split(',')[0], "Query doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_duration"], int(arguments["--time"]) * 1e9, "Max duration doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_runs"], int(arguments["--runs"]), "Max runs don't match with JSON:", return_error)
  return_error = check_json(output["context"]["benchmark_mode"], arguments["--mode"].replace("'", ''), "Benchmark mode doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["encoding"]["default"]["encoding"], arguments["--encoding"].replace("'", ''), "Encoding doesn't match with JSON:", return_error)
  return_error = check_json(str(output["context"]["using_scheduler"]).lower(), arguments["--scheduler"], "Scheduler doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["clients"], int(arguments["--clients"]), "Client count doesn't match with JSON:", return_error)

  arguments = {}
  arguments["--table_path"] = "'resources/test_data/tbl/file_based/'"
  arguments["--query_path"] = "'resources/test_data/queries/file_based/'"
  arguments["--queries"] = "'select_statement'"
  arguments["--time"] = "10"
  arguments["--runs"] = "100"
  arguments["--warmup"] = "5"
  arguments["--encoding"] = "'LZ4'"
  arguments["--compression"] = "'SIMD-BP128'"
  arguments["--scheduler"] = "true"
  arguments["--clients"] = "4"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkFileBased", True)

  benchmark.expect_exact("Running in multi-threaded mode using all available cores")
  benchmark.expect_exact("4 simulated clients are scheduling items in parallel")
  benchmark.expect_exact("Running benchmark in 'Ordered' mode")
  benchmark.expect_exact("Encoding is 'LZ4'")
  benchmark.expect_exact("Chunk size is 100000")
  benchmark.expect_exact("Max runs per item is 100")
  benchmark.expect_exact("Max duration per item is 10 seconds")
  benchmark.expect_exact("Warmup duration per item is 5 seconds")
  benchmark.expect_exact("Automatically verifying results with SQLite. This will make the performance numbers invalid.")
  benchmark.expect_exact("Not caching tables as binary files")
  benchmark.expect_exact("Benchmarking queries from resources/test_data/queries/file_based/")
  benchmark.expect_exact("Running on tables from resources/test_data/tbl/file_based/")
  benchmark.expect_exact("Running subset of queries: select_statement")
  benchmark.expect_exact("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if benchmark.before.count('Verification failed'):
    return_error = True

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
