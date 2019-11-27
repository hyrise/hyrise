#!/usr/bin/python

from hyriseBenchmarkCore import *

# This test runs the binary hyriseBenchmarkTPCH with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():

  return_error = False

  arguments = {}
  arguments["--scale"] = ".01"
  arguments["--use_prepared_statements"] = "true"
  arguments["--queries"] = "'1,13,19'"
  arguments["--time"] = "10"
  arguments["--runs"] = "-1"
  arguments["--output"] = "'json_output.txt'"
  arguments["--mode"] = "'Shuffled'"
  arguments["--encoding"] = "'Dictionary'"
  arguments["--compression"] = "'Fixed-size byte-aligned'"
  arguments["--indexes"] = "true"
  arguments["--scheduler"] = "false"
  arguments["--clients"] = "1"
  arguments["--cache_binary_tables"] = "false"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCH", True)

  benchmark.expect_exact("Writing benchmark results to 'json_output.txt'")
  benchmark.expect_exact("Running in single-threaded mode")
  benchmark.expect_exact("1 simulated clients are scheduling items in parallel")
  benchmark.expect_exact("Running benchmark in 'Shuffled' mode")
  benchmark.expect_exact("Encoding is 'Dictionary'")
  benchmark.expect_exact("Chunk size is 100000")
  benchmark.expect_exact("Max duration per item is 10 seconds")
  benchmark.expect_exact("No warmup runs are performed")
  benchmark.expect_exact("Not caching tables as binary files")
  benchmark.expect_exact("Benchmarking Queries: [ 1, 13, 19 ]")
  benchmark.expect_exact("TPCH scale factor is 0.01")
  benchmark.expect_exact("Using prepared statements: yes")
  benchmark.expect_exact("Creating index on customer [ c_custkey ]")
  benchmark.expect_exact("Preparing queries")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if not os.path.isfile(arguments["--output"].replace("'", "")):
    print ("ERROR: Cannot find output file " + arguments["--output"])
    return_error = True

  with open(arguments["--output"].replace("'", '')) as f:
    output = json.load(f)


  return_error = check_json(not output["summary"]["table_size_in_bytes"], 0, "Table size is zero.", return_error)
  return_error = check_json(output["context"]["scale_factor"], float(arguments["--scale"]), "Scale factor doesn't match with JSON:", return_error, 0.001)
  for i in xrange(0,3):
    return_error = check_json(output["benchmarks"][i]["name"].replace('TPC-H 0', '').replace('TPC-H ', ''), arguments["--queries"].replace("'", '').split(',')[i], "Query doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_duration"], int(arguments["--time"]) * 1e9, "Max duration doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["max_runs"], int(arguments["--runs"]), "Max runs don't match with JSON:", return_error)
  return_error = check_json(output["context"]["benchmark_mode"], arguments["--mode"].replace("'", ''), "Benchmark mode doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["encoding"]["default"]["encoding"], arguments["--encoding"].replace("'", ''), "Encoding doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["encoding"]["default"]["compression"], arguments["--compression"].replace("'", ''), "Compression doesn't match with JSON:", return_error)
  return_error = check_json(str(output["context"]["using_scheduler"]).lower(), arguments["--scheduler"], "Scheduler doesn't match with JSON:", return_error)
  return_error = check_json(output["context"]["clients"], int(arguments["--clients"]), "Client count doesn't match with JSON:", return_error)

  arguments = {}
  arguments["--scale"] = ".01"
  arguments["--chunk_size"] = "10000"
  arguments["--queries"] = "'2,4,6'"
  arguments["--time"] = "10"
  arguments["--runs"] = "100"
  arguments["--warmup"] = "10"
  arguments["--encoding"] = "'LZ4'"
  arguments["--compression"] = "'SIMD-BP128'"
  arguments["--indexes"] = "false"
  arguments["--scheduler"] = "true"
  arguments["--clients"] = "4"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCH", True)

  benchmark.expect_exact("Running in multi-threaded mode using all available cores")
  benchmark.expect_exact("4 simulated clients are scheduling items in parallel")
  benchmark.expect_exact("Running benchmark in 'Ordered' mode")
  benchmark.expect_exact("Encoding is 'LZ4'")
  benchmark.expect_exact("Chunk size is 10000")
  benchmark.expect_exact("Max runs per item is 100")
  benchmark.expect_exact("Max duration per item is 10 seconds")
  benchmark.expect_exact("Warmup duration per item is 10 seconds")
  benchmark.expect_exact("Benchmarking Queries: [ 2, 4, 6 ]")
  benchmark.expect_exact("TPCH scale factor is 0.01")
  benchmark.expect_exact("Using prepared statements: no")
  benchmark.expect_exact("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  # Finally test that pruning works end-to-end, that is from the command line parameter all the way to the visualizer
  arguments = {}
  arguments["--scale"] = ".01"
  arguments["--chunk_size"] = "10000"
  arguments["--queries"] = "'6'"
  arguments["--runs"] = "1"
  arguments["--visualize"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCH", True)

  benchmark.expect_exact("Visualizing the plans into SVG files. This will make the performance numbers invalid.")
  benchmark.expect_exact("Chunk size is 10000")
  benchmark.expect_exact("Benchmarking Queries: [ 6 ]")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  visualization_file = 'TPC-H_06-PQP.svg'
  if not os.path.isfile(visualization_file):
    print ("ERROR: Cannot find visualization file " + visualization_file)
    sys.exit(1)
  with open(visualization_file) as f:
    # Check whether the (a) the GetTable node exists and (b) the chunk count is correct for the given scale factor
    if not '/7 chunk(s)' in f.read():
      print ("ERROR: Did not find expected pruning information in the visualization file")
      sys.exit(1)

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
