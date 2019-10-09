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
  arguments["--runs"] = "100"
  arguments["--output"] = "'json_output.txt'"
  arguments["--mode"] = "'Shuffled'"
  arguments["--encoding"] = "'Dictionary'"
  arguments["--compression"] = "'Fixed-size byte-aligned'"
  arguments["--indexes"] = "true"
  arguments["--scheduler"] = "false"
  arguments["--clients"] = "1"
  arguments["--cache_binary_tables"] = "false"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCH", True)

  benchmark.expect("Writing benchmark results to 'json_output.txt'")
  benchmark.expect("Running in single-threaded mode")
  benchmark.expect("1 simulated clients are scheduling items in parallel")
  benchmark.expect("Running benchmark in 'Shuffled' mode")
  benchmark.expect("Encoding is 'Dictionary'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per item is 100")
  benchmark.expect("Max duration per item is 10 seconds")
  benchmark.expect("No warmup runs are performed")
  benchmark.expect("Not caching tables as binary files")
  benchmark.expect("Benchmarking Queries: \[ 1, 13, 19, \]")
  benchmark.expect("TPCH scale factor is 0.01")
  benchmark.expect("Using prepared statements: yes")
  benchmark.expect("Creating index on customer \[ c_custkey \]")
  benchmark.expect("Preparing queries")

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
  arguments["--scale"] = ".005"
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
  arguments["--visualize"] = "true"
  arguments["--verify"] = "true"

  benchmark = initialize(arguments, "hyriseBenchmarkTPCH", True)

  benchmark.expect("Running in multi-threaded mode using all available cores")
  benchmark.expect("4 simulated clients are scheduling items in parallel")
  benchmark.expect("Running benchmark in 'Ordered' mode")
  benchmark.expect("Visualizing the plans into SVG files. This will make the performance numbers invalid.")
  benchmark.expect("Encoding is 'LZ4'")
  benchmark.expect("Chunk size is 10000")
  benchmark.expect("Max runs per item is 100")
  benchmark.expect("Max duration per item is 10 seconds")
  benchmark.expect("Warmup duration per item is 10 seconds")
  benchmark.expect("Benchmarking Queries: \[ 2, 4, 6, \]")
  benchmark.expect("TPCH scale factor is 0.005")
  benchmark.expect("Using prepared statements: no")
  benchmark.expect("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  visualization_file = 'TPC-H_06-PQP.svg'
  if not os.path.isfile(visualization_file):
    print ("ERROR: Cannot find visualization file " + visualization_file)
    sys.exit(1)
  with open(visualization_file) as f:
    # Check whether the (a) the GetTable node exists and (b) the chunk count is correct for the given scale factor
    if not '/4 chunk(s)' in f.read():
      print ("ERROR: Did not find expected pruning information in the visualization file")
      sys.exit(1)


  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
