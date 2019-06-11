#!/usr/bin/python

from hyriseBenchmarkCore import *

# This test runs the binary hyriseBenchmarkTPCH with two different sets of arguments.
# During the first run, the shell output is validated using pexpect.
# After the first run, this test checks if an output file was created and if it matches the arguments.
# During the second run, the shell output is validated using pexpect
# and the test checks if all queries were successfully verified with sqlite.
def main():

  return_error = False

  arguments1 = {}
  arguments1["--scale"] = ".01"
  arguments1["--use_prepared_statements"] = "true"
  arguments1["--queries"] = "'1,13,19'"
  arguments1["--time"] = "1"
  arguments1["--runs"] = "100"
  arguments1["--output"] = "'json_output.txt'"
  arguments1["--mode"] = "'PermutedQuerySet'"
  arguments1["--compression"] = "'Fixed-size byte-aligned'"
  arguments1["--scheduler"] = "true"
  arguments1["--clients"] = "4"
  arguments1["--mvcc"] = "true"
  arguments1["--visualize"] = "true"
  arguments1["--cache_binary_tables"] = "false"

  arguments2 = {}
  arguments2["--scale"] = ".005"
  arguments2["--queries"] = "'2,4,6'"
  arguments2["--time"] = "1"
  arguments2["--runs"] = "100"
  arguments2["--warmup"] = "1"
  arguments2["--encoding"] = "'LZ4'"
  arguments2["--compression"] = "'SIMD-BP128'"
  arguments2["--verify"] = "true"

  benchmark = initialize(arguments1, "hyriseBenchmarkTPCH")

  benchmark.expect("Writing benchmark results to 'json_output.txt'")
  benchmark.expect("MVCC is enabled")
  benchmark.expect("Running in multi-threaded mode using all available cores")
  benchmark.expect("4 simulated clients are scheduling queries in parallel")
  benchmark.expect("Running benchmark in 'PermutedQuerySet' mode")
  benchmark.expect("Visualization is on")
  benchmark.expect("Encoding is 'Dictionary'")
  benchmark.expect("Chunk size is 100000")
  benchmark.expect("Max runs per query is 100")
  benchmark.expect("Max duration per query is 1 seconds")
  benchmark.expect("No warmup runs are performed")
  benchmark.expect("Not caching tables as binary files")
  benchmark.expect("Benchmarking Queries: \[ 1, 13, 19, \]")
  benchmark.expect("TPCH scale factor is 0.01")
  benchmark.expect("Using prepared statements: yes")
  benchmark.expect("Multi-threaded Topology:")

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if not os.path.isfile(arguments1["--output"].replace("'", "")):
    print ("Cannot find output file " + arguments1["--output"])
    return_error = True

  with open(arguments1["--output"].replace("'", '')) as f:
    output = json.load(f)

  if output["summary"]["table_size_in_bytes"] == 0:
    return_error = True
    print("ERROR: Table size is zero.")
  if abs(output["context"]["scale_factor"] - float(arguments1["--scale"])) >= 0.001:
    return_error = True
    print("ERROR: Scale factor doesn't match with JSON:", output["context"]["scale_factor"], float(arguments1["--scale"]))
  for i in xrange(0,3):
    if output["benchmarks"][i]["name"].replace('TPC-H ', '') != arguments1["--queries"].replace("'", '').replace("[", '').replace("]", '').split(',')[i]:
      return_error = True
      print("ERROR: Query doesn't match with JSON:", output["benchmarks"][i]["name"].replace('TPC-H ', ''), arguments1["--queries"].replace("'", '').split(',')[i])
  if output["context"]["max_duration"] != int(arguments1["--time"]) * 1e9:
    return_error = True
    print("ERROR: Max duration doesn't match with JSON:", output["context"]["max_duration"], int(arguments1["--time"]) * 1e9)
  if output["context"]["max_runs"] != int(arguments1["--runs"]):
    return_error = True
    print("ERROR: Max runs don't match with JSON:", output["context"]["max_runs"], int(arguments1["--runs"]))
  if output["context"]["benchmark_mode"] != arguments1["--mode"].replace("'", ''):
    return_error = True
    print("ERROR: Benchmark mode doesn't match with JSON:", output["context"]["benchmark_mode"], arguments1["--mode"].replace("'", ''))
  if output["context"]["encoding"]["default"]["compression"] != arguments1["--compression"].replace("'", ''):
    return_error = True
    print("ERROR: Compression doesn't match with JSON:", output["context"]["encoding"]["default"]["compression"], arguments1["--compression"].replace("'", ''))
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

  benchmark = initialize(arguments2, "hyriseBenchmarkTPCH")

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
  benchmark.expect("Benchmarking Queries: \[ 2, 4, 6, \]")
  benchmark.expect("TPCH scale factor is 0.005")
  benchmark.expect("Using prepared statements: no")

  benchmark.expect("-> Executed")

  if benchmark.before.count('Verification failed'):
    return_error = True

  close_benchmark(benchmark)
  check_exit_status(benchmark)

  if return_error:
    sys.exit(1)

if __name__ == '__main__':
  main()
