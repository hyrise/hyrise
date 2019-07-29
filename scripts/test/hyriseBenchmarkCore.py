import os
import sys
import pexpect
import glob
import json

def close_benchmark(benchmark):
  benchmark.expect(pexpect.EOF, timeout=None)
  benchmark.close()

def check_exit_status(benchmark):
  if benchmark.exitstatus == None:
    sys.exit(benchmark.signalstatus)

def check_json(json, argument, error, return_error, difference=None):
  if type(json) is not float:
    if json != argument:
      print("ERROR: " + error + " " + str(json) + " " + str(argument))
      return True
  else:
    if abs(json - argument) > difference:
      print("ERROR: " + error + " " + str(json) + " " + str(argument) + " " + str(difference))
      return True
  return return_error

def initialize(arguments, benchmark_name, verbose):
  if len(sys.argv) == 1:
    print ("Usage: ./scripts/test/" + benchmark_name + "_test.py <build_dir>")
    sys.exit(1)

  if "--table_path" in arguments and not os.path.isdir(arguments["--table_path"].replace("'", "")):
    print ("Cannot find " + arguments["--table_path"] + ". Are you running the test suite from the main folder of the Hyrise repository?")
    sys.exit(1)

  if "--query_path" in arguments and not os.path.isdir(arguments["--query_path"].replace("'", "")):
    print ("Cannot find " + arguments["--query_path"] + ". Are you running the test suite from the main folder of the Hyrise repository?")
    sys.exit(1)

  build_dir = sys.argv[1]

  concat_arguments = ' '.join(['='.join(map(str, x)) for x in arguments.items()])

  benchmark = pexpect.spawn(build_dir + "/" + benchmark_name + " " + concat_arguments, maxread=1000000, timeout=600, dimensions=(200, 64))
  if verbose:
    benchmark.logfile = sys.stdout
  return benchmark
