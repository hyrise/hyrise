#!/bin/bash

# list of memory limits in GB
memory_limits=(1 2 3 5 10 20)

#if zero parameters are given, print usage
if [ $# -eq 0 ]; then
  echo "Usage: $0 <path to TPCHBenchmark executable with parameters> "
  exit 1
fi

# get the path to the executable
executable="$@"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# for each memory limit run the executable
for limit in "${memory_limits[@]}"
do
  echo "Running $executable with memory limit of $limit GB"
  source $SCRIPT_DIR/run_with_limited_memory.sh $limit $executable -o "benchmark_memory_limit_$limit.json"
  #hot fix as binary file removal is not yet working
  sudo rm -f *.bin
done

