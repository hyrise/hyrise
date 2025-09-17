#!/bin/bash
# This builds an optimized version of the binary with bolt
#
# The profile data is captured by binary instrumentation via bolt.
# You can find a version of file where the profile is captured using perf on Github in PR 2724
# In our experience, instrumentation resulted in similar performance and was more portable.
# This file needs to be executed inside a cmake build folder. We recommend having a separate folder
# as this file changes the cmake configuration.
#
# For a usage string, run this binary with -h

build_folder=$(pwd)
os="$(uname -s)"
case "${os}" in
  Linux*)   num_cores="$(lscpu -p | grep -Ev '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_cores="$(sysctl -n hw.physicalcpu)";;
esac
time_per_benchmark=1800
benchmarks=("hyriseBenchmarkTPCH" "hyriseBenchmarkTPCDS" "hyriseBenchmarkTPCC" "hyriseBenchmarkJoinOrder" "hyriseBenchmarkStarSchema")

# Parse cli args
VALID_ARGS=$(getopt -o "ht:n:c:" --longoptions "help,time:,num-cores:,compile-cores:" -- "$@")
USAGE="Usage: $(basename $0) [-h] [-t seconds per benchmark] [-n num cores and clients while profiling] [-c num cores while building] [benchmarks to be run ...]"
if [[ $? -ne 0 ]]
then
  exit 1
fi
eval set -- "$VALID_ARGS"

while [ : ]
do
  case "$1" in
    -h | --help)
      echo -e "Build an optimized version of the hyrise binary with BOLT\n$USAGE"
      exit 0
      ;;
    -n | --num-cores)
      if [ -z "$2" ] || ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid number of cores\n$USAGE"
          exit 1
      fi
      num_cores="$2"
      shift 2
      ;;
    -t | --time)
      if [ -z "$2" ] || ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid time\n$USAGE"
          exit 1
      fi
      time_per_benchmark="$2"
      shift 2
      ;;
    -c | --compile-cores)
      if [ -z "$2" ] || ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid number of cores\n$USAGE"
          exit 1
      fi
      compile_cores="$2"
      shift  2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo -e "Unknown positional argument $1\n$USAGE"
      shift
      ;;
  esac
done

if [ -z "$time_per_benchmark" ]
then
  echo 'Error: Cannot read the number of cores from the operating system and no number of cores specified in cli args.'
  exit 1
fi

if [ $# -gt 0 ]
then
  benchmarks="$@"
fi

cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCOMPILE_FOR_BOLT=TRUE ..

ninja clean
ninja all -j "$compile_cores"

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
llvm-bolt-17 lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so

pushd ..

for benchmark in benchmarks
do
    "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$time_per_benchmark" -m Shuffled
    mv /tmp/prof.fdata "$build_folder/$benchmark.fdata"
done

popd

merge-fdata-17 *.fdata > bolt.fdata

llvm-bolt-17 lib/libhyrise_impl.so.old -o lib/libhyrise_impl.so -data bolt.fdata -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold -split-eh -dyno-stats
