#!/bin/bash
# This builds an optimized version of the binary with PGO and bolt.
#
# For a usage string, run this binary with -h

build_folder=$(pwd)

# Get the number of available cpu cores from the os
os="$(uname -s)"
case "${os}" in
  Linux*)   num_cores="$(lscpu -p | grep -Ev '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_cores="$(sysctl -n hw.physicalcpu)";;
esac

# We recommend profiling for at least some time. However, this value is overwritten in the CI for faster runs.
seconds_per_benchmark=1800
benchmarks=()

# Parse cli args
VALID_ARGS=$(getopt -o "ht:n:c:b:" --longoptions "help,time:,num-cores:,compile-cores:benchmark:" -- "$@")
USAGE="Usage: $(basename $0) [-h] [-t seconds per benchmark] [-n num cores and clients while profiling] [-c num cores while building] [-b benchmark to be run (can be repeated)]"
if [[ $? -ne 0 ]]
then
  exit 1
fi
eval set -- "$VALID_ARGS"

# Populate variables with CLI args
while [ : ]
do
  case "$1" in
    -h | --help)
      echo -e "Build an optimized version of the hyrise binary with BOLT\n$USAGE"
      exit 0
      ;;
    -n | --num-cores)
      # Check if num cores is a number
      if ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid number of cores\n$USAGE"
          exit 1
      fi
      num_cores="$2"
      shift 2
      ;;
    -t | --time)
      # Check if time is a number
      if ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid time\n$USAGE"
          exit 1
      fi
      seconds_per_benchmark="$2"
      shift 2
      ;;
    -c | --compile-cores)
      # Check if num compile cores is a number
      if ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid number of cores\n$USAGE"
          exit 1
      fi
      compile_cores="$2"
      shift  2
      ;;
    -b | --benchmark)
      benchmarks+=("$2")
      shift 2
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

# Check if time is empty. This can only happen if the os and the user both did not provide a value.
if [ -z "$seconds_per_benchmark" ]
then
  echo 'Error: Cannot read the number of cores from the operating system and no number of cores specified in cli args.'
  exit 1
fi

# Check if the benchmarks array is empty. This can only happen if the user does not supply any benchmarks.
if [ ${#arr[@]} -eq 0 ]
then
  benchmarks=("hyriseBenchmarkTPCH" "hyriseBenchmarkTPCDS" "hyriseBenchmarkTPCC" "hyriseBenchmarkJoinOrder" "hyriseBenchmarkStarSchema")
fi

cmake -DCOMPILE_FOR_BOLT=TRUE -DPGO_INSTRUMENT ..

ninja clean
# Only compile the benchmarks that we need.
ninja ${arr[@]} -j "$compile_cores"

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so

pushd ..

for benchmark in $benchmarks
do
  # We use shuffled runs with high pressure to profile cases that are relevant.
  "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled
  mv /tmp/prof.fdata "$build_folder/$benchmark.fdata"
  mv default.profraw "$build_folder/$benchmark.profraw"
done

popd

merge-fdata *.fdata > ../resources/bolt.fdata
llvm-profdata merge -output ../resources/pgo.profdata *.profraw
