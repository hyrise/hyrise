#!/bin/bash
# This builds an optimized version of the binary with bolt.
#
# The profile data is captured by binary instrumentation via bolt. You can find a file where the profile is captured
# using perf here: https://github.com/hyrise/hyrise/blob/d0c46cb61dca9d3dcbbf1f39c0e886bcb6c844fe/scripts/bolt-lbr.sh.
# In our experience, instrumentation resulted in similar performance and was more portable. This file needs to be
# executed inside a cmake build folder. We recommend having a separate cmake folder for bolt as this file changes the
# cmake configuration.
#
# For a usage string, run this binary with -h

build_folder=$(pwd)

# Get the number of available cpu cores from the os
os="$(uname -s)"
case "${os}" in
  Linux*)   num_cores="$(lscpu -p | grep -Ev '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_cores="$(sysctl -n hw.physicalcpu)";;
  *)        echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

# We recommend profiling for at least some time. However, this value is overwritten in the CI for faster runs.
seconds_per_benchmark=1800
benchmarks=("hyriseBenchmarkTPCH" "hyriseBenchmarkTPCDS" "hyriseBenchmarkTPCC" "hyriseBenchmarkJoinOrder" "hyriseBenchmarkStarSchema")

# Parse cli args
VALID_ARGS=$(getopt -o "ht:c" --longoptions "help,time:,cli" -- "$@")
USAGE="Usage: $(basename $0) [-h] [-t seconds per benchmark] [--cli]"
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
    -c | --cli)
      seconds_per_benchmark=120
      benchmarks=("hyriseBenchmarkTPCH" "hyriseBenchmarkTPCDS" "hyriseBenchmarkTPCC" "hyriseBenchmarkStarSchema")
      num_cores=8
      cli=1
      shift
      ;;
    --)
      shift
      break
    *)
      echo -e "Unknown positional argument $1\n$USAGE"
      shift
      ;;
  esac
done


# We have to add a few compile flags, so BOLT can work:
# > BOLT operates on X86-64 and AArch64 ELF binaries. At the minimum, the binaries should have an unstripped symbol
# > table, and, to get maximum performance gains, they should be linked with relocations (--emit-relocs or -q linker
# > flag).
# > NOTE: BOLT is currently incompatible with the -freorder-blocks-and-partition compiler option. Since GCC8 enables
# > this option by default, you have to explicitly disable it by adding -fno-reorder-blocks-and-partition flag if you
# > are compiling with GCC8 or above.
# https://github.com/llvm/llvm-project/tree/main/bolt
cmake -DCOMPILE_FOR_BOLT=TRUE ..

ninja clean
# Only compile the benchmarks that we need.
time ninja ${benchmarks[@]} -j "$num_cores"

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
time llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so

pushd ..

for benchmark in ${benchmarks[@]}
do
  # We use shuffled runs with high pressure to profile cases that are relevant.
  if [ "$cli" -eq 1 ]
  then
    # Use a minimum scale factor for runs in cli
    if [ "$benchmark" == "hyriseBenchmarkTPCH" -o "$benchmark" == "hyriseBenchmarkStarSchema" ]
    then
      # These benchmarks support floating point scaling factors
      time "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s .01
    else
      # These benchmarks require integer scaling factors
      time "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s 1
    fi
  else
    time "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled
  fi
  mv /tmp/prof.fdata "$build_folder/$benchmark.fdata"
done

popd

time merge-fdata *.fdata > bolt.fdata

time llvm-bolt lib/libhyrise_impl.so.old -o lib/libhyrise_impl.so -data bolt.fdata -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold -split-eh -dyno-stats

# time strip lib/libhyrise_impl.so
