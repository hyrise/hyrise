#!/bin/bash
# This script builds an optimized version of the binary with bolt.
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
VALID_ARGS=$(getopt -o "ht:n:c" --longoptions "help,time:,ncpu:cli" -- "$@")
USAGE="Usage: $(basename $0) [-h] [-t seconds per benchmark] [-n number of cores used] [--cli]"
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
    -n | --ncpu)
      # Check if this is a number
      if ! [[ "$2" =~ ^[0-9]+$ ]]
      then
          echo -e "$2 is not a valid number of cores\n$USAGE"
          exit 1
      fi
      num_cores="$2"
      shift 2
      ;;
    -c | --cli)
      cli=1
      shift
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


# Build the benchmarks with PGO instrumentation
cmake -DCOMPILE_FOR_BOLT=ON -DPGO_INSTRUMENT=ON ..
ninja clean
if [ "$cli" == "1" ]
then
  time ninja hyriseBenchmarkTPCH hyriseBenchmarkStarSchema hyriseBenchmarkTPCDS hyriseBenchmarkTPCC -j "$num_cores"
else
  time ninja ${benchmarks[@]} -j "$num_cores"
fi

# Instrument with BOLT
mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
time llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so

# Run benchmarks and move prof.fdata (BOLT) and *.profraw (PGO) to the build folder
pushd ..

if [ "$cli" == "1" ]
then
    # We use a minimum scale factor for runs in cli
    time "$build_folder/hyriseBenchmarkTPCH" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s .01
    mv /tmp/prof.fdata "$build_folder/hyriseBenchmarkTPCH.fdata"
    time "$build_folder/hyriseBenchmarkStarSchema" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s .01
    mv /tmp/prof.fdata "$build_folder/hyriseBenchmarkStarSchema.fdata"
    time "$build_folder/hyriseBenchmarkTPCDS" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s 1
    mv /tmp/prof.fdata "$build_folder/hyriseBenchmarkTPCDS.fdata"
    time "$build_folder/hyriseBenchmarkTPCC" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled -s 1
    mv /tmp/prof.fdata "$build_folder/hyriseBenchmarkTPCC.fdata"
else
  for benchmark in ${benchmarks[@]}
  do
    # We use shuffled runs with high pressure to profile cases that are relevant.
    time "$build_folder/$benchmark" --scheduler --clients "$num_cores" --cores "$num_cores" -t "$seconds_per_benchmark" -m Shuffled
    mv /tmp/prof.fdata "$build_folder/$benchmark.fdata"
  done
fi

# PGO can merge the profile of multiple runs while generating it, so we don't need to move the profile after every
# benchmark. The name of the profile is default_<some id assigned to the hyrise lib>.profraw. PGO assigns those ids to
# distinguish profile data from different files. So there should only be a single .profraw file.
mv *.profraw "$build_folder/libhyrise.profraw"

popd

# Prepare profiles for optimization. For BOLT, we have to merge the different profiles. For PGO, we have to change the
# format of the profile.
time merge-fdata *.fdata > bolt.fdata
time llvm-profdata merge -output all.profdata libhyrise.profraw

# Build with PGO optimization
cmake -DPGO_INSTRUMENT=OFF -DPGO_PROFILE=all.profdata ..
ninja clean
if [ "$cli" -eq 1 ]
then
  time ninja hyriseTest -j "$num_cores"
else
  time ninja -j "$num_cores"
fi

# Optimize with BOLT
mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
time llvm-bolt lib/libhyrise_impl.so.old -o lib/libhyrise_impl.so -data bolt.fdata -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold -split-eh -dyno-stats

# Strip static relocations (which have been added to support BOLT)
time strip -R .rela.text -R ".rela.text.*" -R .rela.data -R ".rela.data.*" lib/libhyrise_impl.so
