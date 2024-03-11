#!/bin/bash

set -e

if [ $# -ne 1 ]
then
  echo 'Please provide a git revision!.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_compare_plugin_sf.sh HEAD'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkStarSchema'
scale_factors="1 10 20 30 40 50 60 70 80 90 100"
# Set to 1 because even a single warmup run of a query makes the observed runtimes much more stable. See discussion in #2405 for some preliminary reasoning.
warmup_seconds=0
runs=0

output="$(uname -s)"
case "${output}" in
    Linux*)     lib_suffix="so";;
    Darwin*)    lib_suffix="dylib";;
    *)          echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

# Retrieve SHA-1 hashes from arguments (e.g., translate "master" into an actual hash)
commit_reference=$1

commit=$(git rev-parse "$commit_reference" | head -n 1)

# Check status of repository
if [[ $(git status --untracked-files=no --porcelain) ]]
then
  echo 'Cowardly refusing to execute on a dirty workspace'
  exit 1
fi

# Ensure that this runs on a release build. Not really necessary, just a sanity check.
output=$(grep 'CMAKE_BUILD_TYPE:STRING=Release' CMakeCache.txt || true)
if [ -z "$output" ]
then
  echo 'Current folder is not configured as a release build'
  exit 1
fi

# Check whether to use ninja or make
output=$(grep 'CMAKE_MAKE_PROGRAM' CMakeCache.txt | grep ninja || true)
if [ -n "$output" ]
then
  build_system='ninja'
else
  build_system='make'
fi

# Create the result folder if necessary
mkdir benchmark_plugin_results 2>/dev/null || true

build_folder=$(pwd)


# Here comes the actual work
# Checkout and build from scratch, tracking the compile time
echo $commit
git checkout "$commit"
git submodule update --init --recursive

echo "Building $commit..."
if [[ -z "$FORCE_CLEAN"  ||  "$FORCE_CLEAN" != "false"  ]]
then
  echo "Clean build dir"
  $build_system clean
fi

/usr/bin/time -p sh -c "( $build_system -j $(nproc) ${benchmarks} hyriseDependencyDiscoveryPlugin 2>&1 ) | tee benchmark_plugin_results/build_${commit}.log" 2>"benchmark_plugin_results/build_time_${commit}.txt"

# Run the benchmarks
cd ..  # hyriseBenchmarkJoinOrder needs to run from project root
for benchmark in $benchmarks
do
  for sf in $scale_factors
  do

    runtime=$((sf * 6))
    runtime=$(( runtime > 60 ? runtime : 60 ))

    caching=""

    if [[ "$sf" != "10" && "$sf" != "1" ]]; then
      caching="--dont_cache_binary_tables"
    fi

    echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) w/ plugin NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} ${caching} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_plugin.log"
  done
done

benchmark="hyriseBenchmarkJoinOrder"
    echo "Running $benchmark for $commit... (single-threaded, w/ plugin NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_plugin.log"

cd "${build_folder}"

exit 0
