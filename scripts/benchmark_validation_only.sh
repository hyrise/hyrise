#!/bin/bash

set -e

if [ $# -ne 1 ]
then
  echo 'Please provide a git revision!.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_single_optimizations.sh HEAD'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkStarSchema'
sf='10'
# Set to 1 because even a single warmup run of a query makes the observed runtimes much more stable. See discussion in #2405 for some preliminary reasoning.
warmup_seconds=0
runs=0
runtime=60

if [[ -n "$SCALE_FACTOR" ]]
then
  sf=${SCALE_FACTOR}
fi
echo "Using a scale factor of ${sf} for TPC-H, TPC-DS, and SSB."
sleep 1

# Setting the number of clients used for the multi-threaded scenario to the machine's physical core count. This only works for macOS and Linux.
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

/usr/bin/time sh -c "( $build_system -j $(nproc) ${benchmarks} hyriseBenchmarkJoinOrder hyriseDependencyDiscoveryPlugin 2>&1 ) | tee benchmark_plugin_results/build_${commit}.log" 2>"benchmark_plugin_results/build_time_${commit}.txt"

# Run the benchmarks
cd ..  # hyriseBenchmarkJoinOrder needs to run from project root
for benchmark in $benchmarks
do

  echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
  ( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/"$benchmark" -s ${sf} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_schema_plugin.log"

  echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_plugin.log"

  echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY DEPENDENT_GROUPBY"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_plugin_dgr.log"

  echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_SEMI"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_plugin_jts.log"

  echo "Running $benchmark for $commit... (single-threaded, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_PREDICATE"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -r ${runs} -t ${runtime} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_s${sf}_plugin_jtp.log"
done


echo "Running hyriseBenchmarkJoinOrder for $commit... (single-threaded) SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/hyriseBenchmarkJoinOrder -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_st_schema_plugin.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (single-threaded) NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_st_plugin.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (single-threaded) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY DEPENDENT_GROUPBY"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_st_plugin_dgr.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (single-threaded) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_SEMI"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_st_plugin_jts.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (single-threaded) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_PREDICATE"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -r ${runs} -w ${warmup_seconds} -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_st_plugin_jtp.log"

cd "${build_folder}"

exit 0
