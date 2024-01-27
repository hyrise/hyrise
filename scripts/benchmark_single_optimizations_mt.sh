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
warmup_seconds=1
runs=100

# Setting the number of clients used for the multi-threaded scenario to the machine's physical core count. This only works for macOS and Linux.
output="$(uname -s)"
case "${output}" in
    Linux*)     num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
    Darwin*)    num_phy_cores="$(sysctl -n hw.physicalcpu)";;
    *)          echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

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

  runtime=$((sf * 6))
  runtime=$(( runtime > 60 ? runtime : 60 ))

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) NO SCHEMA CONSTRAINTS, ALL OFF"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_all_off.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_all_off.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) SCHEMA CONSTRAINTS, ALL ON"
  ( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_schema.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_schema.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
  ( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_schema_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_schema_plugin.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY DEPENDENT_GROUPBY"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_dgr.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_dgr.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_SEMI"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_jts.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_jts.log"

  echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client, SF ${sf}) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_PREDICATE"
  ( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/"$benchmark" -s ${sf} -t ${runtime} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_jtp.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_s${sf}_plugin_jtp.log"
done

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, ALL OFF"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0  "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_all_off.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_all_off.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) SCHEMA CONSTRAINTS, ALL ON"
( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_schema.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_schema.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
( SCHEMA_CONSTRAINTS=1 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_schema_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_schema_plugin.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, PLUGIN, ALL ON"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY DEPENDENT_GROUPBY"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=1 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_dgr.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_dgr.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_SEMI"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=1 JOIN_TO_PREDICATE=0 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_jts.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_jts.log"

echo "Running hyriseBenchmarkJoinOrder for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, PLUGIN, ONLY JOIN_TO_PREDICATE"
( SCHEMA_CONSTRAINTS=0 DEPENDENT_GROUPBY=0 JOIN_TO_SEMI=0 JOIN_TO_PREDICATE=1 VALIDATION_LOOPS=1000 "${build_folder}"/hyriseBenchmarkJoinOrder -t 60 --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_jtp.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/hyriseBenchmarkJoinOrder_${commit}_mt_ordered_plugin_jtp.log"

cd "${build_folder}"

exit 0
