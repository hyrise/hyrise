#!/bin/bash

set -e

if [ $# -ne 1 ]
then
  echo 'This script is used to compare the performance impact of a change. Running it takes a few hours.'
  echo '  It compares two git revisions using various benchmarks (see below) and prints the result in a format'
  echo '  that is copyable to Github.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_all.sh origin/master HEAD'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema'
# Set to 1 because even a single warmup run of a query makes the observed runtimes much more stable. See discussion in #2405 for some preliminary reasoning.
warmup_seconds=1
mt_shuffled_runtime=1200
runs=100

# Setting the number of clients used for the multi-threaded scenario to the machine's physical core count. This only works for macOS and Linux.
output="$(uname -s)"
case "${output}" in
    Linux*)     num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
    Darwin*)    num_phy_cores="$(sysctl -n hw.physicalcpu)";;
    *)          echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

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
# Skip commits where we already have all results
if [ -f "benchmark_plugin_results/complete_${commit}" ]
then
  echo "Benchmarks have already been executed for ${commit}. Skipping."
  echo "Run rm benchmark_plugin_results/*${commit}* to re-execute."
  sleep 1  # Easiest way to make sure the above is read
else

  # Checkout and build from scratch, tracking the compile time
  echo $commit
  git checkout "$commit"
  git submodule update --init --recursive

  echo "Building $commit..."
  $build_system clean
  /usr/bin/time -p sh -c "( $build_system -j $(nproc) ${benchmarks} hyriseDependencyDiscoveryPlugin 2>&1 ) | tee benchmark_plugin_results/build_${commit}.log" 2>"benchmark_plugin_results/build_time_${commit}.txt"

  # Run the benchmarks
  cd ..  # hyriseBenchmarkJoinOrder needs to run from project root
  for benchmark in $benchmarks
  do
    echo "Running $benchmark for $commit... (single-threaded) NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" -r ${runs} -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st.log"

    echo "Running $benchmark for $commit... (single-threaded) w/ plugin NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" -r ${runs} -w ${warmup_seconds} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_st_plugin.log"

    echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client) NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered.log"

    echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client) w/ plugin NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_ordered_plugin.log"

    echo "Running $benchmark for $commit... (multi-threaded, shuffled, $num_phy_cores clients) NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -m Shuffled -t ${mt_shuffled_runtime} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt.json" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt.log"

    echo "Running $benchmark for $commit... (multi-threaded, shuffled, $num_phy_cores clients) w/ plugin NO SCHEMA CONSTRAINTS, ALL ON"
    ( SCHEMA_CONSTRAINTS=0 "${build_folder}"/"$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -m Shuffled -t ${mt_shuffled_runtime} -o "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_plugin.json" -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.${lib_suffix}" 2>&1 ) | tee "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_mt_plugin.log"
  done
  cd "${build_folder}"

  # After all benchmarks are done, leave a marker so that this commit can be skipped next time
  touch "benchmark_plugin_results/complete_${commit}"
fi

# Print the results

echo ""
echo "==========="
echo ""

# Print information for each benchmark
for benchmark in $benchmarks
do
  configs="st mt_ordered mt"

  for config in $configs
  do
    output=$(../scripts/compare_benchmarks.py "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_${config}.json" "${build_folder}/benchmark_plugin_results/${benchmark}_${commit}_${config}_plugin.json" --github 2>/dev/null)
    echo ""
    echo ""
    echo -n "**${benchmark} - "
      case "${config}" in
        "st") echo -n "single-threaded" ;;
        "mt") echo -n "multi-threaded, ordered, 1 client, ${num_phy_cores} cores" ;;
        "mt") echo -n "multi-threaded, shuffled, ${num_phy_cores} clients, ${num_phy_cores} cores" ;;
      esac

    echo "**"
    echo "<details>"
    echo "<summary>"
    echo "$output" | grep '| Sum ' | sed -E 's/^[+-]//' | awk -F"|" '{print $2, $6}'  | sed 's/Sum//; s/[ ]//g; s/^/Sum of avg. item runtimes: /'
    echo " || "
    echo "$output" | grep '| Geomean ' | sed -E 's/^[+-]//' | sed 's/Geomean//; s/[ |]//g; s/^/Geometric mean of throughput changes: /'
    echo "</summary>"
    echo ""
    echo "$output"
    echo "</details>"
  done
done
