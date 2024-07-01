#!/bin/bash

set -e

if [ $# -ne 2 ]
then
  echo 'This script is used to compare the performance impact of a change. Running it takes a few hours.'
  echo '  It compares two git revisions using various benchmarks (see below) and prints the result in a format'
  echo '  that is copyable to Github.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_all.sh origin/master HEAD'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema'
# Set to 1 because even a single warmup run of a query makes the observed runtimes much more stable. See discussion in
# #2405 for some preliminary reasoning.
warmup_seconds=1
mt_shuffled_runtime=1200
runs=50

# Setting the number of clients used for the multi-threaded scenario to the machine's physical core count. This only
# works for macOS and Linux. We do not use hyper-threads because the benchmark results are less stable on them. We
# assume that the physical cores have lower IDs than the hyper-threads. Furthermore, we restrict the benchmarks to run
# on one NUMA node, even if the script is not orchestrated via numactl, as Hyrise is not optimized for multiple nodes.
# To obtain the number of cores per node, we just count the physical cores for the node with ID 0.
#
# Note: there is no limitation of running Hyrise on all CPU threads and multiple NUMA nodes. However, we are not
# actively testing and optimizing for these scenarios. Hence, we do not test them in this script.
output="$(uname -s)"
case "${output}" in
  Linux*)   num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_phy_cores="$(sysctl -n hw.physicalcpu)";;
  *)        echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

# We run two TPC-C scenarios. Once with high contention with as least twice as many clients as warehouses to test the
# performance of Hyrise's concurrency control. And once a more realistic scenario with no more clients than warehouses.
# For a server with 64 cores, the high contention scenario runs 64 clients on 10 warehouses (low contention: 10 clients,
# 10 warehouses). For an 8 core system, the high contention scenario runs 8 clients on 4 warehouses (low contention: 8
# clients, 10 warehouses).
tpcc_shuffled_runtime=600
tpcc_warehouse_count=10
tpcc_warehouse_count_high_contention=$((num_phy_cores/2<tpcc_warehouse_count ? num_phy_cores/2 : tpcc_warehouse_count))
clients_tpcc_high_contention=$num_phy_cores
clients_tpcc_low_contention=$((num_phy_cores<tpcc_warehouse_count ? num_phy_cores : tpcc_warehouse_count))

# Retrieve SHA-1 hashes from arguments (e.g., translate "master" into an actual hash).
start_commit_reference=$1
end_commit_reference=$2

start_commit=$(git rev-parse "$start_commit_reference" | head -n 1)
end_commit=$(git rev-parse "$end_commit_reference" | head -n 1)

# Check status of repository.
if [[ $(git status --untracked-files=no --porcelain) ]]
then
  echo 'Cowardly refusing to execute on a dirty workspace.'
  exit 1
fi

# Ensure that this runs on a release build. Not really necessary, just a sanity check.
output=$(grep 'CMAKE_BUILD_TYPE:STRING=Release' CMakeCache.txt || true)
if [ -z "$output" ]
then
  echo 'Current folder is not configured as a release build.'
  exit 1
fi

# Check whether to use ninja or make.
output=$(grep 'CMAKE_MAKE_PROGRAM' CMakeCache.txt | grep ninja || true)
if [ -n "$output" ]
then
  build_system='ninja'
else
  build_system='make'
fi

# Create the result folder if necessary.
mkdir benchmark_all_results 2>/dev/null || true

build_folder=$(pwd)

# Here comes the actual work.
for commit in $start_commit $end_commit
do
  # Skip commits where we already have all results.
  if [ -f "benchmark_all_results/complete_${commit}" ]
  then
    echo "Benchmarks have already been executed for ${commit}. Skipping."
    echo "Run rm benchmark_all_results/*${commit}* to re-execute."
    sleep 1  # Easiest way to make sure the above is read.
    continue
  fi

  # Checkout and build from scratch, tracking the compile time.
  git checkout "$commit"
  git submodule update --init --recursive
  echo "Building $commit..."
  $build_system clean
  /usr/bin/time -p sh -c "( $build_system -j $(nproc) ${benchmarks} 2>&1 ) | tee benchmark_all_results/build_${commit}.log" 2>"benchmark_all_results/build_time_${commit}.txt"

  # Run the benchmarks.
  cd ..  # hyriseBenchmarkJoinOrder needs to run from project root.
  for benchmark in $benchmarks
  do
    if [ "$benchmark" = "hyriseBenchmarkTPCC" ]; then
      echo "Running $benchmark for $commit... (single-threaded)"
      # Warming up does not make sense/much of a difference for TPCC.
      ( "${build_folder}"/"$benchmark" -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.log"
    else
      echo "Running $benchmark for $commit... (single-threaded)"
      ( "${build_folder}"/"$benchmark" -r ${runs} -w ${warmup_seconds} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.log"
    fi

    if [ "$benchmark" = "hyriseBenchmarkTPCH" ]; then
      echo "Running $benchmark for $commit... (single-threaded, SF 0.01)"
      ( "${build_folder}"/"$benchmark" -s .01 -r ${runs} -w ${warmup_seconds} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st_s01.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st_s01.log"
    fi

    if [ "$benchmark" != "hyriseBenchmarkTPCC" ]; then
      echo "Running $benchmark for $commit... (multi-threaded, ordered, 1 client)"
      ( "${build_folder}"/"$benchmark" --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -r ${runs} -w ${warmup_seconds} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_ordered.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_ordered.log"
    fi

    if [ "$benchmark" = "hyriseBenchmarkTPCC" ]; then
      # We test two scenarios: high and normal contention (see above, each for 10 minutes).
      echo "Running $benchmark for $commit... (multi-threaded, shuffled, high contention: $clients_tpcc_high_contention clients and $tpcc_warehouse_count_high_contention warehouses)"
      ( "${build_folder}"/"$benchmark" --scheduler --clients ${clients_tpcc_high_contention} --cores ${num_phy_cores} -s $tpcc_warehouse_count_high_contention -m Shuffled -t ${tpcc_shuffled_runtime} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_highcont.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_highcont.log"
      echo "Running $benchmark for $commit... (multi-threaded, shuffled, low contention: $clients_tpcc_low_contention clients and $tpcc_warehouse_count warehouses)"
      ( "${build_folder}"/"$benchmark" --scheduler --clients ${clients_tpcc_low_contention} --cores ${num_phy_cores} -s ${tpcc_warehouse_count} -m Shuffled -t ${tpcc_shuffled_runtime} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_lowcont.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt_lowcont.log"
    else
      echo "Running $benchmark for $commit... (multi-threaded, shuffled, $num_phy_cores clients)"
      ( "${build_folder}"/"$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -m Shuffled -t ${mt_shuffled_runtime} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt.log"
    fi
  done
  cd "${build_folder}"

  # After all benchmarks are done, leave a marker so that this commit can be skipped next time.
  touch "benchmark_all_results/complete_${commit}"
done

# Print the results.

echo ""
echo "==========="
echo ""

# Print information about the system.
echo "**System**"
echo "<details>"
echo "<summary>$(hostname) - click to expand</summary>"
echo ""
echo "| property | value |"
echo "| -- | -- |"
echo "| Hostname | $(hostname) |"
if [[ "$(uname)" == 'Darwin' ]]; then
  # Retrieve the model name. Might be more than one line if the hardware has been updated.
  echo "| Model | $(defaults read ~/Library/Preferences/com.apple.SystemProfiler.plist 'CPU Names' | cut -sd '"' -f 4 | tail -n 1) |"
  echo "| CPU | $(sysctl -n machdep.cpu.brand_string) |"
  echo "| Memory | $(system_profiler SPHardwareDataType | grep '  Memory:' | sed 's/Memory://') |"
else
  echo "| CPU | $(lscpu | grep 'Model name' | sed 's/Model name: *//') |"
  echo "| Memory | $(free --si -h | grep Mem | awk '{ print $4 "B" }') |"
  if [ -x /usr/bin/numactl ]; then
    for bind_type in nodebind membind
    do
      echo "| numactl | $(numactl --show | grep --color=never ${bind_type}) |"
    done
  else
    echo "| numactl | binary not found |"
  fi
fi
echo "</details>"

# Print information about the time spent building the commits.
echo ""
echo "**Commit Info and Build Time**"
echo "| commit | date | message | build time |"
echo "| -- | -- | -- | -- |"
echo -n "| $(git show -s --date=format:'%d.%m.%Y %H:%M' --format='%h | %cd | %s' "${start_commit}") | "
xargs < "${build_folder}/benchmark_all_results/build_time_${start_commit}.txt" | awk '{printf $0 "|\n"}'
echo -n "| $(git show -s --date=format:'%d.%m.%Y %H:%M' --format='%h | %cd | %s' "${end_commit}") | "
xargs < "${build_folder}/benchmark_all_results/build_time_${end_commit}.txt" | awk '{printf $0 "|\n"}'

# Print information for each benchmark.
for benchmark in $benchmarks
do
  case "${benchmark}" in
    "hyriseBenchmarkTPCC")  configs="st mt_highcont mt_lowcont";;
    "hyriseBenchmarkTPCH")  configs="st st_s01 mt_ordered mt";;
    *)                      configs="st mt_ordered mt";;
  esac

  for config in $configs
  do
    output=$(../scripts/compare_benchmarks.py "${build_folder}/benchmark_all_results/${benchmark}_${start_commit}_${config}.json" "${build_folder}/benchmark_all_results/${benchmark}_${end_commit}_${config}.json" --github 2>/dev/null)
    echo ""
    echo ""
    echo -n "**${benchmark} - "
    if [ "$benchmark" = "hyriseBenchmarkTPCH" ]; then
      case "${config}" in
        "st")          echo -n "single-threaded, SF 10.0" ;;
        "st_s01")      echo -n "single-threaded, SF 0.01" ;;
        "mt")          echo -n "multi-threaded, shuffled, ${num_phy_cores} clients, ${num_phy_cores} cores, SF 10.0" ;;
        "mt_ordered")  echo -n "multi-threaded, ordered, 1 client, ${num_phy_cores} cores, SF 10.0" ;;
      esac
    elif [ "$benchmark" = "hyriseBenchmarkTPCC" ]; then
      case "${config}" in
        "st")          echo -n "single-threaded" ;;
        "mt_highcont") echo -n "multi-threaded, shuffled, $clients_tpcc_high_contention clients, $tpcc_warehouse_count_high_contention warehouses, ${num_phy_cores} cores (high contention)" ;;
        "mt_lowcont")  echo -n "multi-threaded, shuffled, $clients_tpcc_low_contention clients, $tpcc_warehouse_count warehouses, ${num_phy_cores} cores (low contention)" ;;
      esac
    else
      case "${config}" in
        "st")          echo -n "single-threaded" ;;
        "mt")          echo -n "multi-threaded, shuffled, ${num_phy_cores} clients, ${num_phy_cores} cores" ;;
        "mt_ordered")  echo -n "multi-threaded, ordered, 1 client, ${num_phy_cores} cores" ;;
      esac
    fi

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
