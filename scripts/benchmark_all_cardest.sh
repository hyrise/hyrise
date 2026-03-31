#!/bin/bash

set -e

if [ $# -ne 0 ]
then
  echo 'This script runs cardinality-estimation benchmark scenarios on the current branch only.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_all_cardest.sh'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema'
# Set to 1 because even a single warmup run of a query makes the observed runtimes much more stable.
warmup_seconds=1
mt_shuffled_runtime=1200
runs=50

# Setting the number of clients used for the multi-threaded scenario to the machine's physical core count.
os_name="$(uname -s)"
case "${os_name}" in
  Linux*)
    num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)"
    ;;
  Darwin*)
    num_phy_cores="$(sysctl -n hw.physicalcpu)"
    ;;
  *)
    echo 'Unsupported operating system. Aborting.'
    exit 1
    ;;
esac

# Ensure that this runs on a release build.
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

build_jobs=${num_phy_cores}
if [ -z "${build_jobs}" ] || [ "${build_jobs}" -lt 1 ] 2>/dev/null; then
  build_jobs=1
fi

timestamp=$(date +%Y%m%d_%H%M%S)
result_dir="benchmark_all_results_cardest_${timestamp}"
mkdir -p "${result_dir}"

echo "Building benchmark binaries on current branch..."
${build_system} clean
/usr/bin/time -p sh -c "( ${build_system} -j ${build_jobs} ${benchmarks} 2>&1 ) | tee ${result_dir}/build.log" 2>"${result_dir}/build_time.txt"

echo ""
echo "Running additional pipeline-metrics commands first..."

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=1 OD_PREDICATE=0 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkStarSchema -r 1 -s 10 --pipeline_metrics -o "${result_dir}/ssb_ind_ucc.json" \
  2>&1 | tee "${result_dir}/ssb_ind_ucc.log"

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=0 OD_PREDICATE=1 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkStarSchema -r 1 -s 10 --pipeline_metrics -o "${result_dir}/ssb_od_predicate.json" \
  2>&1 | tee "${result_dir}/ssb_od_predicate.log"

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=1 OD_PREDICATE=0 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkTPCH -r 1 -s 10 --pipeline_metrics -o "${result_dir}/tpch_ind_ucc.json" \
  2>&1 | tee "${result_dir}/tpch_ind_ucc.log"

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=0 OD_PREDICATE=1 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkTPCH -r 1 -s 10 --pipeline_metrics -o "${result_dir}/tpch_od_predicate.json" \
  2>&1 | tee "${result_dir}/tpch_od_predicate.log"

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=1 OD_PREDICATE=0 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkTPCDS -r 1 -s 10 --pipeline_metrics -o "${result_dir}/tpcds_ind_ucc.json" \
  2>&1 | tee "${result_dir}/tpcds_ind_ucc.log"

WITH_ODS=1 FD_PREDICATE=0 UCC_FD_AGGREGATE=0 IND_UCC_JOIN=0 OD_PREDICATE=1 NEW_INSTANCE_OPTIMIZED=OFF \
  ./hyriseBenchmarkTPCDS -r 1 -s 10 --pipeline_metrics -o "${result_dir}/tpcds_od_predicate.json" \
  2>&1 | tee "${result_dir}/tpcds_od_predicate.log"

echo ""
echo "Running remaining benchmarks with two environment profiles..."

run_profile() {
  local profile_name=$1
  local env_prefix=$2
  local benchmark=$3

  echo "Running ${benchmark} (${profile_name})... (single-threaded)"
  eval "${env_prefix} ./${benchmark} -r ${runs} -w ${warmup_seconds} -o \"${result_dir}/${benchmark}_${profile_name}_st.json\"" \
    2>&1 | tee "${result_dir}/${benchmark}_${profile_name}_st.log"

  if [ "${benchmark}" = "hyriseBenchmarkTPCH" ]; then
    echo "Running ${benchmark} (${profile_name})... (single-threaded, SF 0.01)"
    eval "${env_prefix} ./${benchmark} -s .01 -r ${runs} -w ${warmup_seconds} -o \"${result_dir}/${benchmark}_${profile_name}_st_s01.json\"" \
      2>&1 | tee "${result_dir}/${benchmark}_${profile_name}_st_s01.log"
  fi

  echo "Running ${benchmark} (${profile_name})... (multi-threaded, ordered, 1 client)"
  eval "${env_prefix} ./${benchmark} --scheduler --clients 1 --cores ${num_phy_cores} -m Ordered -r ${runs} -w ${warmup_seconds} -o \"${result_dir}/${benchmark}_${profile_name}_mt_ordered.json\"" \
    2>&1 | tee "${result_dir}/${benchmark}_${profile_name}_mt_ordered.log"

  echo "Running ${benchmark} (${profile_name})... (multi-threaded, shuffled, ${num_phy_cores} clients)"
  eval "${env_prefix} ./${benchmark} --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -m Shuffled -t ${mt_shuffled_runtime} -o \"${result_dir}/${benchmark}_${profile_name}_mt.json\"" \
    2>&1 | tee "${result_dir}/${benchmark}_${profile_name}_mt.log"
}

for benchmark in ${benchmarks}
do
  run_profile "profile_off" "WITH_ODS=1 IND_UCC_JOIN=0 OD_PREDICATE=0 NEW_INSTANCE_OPTIMIZED=OFF" "${benchmark}"
  run_profile "profile_on" "WITH_ODS=1 IND_UCC_JOIN=1 OD_PREDICATE=0 NEW_INSTANCE_OPTIMIZED=ON" "${benchmark}"
done

echo ""
echo "All benchmark runs completed."
echo "Results directory: ${result_dir}"
