#!/bin/bash

set -e

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder'
num_mt_clients=50

if [ $# -ne 2 ]
then
  echo 'This script is used to compare the performance impact of a change. Running it takes more than an hour.'
  echo '  It compares two git revisions using various benchmarks (see below) and prints the result in a format'
  echo '  that is copyable to Github.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_all.sh master HEAD'
  exit 1
fi

start_commit_reference=$1
end_commit_reference=$2
shift; shift

start_commit=$(git rev-parse $start_commit_reference | head -n 1)
end_commit=$(git rev-parse $end_commit_reference | head -n 1)

if [[ $(git status --untracked-files=no --porcelain) ]]
then
  echo 'Cowardly refusing to execute on a dirty workspace'
  exit 1
fi

output=$(grep 'CMAKE_BUILD_TYPE:STRING=Release' CMakeCache.txt || true)
if [ -z "$output" ]
then
  echo 'Current folder is not configured as a release build'
  exit 1
fi

output=$(grep 'CMAKE_MAKE_PROGRAM' CMakeCache.txt | grep ninja || true)
if [ ! -z "$output" ]
then
  build='ninja'
else
  build='make'
fi

mkdir benchmark_all_results 2>/dev/null || true

build_folder=$(pwd)

for commit in $start_commit $end_commit
do
  if [ -f "benchmark_all_results/complete_${commit}" ]
  then
    echo "Benchmarks have already been executed for ${commit}. Skipping."
    echo "Run rm benchmark_all_results/*${commit}* to re-execute."
    sleep 1
    continue
  fi

  git checkout $start_commit
  git submodule update --init
  echo "Building $commit..."
  $build clean
  /usr/bin/time -p sh -c "( $build -j $(nproc) ${benchmarks} 2>&1 ) | tee benchmark_all_results/build_${commit}.log" 2>"benchmark_all_results/build_time_${commit}.txt"

  cd ..  # hyriseBenchmarkJoinOrder needs to run from project root 
  for benchmark in $benchmarks
  do
    echo "Running $benchmark for $commit... (single-threaded)"
    ( ${build_folder}/$benchmark -t 10 -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_st.log" # TODO remove -t

    echo "Running $benchmark for $commit... (multi-threaded)"
    ( ${build_folder}/$benchmark -t 10 --scheduler --clients ${num_mt_clients} -o "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt.json" 2>&1 ) | tee "${build_folder}/benchmark_all_results/${benchmark}_${commit}_mt.log" # TODO remove -t
  done
  cd "${build_folder}"
done

touch "benchmark_all_results/complete_${commit}"

echo "Comparing ${start_commit} and ${end_commit}..."

echo ""
echo "==========="
echo ""

echo "**System**"
echo "<details>"
echo "<summary>$(hostname) - click to expand</summary>"
echo ""
echo "| property | value |"
echo "| -- | -- |"
echo "| Hostname | $(hostname) |"
if [[ "$(uname)" == 'Darwin' ]]; then
  echo "| Model | $(defaults read ~/Library/Preferences/com.apple.SystemProfiler.plist 'CPU Names' | cut -sd '"' -f 4) |"
  echo "| CPU | $(sysctl -n machdep.cpu.brand_string) |"
  echo "| Memory | $(system_profiler SPHardwareDataType | grep "  Memory:" | sed 's/Memory://') |"
else
  echo "| CPU | $(lscpu | grep "Model name" | sed 's/Model name: *//') |"
  echo "| Memory | $(free --si -h | grep Mem | awk '{ print $4 "B" }') |"
  if [ -x /usr/bin/numactl ]; then
    for bind_type in nodebind membind
    do
      echo "| numactl nodebind | $(numactl --show | grep --color=never $(bind_type)) |"
    done
  else
    echo "| numactl | binary not found |"
  fi
fi
echo "</details>"

echo ""
echo "**Build Time**"
echo "| commit | build time |"
echo "| -- | -- |"
echo -n "| ${start_commit} |"
awk '{printf $0 "|\n"}' "${build_folder}/benchmark_all_results/build_time_${start_commit}.txt"
echo -n "| ${end_commit} |"
awk '{printf $0 "|\n"}' "${build_folder}/benchmark_all_results/build_time_${end_commit}.txt"

for benchmark in $benchmarks
do
  for config in st mt
  do
    output=$(../scripts/compare_benchmarks.py "${build_folder}/benchmark_all_results/${benchmark}_${start_commit}.json" "${build_folder}/benchmark_all_results/${benchmark}_${end_commit}.json" --github 2>/dev/null)

    echo ""
    echo ""
    echo -n "**${benchmark} - "
    case "${config}" in
      "st") echo -n "single-threaded" ;;
      "mt") echo -n "multi-threaded (${num_mt_clients} clients)" ;;
    esac
    echo "**"
    echo "<details>"
    echo "<summary>"
    echo "Sum of average per-item runtime: TODO,"
    echo "$output" | grep 'geometric mean' | sed 's/geometric mean//; s/[ |]//g; s/^/Geometric Mean: /'
    echo "</summary>"
    echo "$output"
    echo "</details>"
  done
done