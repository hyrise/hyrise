#!/bin/bash

set -e

if [ $# -ne 2 ]
then
  echo 'This script is used to compare the performance impact of a change. Running it takes more than an hour.'
  echo '  It compares two git revisions using various benchmarks (see below) and prints the result in a format'
  echo '  that is copyable to Github.'
  echo 'Typical call (in a release build folder): ../scripts/benchmark_all.sh master HEAD'
  exit 1
fi

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder'
num_mt_clients=50
files_to_delete='CMakeCache.txt CPackConfig.cmake CPackSourceConfig.cmake build.ninja cmake_install.cmake build.ninja cmake_install.cmake provider.hpp rules.ninja version.hpp'
dirs_to_delete='CMakeFiles bin include lib resources src third_party'

for file in $files_to_delete
do
  # echo "${file}"
  rm -f $file
done

for file in $dirs_to_delete
do
  #echo "${file}"
  rm -rf $file
done

# Retrieve SHA-1 hashes from arguments (e.g., translate "master" into an actual hash)
commit_reference=$1
config=$2

commit=$(git rev-parse $commit_reference | head -n 1)

# Check status of repository
if [[ $(git status --untracked-files=no --porcelain) ]]
then
  echo 'Cowardly refusing to execute on a dirty workspace'
  exit 1
fi


build_system='ninja'
plugin_path='/home/Daniel.Lindner/hyrise/cmake-build-release/lib/libhyriseDependencyMiningPlugin.so'

# Create the result folder if necessary
mkdir benchmark_midterm 2>/dev/null || true

build_folder=$(pwd)

# Checkout and build from scratch, tracking the compile time
git checkout "dey4ss/thesis"
git checkout $commit
git submodule update --init --recursive
echo "Building $commit..."
cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -GNinja ..
$build_system clean
/usr/bin/time -p sh -c "( $build_system -j $(nproc) ${benchmarks} hyriseDependencyMiningPlugin 2>&1 ) | tee benchmark_midterm/build_${config}.log" 2>"benchmark_midterm/build_time_${config}.txt"
# $build_system hyriseBenchmarkFileBased

# Run the benchmarks
cd ..  # hyriseBenchmarkJoinOrder needs to run from project root
for benchmark in $benchmarks
do
echo "Running $benchmark for $commit... (single-threaded)"
if [ "$benchmark" = "hyriseBenchmarkFileBased" ]; then
  ( ${build_folder}/$benchmark -o "${build_folder}/benchmark_midterm/${benchmark}_${config}_st.json" -r 100 -w 5 --table_path tpcds_cached_tables/sf-10/ --query_path tpcds_date_between/runnable/  2>&1 ) | tee "${build_folder}/benchmark_midterm/${benchmark}_${config}_st.log"
else
  ( ${build_folder}/$benchmark -o "${build_folder}/benchmark_midterm/${benchmark}_${config}_st.json" --dep_mining_plugin $plugin_path -r 100 -w 5 2>&1 ) | tee "${build_folder}/benchmark_midterm/${benchmark}_${config}_st.log"
fi
done

cd "${build_folder}"

# After all benchmarks are done, leave a marker so that this commit can be skipped next time
touch "benchmark_midterm/complete_${config}"


# Print the results

echo ""
echo "==========="
echo ""
