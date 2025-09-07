#!/bin/bash
# This file captures profile data for bolt
#
# The profile data is captured by binary instrumentation via bolt.
# It therefore does not require a CPU with a last branch register and can run on both AMD and INTEL

CMAKE_DIR="cmake-build-bolt-instrumented"

mkdir "$CMAKE_DIR"

pushd "$CMAKE_DIR"
cmake -DCMAKE_UNITY_BUILD=ON -GNinja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCOMPILE_FOR_BOLT=TRUE ..
ninja all

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.source
llvm-bolt lib/libhyrise_impl.so.source -instrument -o lib/libhyrise_impl.so
popd

output="$(uname -s)"
case "${output}" in
  Linux*)   num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_phy_cores="$(sysctl -n hw.physicalcpu)";;
  *)        echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

for benchmark in hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema
do
    "$CMAKE_DIR/$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -t 1800 -m Shuffled
    mv /tmp/prof.fdata "$CMAKE_DIR/$benchmark.fdata"
done

merge-fdata "$CMAKE_DIR/*.fdata" > resources/bolt.fdata
