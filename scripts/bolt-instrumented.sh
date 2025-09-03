#!/bin/bash
# This file captures profile data for bolt
#
# The profile data is captured by binary instrumentation via bolt.
# It therefore does not require a CPU with a last branch register and can run on both AMD and INTEL

CMAKE_DIR="cmake-build-bolt-instrumented"

mkdir "$CMAKE_DIR"

pushd "$CMAKE_DIR"
cmake -DCMAKE_UNITY_BUILD=ON -GNinja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCOMPILE_FOR_BOLT=TRUE ..
ninja all -j $(nproc)

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.source
llvm-bolt lib/libhyrise_impl.so.source -instrument -o lib/libhyrise_impl.so
popd

for benchmark in hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema
do
    # TODO: Multi-threaded benchmarks?
    "$CMAKE_DIR/$benchmark" -t 360 -m Shuffled
    mv /tmp/prof.fdata "$CMAKE_DIR/$benchmark.fdata"
done

merge-fdata "$CMAKE_DIR/*.fdata" > resources/bolt.fdata
