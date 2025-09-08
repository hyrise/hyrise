#!/bin/bash
# This file captures profile data for bolt
#
# The profile data is captured by binary instrumentation via bolt.
# It therefore does not require a CPU with a last branch register and can run on both AMD and INTEL

build_folder=$(pwd)

ninja clean
ninja all

mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old
llvm-bolt-17 lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so

cd ..

output="$(uname -s)"
case "${output}" in
  Linux*)   num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_phy_cores="$(sysctl -n hw.physicalcpu)";;
  *)        echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

for benchmark in hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema
do
    "$build_folder/$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -t 1800 -m Shuffled
    mv /tmp/prof.fdata "$build_folder/$benchmark.fdata"
done

merge-fdata-17 "$build_folder/*.fdata" > resources/bolt.fdata
