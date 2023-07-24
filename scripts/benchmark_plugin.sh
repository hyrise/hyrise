#!/bin/bash

set -e

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema'

build_folder=$(pwd)

cd .. 

for benchmark in $benchmarks
do
  echo "Running $benchmark"
  ( "${build_folder}"/"$benchmark" -r 0 -p "${build_folder}/lib/libhyriseDependencyDiscoveryPlugin.dylib" 2>&1 ) | tee "${build_folder}/${benchmark}.log"
done

cd "${build_folder}"
