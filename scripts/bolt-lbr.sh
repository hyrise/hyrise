#!/bin/bash
# This file captures profile data for bolt
#
# It is written to run only on cpus with a last branch register (i.e. Intel).
#
# This script also requires perf to be installed. The perf installation has to match the kernel version.
# This can be tricky in the case of containers, as you don't know the kernel version at build time.
# There are commands below for installing perf on an ubuntu 24.04 container to match a ubuntu 22.04 or 20.04 kernel.
# Keep in mind that you have to run them on the target host to receive accurate information from uname -r
# echo "deb http://archive.ubuntu.com/ubuntu jammy main universe" | sudo tee /etc/apt/sources.list.d/jammy.list
# echo "deb http://archive.ubuntu.com/ubuntu jammy-updates main universe" | sudo tee -a /etc/apt/sources.list.d/jammy.list
# echo "deb http://security.ubuntu.com/ubuntu jammy-security main universe" | sudo tee -a /etc/apt/sources.list.d/jammy.list
# echo "deb http://archive.ubuntu.com/ubuntu focal main universe" | sudo tee /etc/apt/sources.list.d/focal.list
# echo "deb http://archive.ubuntu.com/ubuntu focal-updates main universe" | sudo tee -a /etc/apt/sources.list.d/focal.list
# echo "deb http://security.ubuntu.com/ubuntu focal-security main universe" | sudo tee -a /etc/apt/sources.list.d/focal.list
# apt update
# apt install linux-tools-common linux-tools-`uname -r`

CMAKE_DIR="cmake-build-bolt-lbr"

mkdir "$CMAKE_DIR"

pushd "$CMAKE_DIR"
cmake -DCMAKE_UNITY_BUILD=ON -GNinja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCOMPILE_FOR_BOLT=TRUE ..
ninja all
popd

output="$(uname -s)"
case "${output}" in
  Linux*)   num_phy_cores="$(lscpu -p | egrep -v '^#' | grep '^[0-9]*,[0-9]*,0,0' | sort -u -t, -k 2,4 | wc -l)";;
  Darwin*)  num_phy_cores="$(sysctl -n hw.physicalcpu)";;
  *)        echo 'Unsupported operating system. Aborting.' && exit 1;;
esac

for benchmark in hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema
do
    perf record -e cycles:u -j any,u -o "$CMAKE_DIR/$benchmark.data" -- "$CMAKE_DIR/$benchmark" --scheduler --clients ${num_phy_cores} --cores ${num_phy_cores} -t 1800 -m Shuffled
    perf2bolt -p "$CMAKE_DIR/$benchmark.data" -o "$CMAKE_DIR/$benchmark.fdata" "$CMAKE_DIR/lib/libhyrise_impl.so"
done

merge-fdata "$CMAKE_DIR/*.fdata" > resources/bolt.fdata
