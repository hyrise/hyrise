#!/bin/bash
# This file captures profile data for bolt
#
# It is written to run only on cpus with a last branch register (i.e. Intel).
# AMD cpus most likely don't have it. If you still want to run this script on AMD
# then follow the comments below. There will be a performance hit. It is possible to
# capture the profile data on an intel cpu and then optimize and run the binary on AMD.
#
# This script also requires perf to be installed. The perf installation has to match the kernel version.
# This can be tricky in the case of containers, as you don't know the kernel version at build time.
# There are commands below for installing perf on an ubuntu 24.04 container to match a ubuntu 22.04 or 20.04 kernel.
# Keep in mind that you have to run them on the target host to receive accurate information from uname -r
#echo "deb http://archive.ubuntu.com/ubuntu jammy main universe" | sudo tee /etc/apt/sources.list.d/jammy.list
#echo "deb http://archive.ubuntu.com/ubuntu jammy-updates main universe" | sudo tee -a /etc/apt/sources.list.d/jammy.list
#echo "deb http://security.ubuntu.com/ubuntu jammy-security main universe" | sudo tee -a /etc/apt/sources.list.d/jammy.list
#echo "deb http://archive.ubuntu.com/ubuntu focal main universe" | sudo tee /etc/apt/sources.list.d/focal.list
#echo "deb http://archive.ubuntu.com/ubuntu focal-updates main universe" | sudo tee -a /etc/apt/sources.list.d/focal.list
#echo "deb http://security.ubuntu.com/ubuntu focal-security main universe" | sudo tee -a /etc/apt/sources.list.d/focal.list
#apt update
#apt install linux-tools-common linux-tools-`uname -r`

mkdir clang-bolt
cd clang-bolt
cmake -DCMAKE_UNITY_BUILD=ON -GNinja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCOMPILE_FOR_BOLT=TRUE ..
ninja all -j $(nproc)

benchmarks='hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkTPCC hyriseBenchmarkJoinOrder hyriseBenchmarkStarSchema'
mkdir -p ../resources/bolt
for benchmark in $benchmarks
do
    # TODO: Multi-threaded benchmarks?

    # remove -j any,u on cpus that do not have a last branch register (i.e. AMD)
    perf record -e cycles:u -j any,u -o $benchmark.data -- ./$benchmark -t 360 -m Shuffled
    # Add -nl on cpus that do not have a last branch register (i.e. AMD)
    perf2bolt-17 -p $benchmark.data -o ../resources/bolt/$benchmark.fdata ./$benchmark
done
