#!/bin/bash

set -e

while [ $# -gt 0 ]; do
  case "$1" in
    --generate_badge=*)
      generate_badge="${1#*=}"
      ;;
    --launcher=*)
      launcher="${1#*=}"
      ;;
    *)
      printf "Error: Invalid argument."
      exit 1
  esac
  shift
done

if [ ! -d "third_party" ]; then
  echo "You should call this script from the root of the project"
  exit
fi

mkdir -p build-coverage
cd build-coverage

path_to_compiler=''

unamestr=`uname`
if [[ "$unamestr" == 'Darwin' ]]; then
   # Use homebrew clang for OS X
   path_to_compiler='/usr/local/opt/llvm/bin/'
fi

cmake -DCMAKE_CXX_COMPILER_LAUNCHER=$launcher -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=${path_to_compiler}clang -DCMAKE_CXX_COMPILER=${path_to_compiler}clang++ -DENABLE_COVERAGE=ON ..

cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
make hyriseTest -j $((cores / 2))
cd -

rm -fr coverage; mkdir coverage
./build-coverage/hyriseTest build-coverage --gtest_filter=-SQLiteTestRunnerInstances/*
  
# merge the profile data using the llvm-profdata tool:
${path_to_compiler}llvm-profdata merge -o ./default.profdata ./default.profraw

# gather list of files for SOURCES environment variable (because llvm-cov does not support exclusions)
SOURCES=$(${path_to_compiler}llvm-cov show -dump-collected-paths -instr-profile ./default.profdata build-coverage/hyriseTest ./src/lib/ 2>&1 | awk '{print "./" substr($0,'$((${#PWD}+2))')}' | grep -v operators/jit_operator/specialization/llvm/)

# run LLVM’s code coverage tool
${path_to_compiler}llvm-cov show -format=html -instr-profile ./default.profdata build-coverage/hyriseTest -output-dir=./coverage $SOURCES

echo Coverage Information is in ./coverage/index.html

# Continuing only if diff output is needed with Linux/gcc
if [ "true" == "$generate_badge" ]; then

  ${path_to_compiler}llvm-cov report -instr-profile ./default.profdata build-coverage/hyriseTest $SOURCES > coverage.txt

  # coverage badge generation
  coverage_percent=$(tail -c 7 coverage.txt)
  # cut off percentage sign and new line byte
  coverage_percent=${coverage_percent:0:5}
  echo $coverage_percent > coverage_percent.txt
  if (( $(bc <<< "$coverage_percent >= 90") ))
  then
      color="brightgreen"
  elif (( $(bc <<< "$coverage_percent >= 75") ))
  then
      color="yellow"
  elif (( $(bc <<< "$coverage_percent < 75") ))
  then
      color="red"
  fi

  url="https://img.shields.io/badge/Coverage-$coverage_percent%25-$color.svg"
  curl -g -o coverage_badge.svg $url

  rm coverage.txt
fi

rm default.profdata default.profraw
