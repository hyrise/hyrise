#!/bin/bash

set -e

verlte() {
    [  "$1" = "`echo -e "$1\n$2" | sort -V | head -n1`" ]
}

verlt() {
    [ "$1" = "$2" ] && return 1 || verlte $1 $2
}

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

# We need at least clang 7.0.1 for coverage: https://github.com/hyrise/hyrise/pull/1433#issuecomment-458082341
installed_clang_version=`${path_to_compiler}clang++ --version | head -n1 | cut -d" " -f3 | cut -d"-" -f1`
required_clang_version=7.0.1

if verlt ${installed_clang_version} ${required_clang_version} ; then
    echo "Minimum clang version $required_clang_version not met, is $installed_clang_version"
    exit -1
fi

cmake -DCMAKE_CXX_COMPILER_LAUNCHER=$launcher -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=${path_to_compiler}clang -DCMAKE_CXX_COMPILER=${path_to_compiler}clang++ -DENABLE_COVERAGE=ON ..

cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
make hyriseTest -j $((cores / 2))
cd -

rm -fr coverage; mkdir coverage
./build-coverage/hyriseTest build-coverage --gtest_filter=-SQLiteTestRunnerInstances/*
  
# merge the profile data using the llvm-profdata tool:
${path_to_compiler}llvm-profdata merge -o ./default.profdata ./default.profraw

# run LLVM’s code coverage tool
${path_to_compiler}llvm-cov show -format=html -instr-profile ./default.profdata build-coverage/hyriseTest -output-dir=./coverage ./src/lib/

echo Coverage Information is in ./coverage/index.html

# Continuing only if diff output is needed with Linux/gcc
if [ "true" == "$generate_badge" ]; then

  ${path_to_compiler}llvm-cov report -ignore-filename-regex=specialization/llvm/ -instr-profile ./default.profdata build-coverage/hyriseTest ./src/lib/ > coverage.txt

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
