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
platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
   # Use GCC for Linux
   cmake -DCMAKE_CXX_COMPILER_LAUNCHER=$launcher -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DENABLE_COVERAGE=ON ..
elif [[ "$unamestr" == 'Darwin' ]]; then
   # Use Clang for OS X
   cmake -DCMAKE_CXX_COMPILER_LAUNCHER=$launcher -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++ -DENABLE_COVERAGE=ON ..
fi

cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
make hyriseTest -j $((cores / 2))
cd -

rm -fr coverage; mkdir coverage
./build-coverage/hyriseTest build-coverage

if [[ "$unamestr" == 'Darwin' ]]; then
  # LLVM has its own way of dealing with coverage...
  # https://llvm.org/docs/CoverageMappingFormat.html
  
  # merge the profile data using the llvm-profdata tool:
  /usr/local/opt/llvm/bin/llvm-profdata merge -o ./default.profdata ./default.profraw

  # run LLVM’s code coverage tool
  /usr/local/opt/llvm/bin/llvm-cov show -format=html -instr-profile default.profdata build-coverage/hyriseTest -output-dir=coverage src/lib/

  exit
fi

# Continuing only with Linux/gcc

# call gcovr twice b/c of https://github.com/gcovr/gcovr/issues/112
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k

if [ "true" == "$generate_badge" ]
then
    # in this step, keep coverage information only if we need it for pycobertura later
    keep="-k"
fi

# generate HTML
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches $keep -g --html --html-details -o coverage/index.html > coverage_output.txt
cat coverage_output.txt

if [ "true" == "$generate_badge" ]
then
    # generate XML for pycobertura
    gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -g --xml > coverage.xml
    curl -g -o coverage_master.xml https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/hyrise/job/master/lastStableBuild/artifact/coverage.xml
    pycobertura diff coverage_master.xml coverage.xml --format html --output coverage_diff.html || true

    # coverage badge generation
    coverage_percent=$(cat coverage_output.txt | grep lines: | sed -e 's/lines: //; s/% .*$//')
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
fi

rm coverage_output.txt
