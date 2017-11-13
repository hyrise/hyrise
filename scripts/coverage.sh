#!/bin/bash
set -e

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

cd $1
make hyriseCoverage -j \$(( \$(cat /proc/cpuinfo | grep processor | wc -l) / 2))
cd -

./$1/hyriseCoverage
rm -fr coverage; mkdir coverage
# call gcovr twice b/c of https://github.com/gcovr/gcovr/issues/112
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -g --html --html-details -o coverage/index.html > coverage_output.txt
cat coverage_output.txt

if [ ! -z "$2" ]
then
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
