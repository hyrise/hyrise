#!/bin/bash
set -e

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

cd $1
cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
make hyriseCoverage -j $((cores / 2))
cd -

./$1/hyriseCoverage
rm -fr coverage; mkdir coverage
# call gcovr twice b/c of https://github.com/gcovr/gcovr/issues/112
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k

# generate HTML
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k -g --html --html-details -o coverage/index.html > coverage_output.txt
cat coverage_output.txt

if [ ! -z "$2" ]
then
    # generate XML for pycobertura
    gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -g --xml > coverage.xml
    curl -g -o coverage_master.xml https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/hyrise/job/mrks%2Fpycobertura/lastStableBuild/artifact/coverage.xml
    pycobertura diff coverage_master.xml coverage.xml --format html --output coverage_diff.html

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
