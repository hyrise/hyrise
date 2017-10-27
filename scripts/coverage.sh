#!/bin/bash

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

./$1/hyriseCoverage && rm -fr coverage; mkdir coverage && gcovr -s -p -r . --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches --html --html-details -o coverage/index.html > coverage_output.txt

# without coverage badge generation
if [ -z "$2" ]
  then
    cat coverage_output.txt
else
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
