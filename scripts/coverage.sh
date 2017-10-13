#!/bin/sh

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

# without coverage badge generation
if [ -z "$2" ]
  then
    ./$1/opossumCoverage && rm -fr coverage; mkdir coverage && gcovr -s -r . --exclude="(.*types*.|.*test*.|.*\.pb\.|third_party)" --html --html-details -o coverage/index.html
else
    coverage_percent=$(./$1/opossumCoverage && rm -fr coverage; mkdir coverage && gcovr -s -r . --exclude="(.*types*.|.*test*.|.*\.pb\.|third_party)" --html --html-details -o coverage/index.html | grep lines: | sed -e 's/lines: //; s/% .*$//')
    echo $coverage_percent
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

    curl -o coverage_badge.svg https://img.shields.io/badge/Coverage-"${coverage_percent}"-"${color}".svg
fi
