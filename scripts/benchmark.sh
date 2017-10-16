#!/bin/sh

if [ -z "$1" ]
  then
    echo "No build directory supplied"
    exit 1
fi

./$1/hyriseBenchmark --benchmark_format=json > benchmark.json