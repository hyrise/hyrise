#!/bin/bash

output=`git rev-parse --show-toplevel`/compile_times.txt

if [ $# -eq 0 ]; then
    absolute_path=$([[ $0 = /* ]] && echo "$0" || echo "$PWD/${0#./}")

	echo "This script helps you in getting a list of the compile times for every file."
	echo "Usage:"
	echo "  0. Clean up previous results:"
	echo "    rm $output"
	echo "  1. Set cmake to use timing wrapper:"
	echo "    cmake -DCMAKE_CXX_COMPILER_LAUNCHER=\"$absolute_path\" .."
	echo "    You can use other options, like setting the compiler or build mode as usual."
	echo "  2. Build the project:"
	echo "    make"
	echo "    Running this in parallel (using make -j) makes results less accurate"
	echo "  3. Get the results"
	echo "    sort -nr $output"
	exit
fi

start=`date +%s`
eval "$@"
end=`date +%s`
runtime=$(($end-$start))
echo "$runtime ${@: -1}" >> $output