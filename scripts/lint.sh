#!/bin/bash

exitcode=0

find src \( -iname "*.cpp" -o -iname "*.hpp" \) -a -not -path "src/lib/operators/jit_operator/specialization/llvm/*.cpp" -a -not -path "src/lib/utils/uninitialized_vector.hpp" -print0 | parallel --null --no-notice -j 100% --nice 17 python2.7 ./scripts/cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11,-build/include_what_you_use,-readability/nolint --linelength=120 {} 2\>\&1 \| grep -v \'\^Done processing\' \| grep -v \'\^Total errors found: 0\' \; test \${PIPESTATUS[0]} -eq 0
let "exitcode |= $?"
#                             /------------------ runs in parallel -------------------\
# Conceptual: find | parallel python cpplint \| grep -v \| test \${PIPESTATUS[0]} -eq 0
#             ^      ^        ^                 ^          ^
#             |      |        |                 |          |
#             |      |        |                 |          Get the return code of the first pipeline item (here: cpplint)
#             |      |        |                 Removes the prints for files without errors
#             |      |        Regular call of cpplint with options
#             |      Runs the following in parallel
#             Finds all .cpp and .hpp files, separated by \0

# All disabled tests should have an issue number
output=$(grep -rn 'DISABLED_' src/test | grep -v '#[0-9]\+' | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Disabled tests should be documented with their issue number (e.g. \/* #123 *\/)/')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# The singleton pattern should not be manually implemented
output=$(grep -rn 'static[^:]*instance;' --exclude singleton.hpp src | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Singletons should not be implemented manually. Have a look at src\/lib\/utils\/singleton.hpp/')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# Check for included cpp files. You would think that this is not necessary, but history proves you wrong.
regex='#include .*\.cpp'
namecheck=$(find src \( -iname "*.cpp" -o -iname "*.hpp" \) -print0 | xargs -0 grep -rn "$regex" | grep -v NOLINT)

let "exitcode |= ! $?"
while IFS= read -r line
do
	if [ ! -z "$line" ]; then
		echo $line | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/Include of cpp file:/' | tr '\n' ' '
		echo $line | sed 's/\(:[^:]*:\)/\1 /'
	fi
done <<< "$namecheck"

for dir in src/*
do
	for file in $(find $dir -name *.cpp -o -name *.hpp)
	do
		if grep $(basename $file) $dir/CMakeLists.txt | grep -v '#' > /dev/null
		then
			continue
		else
			echo $file not found in $dir/CMakeLists.txt
			exitcode=1
		fi
	done
done

# Check if all probes are defined in provider.d
for probe in $(grep -r --include=*.[ch]pp --exclude=probes.hpp --exclude=provider.hpp -h '^\s*DTRACE_PROBE' src | sed -E 's/^ *DTRACE_PROBE[0-9]{0,2}\(HYRISE, *([A-Z_]+).*$/\1/'); do
    grep -i $probe src/lib/utils/tracing/provider.d > /dev/null
    if [ $? -ne 0 ]; then
        echo "Probe $probe is not defined in provider.d"
        exitcode=1
    fi
done

exit $exitcode
