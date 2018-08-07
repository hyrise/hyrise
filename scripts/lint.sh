#!/bin/bash

exitcode=0

find src \( -iname "*.cpp" -o -iname "*.hpp" \) -a -not -path "src/lib/operators/jit_operator/specialization/llvm/*.cpp" -print0 | parallel --null --no-notice -j 100% --nice 17 python2.7 ./scripts/cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11,-build/include_what_you_use,-readability/nolint --linelength=120 {} 2\>\&1 \| grep -v \'\^Done processing\' \| grep -v \'\^Total errors found: 0\' \; test \${PIPESTATUS[0]} -eq 0
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
grep -rn 'DISABLED_' src/test | grep -v '#[0-9]\+' | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Disabled tests should be documented with their issue number (e.g. \/* #123 *\/)/'
let "exitcode |= $?"

# Check for included cpp files. You would think that this is not necessary, but history proves you wrong.
regex='#include .*\.cpp'
namecheck=$(find src \( -iname "*.cpp" -o -iname "*.hpp" \) -print0 | xargs -0 grep -rn "$regex" | grep -v NOLINT)

let "exitcode |= ! $?"
while IFS= read -r line
do
	echo $line | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/Include of cpp file:/' | tr '\n' ' '
	echo $line | sed 's/\(:[^:]*:\)/\1 /'
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

exit $exitcode
