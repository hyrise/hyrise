#!/bin/bash

# If you call this script without parameters, it performs all checks. The CI does it in two steps so that a failure to provide an issue number for a disabled test does not stop the CI build.

exitcode=0

find src \( -iname "*.cpp" -o -iname "*.hpp" \) -a -not -path "src/lib/operators/jit_operator/specialization/llvm/*.cpp" -print0 | parallel --null --no-notice -j 100% --nice 17 python2.7 ./scripts/cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11,-build/include_what_you_use --linelength=120 {} 2\>\&1 \| grep -v \'\^Done processing\' \| grep -v \'\^Total errors found: 0\' \; test \${PIPESTATUS[0]} -eq 0
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

# Check for variable names in camel case. Exceptions may have to be added here for calls to external libraries.
regex=' [a-z]\+\([A-Z][a-z]\+\)\+'
namecheck=$(find src \( -iname "*.cpp" -o -iname "*.hpp" \) -a -not -path "src/lib/operators/jit_operator/specialization/llvm/*.cpp" -print0 | xargs -0 grep -rn "$regex" | grep -v '\w*\*' | grep -v '\w*//' | grep -v NOLINT | grep -v sql)

let "exitcode |= ! $?"
while IFS= read -r line
do
	echo $line | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Possible violation of naming convention:/' | tr '\n' ' '
	echo $line | sed 's/^[^ ]*//' | grep -o "$regex" | head -n1
done <<< "$namecheck"

exit $exitcode
