#!/bin/bash

find src \( -iname "*.cpp" -o -iname "*.hpp" \) -print0 | parallel --null --no-notice -j 100% --nice 17 python2.7 ./scripts/cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11,-build/include_what_you_use --linelength=120 {} 2\>\&1 \| grep -v \'\^Done processing\' \| grep -v \'\^Total errors found: 0\' \; test \${PIPESTATUS[0]} -eq 0

#                             /------------------ runs in parallel -------------------\
# Conceptual: find | parallel python cpplint \| grep -v \| test \${PIPESTATUS[0]} -eq 0
#             ^      ^        ^                 ^          ^
#             |      |        |                 |          |
#             |      |        |                 |          Get the return code of the first pipeline item (here: cpplint)
#             |      |        |                 Removes the prints for files without errors
#             |      |        Regular call of cpplint with options
#             |      Runs the following in parallel
#             Finds all .cpp and .hpp files, separated by \0

# grep -rn 'DISABLED_' src/test | grep -v '#[0-9]\+' | sed 's/\(:[0-9]+\):.*/  Disabled tests should be documented with their issue number (e.g. \/* #123 *\/)/'