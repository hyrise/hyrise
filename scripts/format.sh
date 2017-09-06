#!/bin/bash

format_cmd="clang-format -i -style=file '{}'"

if [ "${1}" = "all" ]; then
    find src -iname "*.cpp" -o -iname "*.hpp" | xargs -I{} sh -c "${format_cmd}"
elif [ "$1" = "modified" ]; then
    # Run on all changed as well as untracked cpp/hpp files, as compared to the current HEAD. Skip deleted files.
    { git diff --diff-filter=d --name-only & git ls-files --others --exclude-standard; } | grep -E "^src.*\.[ch]pp$" | xargs -I{} sh -c "${format_cmd}"
else
    # Run on all changed as well as untracked cpp/hpp files, as compared to the current master. Skip deleted files.
    { git diff --diff-filter=d --name-only master & git ls-files --others --exclude-standard; } | grep -E "^src.*\.[ch]pp$" | xargs -I{} sh -c "${format_cmd}"
fi
