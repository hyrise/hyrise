#!/bin/sh

format_cmd="clang-format -i -style=file '{}'"

if [ -z "${1}" ]; then
    # Run on all changed as well as untracked cpp/hpp files, as compared to the current master. Skip deleted files.
    { git diff --diff-filter=d --name-only master & git ls-files --others --exclude-standard; } | grep -E "\.[ch]pp$" | xargs -I{} sh -c "${format_cmd}"
else
    find src -iname "*.cpp" -o -iname "*.hpp" | xargs -I{} sh -c "${format_cmd}"
fi
