#!/bin/bash

unamestr=$(uname)
if [[ "$unamestr" == 'Darwin' ]]; then
	clang_format="/usr/local/opt/llvm/bin/clang-format"
	$clang_format --version | grep "version 6.0" >/dev/null
	if [ $? -ne 0 ]; then
		echo "incompatible clang-format version detected in $clang_format"
	fi
	format_cmd="$clang_format -i -style=file '{}'"
elif [[ "$unamestr" == 'Linux' ]]; then
	format_cmd="clang-format-6.0 -i -style=file '{}'"
fi


if [ "${1}" = "all" ]; then
    find src -iname "*.cpp" -o -iname "*.hpp" -o -iname "*.ipp" | xargs -I{} sh -c "${format_cmd}"
elif [ "$1" = "modified" ]; then
    # Run on all changed as well as untracked cpp/hpp files, as compared to the current HEAD. Skip deleted files.
    { git diff --diff-filter=d --name-only & git ls-files --others --exclude-standard; } | grep -E "^src.*\.[chi]pp$" | xargs -I{} sh -c "${format_cmd}"
elif [ "$1" = "staged" ]; then
    # Run on all files that are staged to be committed.
    git diff --diff-filter=d --cached --name-only | grep -E "^src.*\.[chi]pp$" | xargs -I{} sh -c "${format_cmd}"
else
    # Run on all changed as well as untracked cpp/hpp files, as compared to the current master. Skip deleted files.
    { git diff --diff-filter=d --name-only master & git ls-files --others --exclude-standard; } | grep -E "^src.*\.[chi]pp$" | xargs -I{} sh -c "${format_cmd}"
fi
