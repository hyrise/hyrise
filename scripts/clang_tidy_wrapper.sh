#!/bin/bash

# We need this wrapper because clang-tidy doesn't come with an option to ignore entire files

cmake_source_dir=$1/
shift  # Remove the source dir from $@
file=$1
shift  # Remove the file from $@

file_relative_to_source=${file//$cmake_source_dir/}

if grep "$file_relative_to_source" $cmake_source_dir/.clang-tidy-ignore > /dev/null; then
	echo "clang-tidy: Ignoring $file_relative_to_source"
	exit 0
else
	exec $(which clang-tidy-7.0 clang-tidy-6.0 clang-tidy | head -n 1) -header-filter=$1/src/ $file $@
fi
