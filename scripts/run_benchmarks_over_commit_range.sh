#!/bin/bash

set -e

if [ $# -lt 3 ]
then
	echo Runs a benchmark for every commit in a given range
	echo Call the script from your build directory \(i.e., ../scripts/$0\)
	echo Arguments: first_commit_id last_commit_id binary [benchmark_arguments]
	echo binary is, for example, hyriseBenchmarkTPCH
	exit 1
fi

start_commit=$(git rev-parse $1 | head -n 1)
end_commit=$(git rev-parse $2 | head -n 1)
benchmark=$3
shift; shift; shift
benchmark_arguments=$@

if [[ $(git status --untracked-files=no --porcelain) ]]
then
	echo Cowardly refusing to execute on a dirty workspace
	exit 1
fi

commit_list=$(git rev-list --ancestry-path ${start_commit}^..${end_commit})

[[ -z "$commit_list" ]] && echo No connection between these commits found && exit

commit_list=$(echo $commit_list | awk '{for (i=NF; i>1; i--) printf("%s ",$i); printf("%s\n",$1)}')  ## revert list

# Check whether to use ninja or make
output=$(grep 'CMAKE_MAKE_PROGRAM' CMakeCache.txt | grep ninja || true)
if [ ! -z "$output" ]
then
	build_system='ninja'
else
	build_system='make'
fi

for commit in $commit_list
do
	if [ -f auto_${commit}.json ]; then
		echo "auto_${commit}.json already exists, skipping"
	else
		echo =======================================================
		git checkout $commit

		cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
		${build_system} $benchmark -j $((cores / 2)) > /dev/null

		./$benchmark $benchmark_arguments -o auto_${commit}.json
	fi

	output=$(grep ${commit} auto_${commit}.json || true)
	if [ -z "$output" ]; then
		echo "Commit Hash ${commit} not found in auto_${commit}.json. It looks like the build failed."
		exit 1
	fi
done

for commit in $commit_list
do
	[[ -z "$previous_commit" ]] && previous_commit=$commit && continue
	echo ========== $commit ========== 
	git log -1 --pretty=%B $commit | head -n 1
	../scripts/compare_benchmarks.py auto_${previous_commit}.json auto_${commit}.json

	previous_commit=$commit
done

echo ===============================================================
echo                            SUMMARY
echo ===============================================================
echo diff from ${start_commit} to ${end_commit}
echo ${start_commit}: $(git log -1 --pretty=%B ${start_commit} | head -n 1)
echo ${end_commit}: $(git log -1 --pretty=%B ${end_commit} | head -n 1)

../scripts/compare_benchmarks.py auto_${start_commit}.json auto_${end_commit}.json
