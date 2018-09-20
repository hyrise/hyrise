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

start_commit=$1
end_commit=$2
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

for commit in $commit_list
do
	if [ -f auto_${commit}.json ]; then
		continue
	fi

	echo =======================================================
	git checkout $commit

	cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
	make $benchmark -j $((cores / 2)) > /dev/null

	./$benchmark $benchmark_arguments -o auto_${commit}.json
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

rm auto_*.json