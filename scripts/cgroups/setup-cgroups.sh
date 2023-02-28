#!/bin/bash

# Fail on first error.
set -e

if [[ "$USER" != "root" ]];
then
	echo "Please run program as superuser!"
	exit 1
fi

limit1=$((50*1024*1024*1024))
limit2=$((50*1024*1024*1024*10))
limit3=$((50*1024*1024*1024*100))

# This is in Bytes! Too low levels will fail since Hyrise can't run without memory!
memory_limits=($limit1 $limit2 $limit3)

####################################################################
#
# CGroups stuff.
# Please look at https://www.youtube.com/watch?v=z7mgaWqiV90
#
####################################################################

CGROUP_NAME="limitedPageCache"

sudo cgcreate -g memory:$CGROUP_NAME

# Get some infos about CGroup
# sudo cgget -g cpu:$CGROUP_NAME
# sudo cgget -g memory:$CGROUP_NAME

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR/../../cmake-build-debug"

for limit in ${memory_limits[@]}; do
	# Set memory.max
	sudo cgset -r memory.max=$limit $CGROUP_NAME
	# Get the result of setting operation.
	sudo cgget -r memory.max $CGROUP_NAME
	# Execute the benchmark.
	sudo cgexec -g memory:$CGROUP_NAME sudo ./hyriseBenchmarkTPCH > output.log
done