#!/bin/bash

GB=$((1024*1024*1024))

# Fail on first error.
set -e

if [[ "$USER" != "root" ]];
then
	echo "Please run program as superuser!"
	exit 1
fi

# this order is needed to be able to pass parameters to the executable along
# if less than two parameters are given, print usage
if [ $# -lt 2 ]; then
  echo "Usage: $0 <memory limit in GB> <path to executable> "
  exit 1
fi

# get the path to the executable and memory limit from the command line
memory_limit=$1
shift 1
executable="$@"

# convert the memory limit to giga bytes
memory_limit=$((memory_limit*GB))

# create a cgroup
CGROUP_NAME="memory-limit"

sudo cgcreate -g memory:$CGROUP_NAME

# Get some infos about CGroup
# sudo cgget -g cpu:$CGROUP_NAME
# sudo cgget -g memory:$CGROUP_NAME

#get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Set memory.max
sudo cgset -r memory.max=$memory_limit $CGROUP_NAME

# Get the result of setting operation.
sudo cgget -r memory.max $CGROUP_NAME

# Execute within cgroup with limited memory.
sudo cgexec -g memory:$CGROUP_NAME sudo $executable
echo "Finished execution of $executable with memory limit of $memory_limit bytes"
