#!/bin/bash

if [[ "$USER" != "root" ]];
then
	echo "Please run program as superuser!"
	exit 1
fi

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
echo $SCRIPT_DIR
cd "$SCRIPT_DIR/../../cmake-build-debug"

sudo cgexec -g memory:$CGROUP_NAME sudo ./hyriseMicroIOReadBenchmark > output.log