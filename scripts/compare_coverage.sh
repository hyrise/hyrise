#!/bin/bash

if ping -q -c 1 -W 1 imresx01.eaalab.hpi.uni-potsdam.de >/dev/null; then
	# Workaround as accessing the DMZ from the the IMR does not work
	master_branch=$(curl http://imresx01.eaalab.hpi.uni-potsdam.de:8080/job/Hyrise/job/hyrise/job/master/lastStableBuild/artifact/coverage_percent.txt)
else
	master_branch=$(curl https://hyrise-ci.epic-hpi.de/job/Hyrise/job/hyrise/job/master/lastStableBuild/artifact/coverage_percent.txt)
fi
this_branch=$(cat coverage_percent.txt)

if (( $(bc <<< "$master_branch < $this_branch") ))
then
echo -n increased by $(bc <<< "$this_branch - $master_branch")
elif (( $(bc <<< "$master_branch == $this_branch") ))
then
echo -n stayed the same
else
echo -n decreased by $(bc <<< "$master_branch - $this_branch")
fi

echo " compared to master and is now $this_branch%"