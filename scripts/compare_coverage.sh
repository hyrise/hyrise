#!/bin/bash

master_branch=$(curl https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/hyrise/job/master/lastStableBuild/artifact/coverage_percent.txt)
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