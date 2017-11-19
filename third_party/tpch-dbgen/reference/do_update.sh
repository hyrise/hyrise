#!/bin/bash
# run the update scripts serially
tmp=`mktemp`
if [ -z $1 ];then
        scalefactors="1 100 300 1000 3000 10000 30000 100000"
else
        scalefactors=$1
fi
echo Generating update data for scale factors $scalefactors

for sf in $scalefactors;do
	c=cmd_update_sf$sf
	echo STARTING $c at `date`
	. $c
	mkdir -p $sf
	mv *.u* $sf
	echo FINISHED $c at `date`
done
	
  
