#!/bin/bash
# this makes all the cmd_base scripts in parallel for each scale factor, wating for each
# scale factor to be done before going on to the next

tmp=`mktemp`
if [ -z $1 ];then
	scalefactors="1 100 300 1000 3000 10000 30000 100000"
else
	scalefactors=$1
fi
echo Generating base table data for scale factors $scalefactors

for sf in $scalefactors;do
	c=cmd_base_sf$sf
	echo STARTING $c at `date`
	# just add a '&' at the end of each line
	sed <$c 's/$/ \&/' >$tmp
	. $tmp
	wait
	mkdir -p $sf
	mv *.tbl.* $sf
	echo FINISHED $c at `date`
done
	
  
