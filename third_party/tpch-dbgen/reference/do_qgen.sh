#!/bin/bash
# run all the cmd_qgen scripts serially

tmp=`mktemp`
cp ../queries/*.sql .
if [ -z $1 ];then
        scalefactors="1 100 300 1000 3000 10000 30000 100000"
else
        scalefactors=$1
fi
echo Generating substitution params for scale factors $scalefactors

for sf in $scalefactors;do
	c=cmd_qgen_sf$sf
	echo STARTING $c at `date`
	. $c >/dev/null 2>&1
	mkdir -p $sf
	mv subparam_* $sf
	echo FINISHED $c at `date`
done
rm *.sql
