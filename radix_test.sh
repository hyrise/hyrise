#!/usr/bin/env bash

for mode in ' ' ' --scheduler --clients 20 --cores 20' ;
do
    for cache in 0.5 0.75 1.0 1.25 1.5 1.75 2.0 3.0 4.0 6.0 8.0
       do
       for semi in 0.025 0.05 0.1 0.15 0.2 0.3 1.0
       do
           for dir in 'rel_clang' 'rel' ;
	   do
               echo "  -> cache of $cache and semi of $semi (${dir} & ${mode})"
	       echo "  -> DATE" `date`
               CACHE_USAGE=$cache SEMI_ADAPTION_FACTOR=$semi numactl -m 3 -N 3 ./$dir/hyriseBenchmarkTPCH -s 10 -q 4,10,17 $mode
	   done
       done
    done
done
