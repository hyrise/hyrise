#!/bin/ksh

# alter the scale factor as desired, testing with sf=1 might be a good idea!
# it is assumed to be run from the dbgen directory and will create
#	flat_files_sf<scale>  into which it will put the flat files for DOP=1
#	flat_files_sf<scale/DOP<n>	for the files for DOP<n>


sf=1
tables="lineitem orders part partsupp supplier customer"

export DSS_CONFIG=`pwd`
mkdir -p flat_files_sf$sf
cd flat_files_sf$sf
../dbgen -s $sf -f >/dev/null 2>&1

for dop in 8 13 33;do
  mkdir -p DOP$dop
  cd DOP$dop
  echo 'Starting test for DOP =' $dop at `date`
  rm -f *.tbl.*
  for ((d=1; d<=dop; d++));do
    ../../dbgen -s $sf -S $d -C $dop -f >/dev/null 2>&1 &
  done
  wait
  rm -f *.conc
  for ((d=1; d<=dop; d++));do
    for table in $tables;do
      cat ${table}.tbl.${d}  >> ${table}.conc &
    done
    wait
  done
  for table in $tables;do
    x=`cksum ../$table.tbl |awk '{print $1 $2}'`
    y=`cksum $table.conc |awk '{print $1 $2}'`
    if [ "$x" = "$y" ];then
       echo table $table matches
       rm $table.conc $table.tbl.*
    else
       echo failure for table $table
    fi
  done
  cd ..
done

