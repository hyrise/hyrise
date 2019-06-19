#!/usr/bin/env bash

if [[ $# -ne 1 ]]
then
	echo "Scale factor is required as parameter."
	exit 1
fi

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     machine=LINUX;;
    Darwin*)    machine=MACOS;;
    *)          machine=${unameOut}
esac

if [ ${machine} != "LINUX" ] && [ ${machine} != "MACOS" ] 
then
	echo "Unsupported machine: "${machine}
	exit 1
fi

cd third_party/tpcds-kit/tools/
echo "building tpc-ds tools"
make "OS="${machine}
./dsdgen -scale $1 -dir ../../../resources/benchmark/tpcds/tables -terminate n -verbose -f
cd ../../../resources/benchmark/tpcds/tables
for x in *.dat; do mv $x ${x%.dat}.csv; done
cd ../../../../
exit 0
