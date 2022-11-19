#!/bin/bash

#SBATCH -A epic
#SBATCH --partition magic
#SBATCH --constraint "CPU_MNF:AMD"
#SBATCH --cpus-per-task 256
#SBATCH --container-name "2204"
#SBATCH --container-writable
#SBATCH --container-remap-root
#SBATCH --container-writable
#SBATCH --container-mounts /hpi/fs00/home/martin.boissier/enroot_mount:/enroot_mount,/hpi/fs00/home/martin.boissier/fg-epic:/fg-epic,/scratch:/scratch
#SBATCH --time=24:0:0
#SBATCH --mem=0
#SBATCH --exclusive

echo "Executing on the machine:" $(hostname)


cd /hyrise

build_dir="deb"
scale_factor=1
runtime=120

build_dir="rel_master"
scale_factor=10
runtime=1800

for FIRST in 1 2 4 8 16
do
    for SECOND in 1 2 4 8 16
    do
        echo $(date) ": benchmarking ${FIRST} anad ${SECOND}."

        sed "s/FIRST/${FIRST}/g" src/lib/scheduler/node_queue_scheduler.cpp2 > src/lib/scheduler/node_queue_scheduler.cpp
        sed -i "s/SECOND/${SECOND}/g" src/lib/scheduler/node_queue_scheduler.cpp

        ninja -C ${build_dir} hyriseBenchmarkTPCH > /dev/null

        numactl -m 0 -N 0 ./${build_dir}/hyriseBenchmarkTPCH --scheduler --clients 48 --cores 64 -s ${scale_factor} --mode=Shuffled --time ${runtime} -o node_scheduler__shuffled__${FIRST}_${SECOND}_30mins.json
        ./${build_dir}/hyriseBenchmarkTPCH --scheduler -s ${scale_factor} -o node_scheduler__bwcase128__${FIRST}_${SECOND}.json
    done
done

