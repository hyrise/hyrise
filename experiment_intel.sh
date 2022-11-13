#!/bin/bash

#SBATCH -A epic
#SBATCH --partition magic
#SBATCH --constraint "CPU_SKU:5220S"
#SBATCH --cpus-per-task 72
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

build_dir="rel_intel"
scale_factor=10
runtime=1800

for MIN_SLEEPTIME in 50 100 200 400
do
    for MAX_SLEEPTIME in 1000 4000 8000
    do
        for LOOP_TIME in 1024 2048 4096
        do
            echo $(date) ": benchmarking ${MIN_SLEEPTIME} to ${MAX_SLEEPTIME} with loop time of ${LOOP_TIME}."

            sed "s/NARF_MIN_SLEEPTIME/${MIN_SLEEPTIME}/g" src/lib/scheduler/worker.hpp2 > src/lib/scheduler/worker.hpp
            sed -i "s/NARF_MAX_SLEEPTIME/${MAX_SLEEPTIME}/g" src/lib/scheduler/worker.hpp

            sed "s/NARF_LOOP_TIME/${LOOP_TIME}/g" src/lib/scheduler/worker.cpp2 > src/lib/scheduler/worker.cpp

	    ninja -C ${build_dir} hyriseBenchmarkTPCH > /dev/null

            numactl --preferred 0 -N 0,1 ./${build_dir}/hyriseBenchmarkTPCH --scheduler --clients 27 --cores 36 -s ${scale_factor} --mode=Shuffled --time ${runtime} -o sleeptimeanalysis_${MIN_SLEEPTIME}_${MAX_SLEEPTIME}_${LOOP_TIME}_intel.json
        done
    done
done
