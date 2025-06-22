#!/usr/bin/env python3

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from itertools import product

class config:
    scale_factors = [100]
    # scale_factors = [0.1]
    times = [1200]
    # times = [1]
    numactl_memory_nodes = [[0,1]]
    numactl_cpu_nodes = [[1]]
    clients = [10]
    cores = [0] # 0: all available cores
    shuffled = [True]
    # default: 0 (all cores)
    cores_data_prep = [0]
    # sockets: 0,1, CXL blades: 2,3,4,5
    rmem_nodes_weights = [
        ([1],[]), # local CPU
        # ([0],[]), # remote CPU
        ([2],[]), # CXL 1 blade
        ([2,3],[]),       # CXL 2 blades
        ([2,3,4],[]),     # CXL 3 blades
        ([2,3,4,5],[]),   # CXL 4 blades
        # ([0,2],[]),       # local + 1 CXL blade
        # ([0,2,3],[]),     # local + 2 CXL blade
        # ([0,2,3,4],[]),   # local + 3 CXL blade
        # ([0,2,3,4,5],[]), # local + 4 CXL blade
        ([1,2],[1,40]),   # weighted, local + 1 CXL blade
        ([1,2],[1,20]),   # weighted, local + 1 CXL blade
        # ([1,2],[1,16]),   # weighted, local + 1 CXL blade
        # ([1,2],[1,13]),   # weighted, local + 1 CXL blade
        ([1,2],[1,10]),   # weighted, local + 1 CXL blade
        ([1,2],[1,9]),    # weighted, local + 1 CXL blade
        ([1,2],[1,8]),    # weighted, local + 1 CXL blade
        ([1,2],[1,7]),    # weighted, local + 1 CXL blade
        ([1,2],[1,6]),    # weighted, local + 1 CXL blade
        ([1,2],[1,5]),    # weighted, local + 1 CXL blade
        ([1,2],[1,4]),    # weighted, local + 1 CXL blade
        ([1,2],[1,3]),    # weighted, local + 1 CXL blade
        ([1,2],[1,2]),    # weighted, local + 1 CXL blade
        ([1,2],[1,1]),    # weighted, local + 1 CXL blade
        ([1,2],[2,1]),    # weighted, local + 1 CXL blade
        ([1,2],[3,1]),    # weighted, local + 1 CXL blade
        ([1,2],[4,1]),    # weighted, local + 1 CXL blade
        ([1,2],[5,1]),    # weighted, local + 1 CXL blade
        ([1,2],[6,1]),    # weighted, local + 1 CXL blade
        ([1,2],[7,1]),    # weighted, local + 1 CXL blade
        ([1,2],[8,1]),    # weighted, local + 1 CXL blade
        ([1,2],[9,1]),    # weighted, local + 1 CXL blade
        ([1,2],[10,1]),   # weighted, local + 1 CXL blade
        # ([1,2],[13,1]),   # weighted, local + 1 CXL blade
        # ([1,2],[16,1]),   # weighted, local + 1 CXL blade
        ([1,2],[20,1]),   # weighted, local + 1 CXL blade
        ([1,2],[40,1]),   # weighted, local + 1 CXL blade
    ]
    # rmem_nodes_weights = [([],[])]

root_dir = (Path(__file__).resolve().parent / "..").resolve()
result_dir = os.path.join(root_dir, "results")
build_dir = os.path.join(root_dir, "build-exp-page-placement")

os.makedirs(build_dir, exist_ok=True)

build_command = f"cmake -S {root_dir} -B {build_dir} -DCMAKE_C_COMPILER=clang-17 -DCMAKE_CXX_COMPILER=clang++-17 -DCMAKE_BUILD_TYPE=Release -DHYRISE_RELAXED_BUILD=On"
build = subprocess.Popen(build_command, shell=True, stdout=None, stderr=None)
build.wait()

make_command = f"make -C {build_dir} -j hyrisePlacementPlugin hyriseBenchmarkTPCH"
make = subprocess.Popen(make_command, shell=True, stdout=None, stderr=None)
make.wait()

# build_command = f"cmake .. -DCMAKE_C_COMPILER=clang-17 -DCMAKE_CXX_COMPILER=clang++-17 -DCMAKE_BUILD_TYPE=Release -DHYRISE_RELAXED_BUILD=On"
# cmake .. -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 -DCMAKE_BUILD_TYPE=Release -DHYRISE_RELAXED_BUILD=On
# make -j hyrisePlacementPlugin hyriseBenchmarkTPCH

print("Binary path:", build_dir)

bench_configs = list(product(
    config.numactl_cpu_nodes,
    config.numactl_memory_nodes,
    config.scale_factors,
    config.times,
    config.clients,
    config.cores,
    config.shuffled,
    config.rmem_nodes_weights,
    config.cores_data_prep)
)

for (numactl_cpu, numactl_mem, s, t, clients, cores, shuffled, (rmem_nodes, rmem_weights), cores_data_prep) in bench_configs:
    numactl_cpu_str = ",".join([str(node) for node in numactl_cpu])
    numactl_mem_str = ",".join([str(node) for node in numactl_mem])
    # do not allow using binary tables: --dont_cache_binary_tables
    command = f"numactl -N {numactl_cpu_str} -m {numactl_mem_str} {build_dir}/hyriseBenchmarkTPCH --plugins {build_dir}/lib/libhyrisePlacementPlugin.so --dont_cache_binary_tables --data_preparation_cores {cores_data_prep}"
    command += f" -s {s}"
    command += f" -t {t}"
    command += f" --clients {clients}"
    if clients > 0:
        command += " --scheduler"
    command += f" --cores {cores}"
    mode = "Ordered"
    if shuffled:
        mode = "Shuffled"
    command += f" --mode {mode}"
    rmem_node_str = ""
    if rmem_nodes:
        rmem_node_str = ",".join([str(node) for node in rmem_nodes])
        command += f" --rmem_ids {rmem_node_str}"
    rmem_weight_str = ""
    if rmem_weights:
        rmem_weight_str = ",".join([str(weight) for weight in rmem_weights])
        command += f" --rmem_weights {rmem_weight_str}"
    date = str(datetime.today().strftime("%Y-%m-%dT%H%M%S"))
    bench_id = f"N{numactl_cpu_str.replace(',','-')}m{numactl_mem_str.replace(',','-')}s{s}t{t}cl{clients}c{cores}{mode}_rn{rmem_node_str.replace(',','-')}rw{rmem_weight_str.replace(',','-')}"
    command += f" -o {result_dir}/{date}_{bench_id}.json"
    log_file = f"{result_dir}/{date}_{bench_id}.log"
    command = f"{command} | tee {log_file}"
    print(command)

    subprocess.run(command, shell=True)
