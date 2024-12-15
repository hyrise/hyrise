#!/usr/bin/env python

import argparse
import json
import multiprocessing
import os
import pandas as pd
import shlex
import socket
import subprocess
import time

from pathlib import Path

hostname = socket.gethostname()

parser = argparse.ArgumentParser()
parser.add_argument('--gcc_path', required=True)
#parser.add_argument('--per_socket', action=argparse.BooleanOptionalAction, required=True)
parser.add_argument('--per_socket', action='store_true')
parser.add_argument('--no-per_socket', dest='per_socket', action='store_false')
parser.set_defaults(per_socket=True)
parser.add_argument('--oneapi_path', required=True)
parser.add_argument('perf_metrics', nargs='+', help='Perf metric(s) to track', default=["power/energy-pkg/"])
args = parser.parse_args()

assert (Path(args.gcc_path) / "bin").exists(), "GCC path is not set properly."
assert (Path(args.oneapi_path) / "libtbb.so").exists(), "oneAPI path is not set properly."

assert Path("tpch_cached_tables").exists(), "Run from root."
# assert os.geteuid() == 0, "Must be run with sudo."

env_vars = {"LD_LIBRARY_PATH": f"{args.gcc_path}/lib64/:{args.oneapi_path}"}

runtime_marker = "Total duration: "

perf_metrics = " ".join([f"-e {metric}" for metric in args.perf_metrics])
perf_sockets = "--per-socket" if args.per_socket else ""


avg_idling_joules_per_second = 0.0

# Get accumulated for all sockets (Sapphire Rapids report millions of Watt when using per socket stats).
if not args.per_socket:
    print(" == Idle Measurements when no measurements per socket work")
    idle_runs = 5
    idle_duration = 60
    #idle_duration = 2
    joules = []
    for idle_run in range(idle_runs):
        if idle_duration < 60:
            print(f"WWWWAARNING!! idle duration is {idle_duration} s")
        stop_command = f"perf stat {perf_sockets} {perf_metrics} sleep {idle_duration}"
        start = time.time()
        result = subprocess.run(shlex.split(stop_command), capture_output=True, text=True, check=True, env=env_vars)
        end = time.time()
        for line in result.stderr.splitlines():
            if "Joules" in line and "power/energy-pkg" in line:
                splitted = line.split()
                print(f"Idling for {idle_duration} s: {splitted[0]} Joules\t\t({line.strip()})")
                assert "Joules" in splitted[1]
                joules.append(float(splitted[0]))
 
    avg_idling_joules_per_second = sum(joules) / idle_duration / idle_runs
    print(f"Avg. Joules per second idling: {avg_idling_joules_per_second}")


scale_factor = 10
#scale_factor = 1
data_loading_runs = 5
data_loading_joules = []
data_loading_runtimes = []
print(" == Data Loading")
for load_run in range(data_loading_runs):
    if scale_factor != 10:
        print(f"WWWWAARNING!! scale factor is {scale_factor}")
    load_command = f"perf stat {perf_sockets} {perf_metrics} numactl -N 0 -m 0 ./rel_gcc13/hyriseBenchmarkTPCH --scheduler --clients 25 -s {scale_factor} -q 2 -r 1"
    start = time.time()
    result = subprocess.run(shlex.split(load_command), capture_output=True, text=True, check=True, env=env_vars)
    end = time.time()
    assert "Joules" in result.stderr, "No energy measurement found."
    for line in result.stderr.splitlines():
        if "Joules" in line and "power/energy-pkg" in line:
            if "S0" in line or not args.per_socket:
                #print(line)
                line_split = line.split()
                joules_result = float(line_split[2 - 2*int(not args.per_socket)])
                print(f"Data Loading: {joules_result} Joules\t\t({line.strip()})")
                assert "Joules" in line_split[3 - 2*int(not args.per_socket)]
                data_loading_joules.append(joules_result)

    for line in result.stderr.splitlines():
        if "seconds time elapsed" in line:
            #print(line)
            line_split = line.split()
            #print(line_split)
            data_loading_runtimes.append(float(line_split[0]))
            assert "seconds" in line_split[1]

    time.sleep(5)

avg_data_loading_joules = sum(data_loading_joules) / data_loading_runs
avg_data_loading_runtime = sum(data_loading_runtimes) / data_loading_runs
print(f"Avg. Joules data loading: {avg_data_loading_joules}")
print(f"Avg. runtime data loading: {avg_data_loading_runtime}")

time.sleep(5)
tpch_runtime = 1800
#tpch_runtime = 30
tpch_runs = 5
tpch_joules = []
tpch_runtimes = []
json_result_filenames = []
print(" == TPC-H Benchmarking")
for run in range(tpch_runs):
    if scale_factor != 10 or tpch_runtime != 1800:
        print(f"WWWWAARNING!! scale factor is {scale_factor} and runtime is {tpch_runtime}")
    json_output_file = f"tpch__{int(time.time())}.json"
    json_result_filenames.append(json_output_file)
    command = f"perf stat {perf_sockets} {perf_metrics} numactl -m 0 -N 0  ./rel_gcc13/hyriseBenchmarkTPCH --scheduler --clients 25 -s {scale_factor} --time {tpch_runtime} --mode=Shuffled -o {json_output_file}"
    start = time.time()
    result = subprocess.run(shlex.split(command), capture_output=True, text=True, check=True, env=env_vars)
    end = time.time()
    assert "Joules" in result.stderr, "No energy measurement found."
    assert Path(json_output_file).exists()
    for line in result.stderr.splitlines():
        if "Joules" in line and "power/energy-pkg" in line:
            if "S0" in line or not args.per_socket:
                #print(line)
                line_split = line.split()
                joules_result = float(line_split[2 - 2*int(not args.per_socket)])
                print(f"TPC-H: {joules_result} Joules\t\t({line.strip()})")
                assert "Joules" in line_split[3 - 2*int(not args.per_socket)]
                tpch_joules.append(joules_result)
            line_split = line.split

    for line in result.stderr.splitlines():
        if "seconds time elapsed" in line:
            line_split = line.split()
            tpch_runtimes.append(float(line_split[0]))
            assert "seconds" in line_split[1]

    time.sleep(5)

avg_tpch_joules = sum(tpch_joules) / tpch_runs
avg_tpch_runtime = sum(tpch_runtimes) / tpch_runs
print(f"Avg. Joules TPC-H: {avg_tpch_joules}")
print(f"Avg. runtime TPC-H: {avg_tpch_runtime}")

print(f" == Attempting to give final answers:")
if args.per_socket:
    print(f"Joules for TPC-H: {avg_tpch_joules - avg_data_loading_joules}")
else:
    print(f"Assuming two sockets.")
    assert avg_idling_joules_per_second > 0.0
    idling_socket_joules_data_loading = avg_data_loading_runtime * avg_idling_joules_per_second / 2
    idling_socket_joules_tpch = avg_tpch_runtime * avg_idling_joules_per_second / 2
    print(f"Joules for TPC-H: {(avg_tpch_joules - idling_socket_joules_tpch) - (avg_data_loading_joules - idling_socket_joules_data_loading)} (({avg_tpch_joules} - {idling_socket_joules_tpch}) - ({avg_data_loading_joules} - {idling_socket_joules_data_loading}))")

avg_runtimes = []
for json_output_filename in json_result_filenames:
    with open(json_output_filename) as file:
        json_data = json.load(file)
        overall_runtime = 0.0
        for benchmark in json_data["benchmarks"]:
            overall_runtime += float(sum([run["duration"] for run in benchmark["successful_runs"]])) / len(benchmark["successful_runs"]) / 1_000_000
        print(f"{json_output_filename}: {overall_runtime} ms")
        avg_runtimes.append(overall_runtime)

print(f"Avg. runtime: {sum(avg_runtimes) / len(avg_runtimes)} ms")
