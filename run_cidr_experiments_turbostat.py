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
#@parser.add_argument('--per_socket', action=argparse.BooleanOptionalAction, required=True)
parser.add_argument('--per_socket', action='store_true')
parser.add_argument('--no-per_socket', dest='per_socket', action='store_false')
parser.set_defaults(per_socket=True)
parser.add_argument('--oneapi_path', required=True)
args = parser.parse_args()

assert (Path(args.gcc_path) / "bin").exists(), "GCC path is not set properly."
assert (Path(args.oneapi_path) / "libtbb.so").exists(), "oneAPI path is not set properly."

assert Path("tpch_cached_tables").exists(), "Run from root."
# assert os.geteuid() == 0, "Must be run with sudo."

env_vars = {"LD_LIBRARY_PATH": f"{args.gcc_path}/lib64/:{args.oneapi_path}"}
export_command = f"export LD_LIBRARY_PATH={args.gcc_path}/lib64/:{args.oneapi_path};"

runtime_marker = "Total duration: "

perf_sockets = "--per-socket" if args.per_socket else ""


avg_idling_joules_per_second = 0.0

# Get accumulated for all sockets (Sapphire Rapids report millions of Watt when using per socket stats).
if not args.per_socket:
    print(" == Idle Measurements when no measurements per socket work")
    idle_runs = 5
    idle_duration = 60
    idle_duration = 2
    joules = []
    for idle_run in range(idle_runs):
        stop_command = f"perf stat {perf_sockets} sleep {idle_duration}"
        start = time.time()
        result = subprocess.run(shlex.split(stop_command), capture_output=True, text=True, check=True, env=env_vars)
        end = time.time()
        for line in result.stderr.splitlines():
            if "Joules" in line and "power/energy-pkg" in line:
                splitted = line.split()
                print(f"Idling for {idle_duration} s: {splitted[2]} Joules\t\t({line.strip()})")
                assert "Joules" in splitted[2]
                joules.append(float(splitted[3]))
 
    avg_idling_joules_per_second = sum(joules) / idle_duration / idle_runs
    print(f"Avg. Joules per second idling: {avg_idling_joules_per_second}")


scale_factor = 10
scale_factor = 1
data_loading_runs = 5
data_loading_joules = []
data_loading_runtimes = []
print(" == Data Loading")
for load_run in range(data_loading_runs):
    if scale_factor != 10:
        print("WWWWAARNING!! scale factor is ", scale_factor)
    load_command = f"sudo turbostat --quiet --Joules --show Pkg_J sh -c '{export_command} numactl -N 0 -m 0 ./rel_gcc13/hyriseBenchmarkTPCH --scheduler --clients 25 -s {scale_factor} -q 2 -r 1'"
    start = time.time()
    result = subprocess.run(shlex.split(load_command), capture_output=True, text=True, check=True, env=env_vars)
    end = time.time()
    assert "Pkg_J" in result.stderr, "No energy measurement found."
    lines_split_raw = result.stderr.splitlines()
    found = False
    lines_split = []
    for line_split in lines_split_raw:
        if line_split.endswith(" sec"):
            found = True

        if found:
            lines_split.append(line_split)
        
    assert "sec" in lines_split[0]
    data_loading_runtimes.append(float(lines_split[0].split()[0]))
    print(f"Joule results per package ({len(lines_split[3:])} results): {' '.join(lines_split[3:])}")
    joules_result = float(lines_split[3])
    print(f"Data Loading: {joules_result} Joules")
    data_loading_joules.append(joules_result)

    time.sleep(5)

avg_data_loading_joules = sum(data_loading_joules) / data_loading_runs
avg_data_loading_runtime = sum(data_loading_runtimes) / data_loading_runs
print(f"Avg. Joules data loading: {avg_data_loading_joules}")
print(f"Avg. runtime data loading: {avg_data_loading_runtime}")

time.sleep(5)
tpch_runtime = 1800
tpch_runtime = 30
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
    command = f"sudo turbostat --quiet --Joules --show Pkg_J sh -c '{export_command} numactl -m 0 -N 0  ./rel_gcc13/hyriseBenchmarkTPCH --scheduler --clients 25 -s {scale_factor} --time {tpch_runtime} --mode=Shuffled -o {json_output_file}'"
    start = time.time()
    result = subprocess.run(shlex.split(command), capture_output=True, text=True, check=True, env=env_vars)
    end = time.time()
    assert "Pkg_J" in result.stderr, "No energy measurement found."
    lines_split_raw = result.stderr.splitlines()
    found = False
    lines_split = []
    for line_split in lines_split_raw:
        if line_split.endswith(" sec"):
            found = True

        if found:
            lines_split.append(line_split)
        
    assert "sec" in lines_split[0]
    tpch_runtimes.append(float(lines_split[0].split()[0]))
    print(f"Joule results per package ({len(lines_split[3:])} results): {' '.join(lines_split[3:])}")
    joules_result = float(lines_split[3])
    print(f"TPC-H: {joules_result} Joules")
    tpch_joules.append(joules_result)

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
    idling_socket_joules_data_loading = avg_data_loading_runtime * avg_idling_joules_per_second
    idling_socket_joules_tpch = avg_tpch_runtime * avg_idling_joules_per_second
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
