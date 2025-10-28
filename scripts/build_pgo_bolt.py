#!/usr/bin/env python
# This script allows

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
from subprocess import run
from shutil import copy, move
from os import cpu_count, getcwd, remove

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-t",  "--time", type=int, default=1800, help="The time to run each benchmark in seconds.")
parser.add_argument("-n", "--num-cores", type=int, default=cpu_count(), help="The number of cpu cores to use for compiling and running.")
parser.add_argument("-c", "--ci", action=BooleanOptionalAction, help="Whether this script is run in the ci (tries to safe time as much as possible)")
parser.add_argument("-e", "--export", action=BooleanOptionalAction, help="Export the profile to resources.")

build_folder = getcwd()
benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkTPCC", "hyriseBenchmarkJoinOrder", "hyriseBenchmarkStarSchema"]
ci_benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkTPCC", "hyriseBenchmarkStarSchema"]

def run_root(cmd):
  run(cmd, cwd=f"{build_folder}/..", shell=True)

def run_build(cmd):
  run(cmd, cwd=build_folder, shell=True)

def build_for_profiling(num_cores, benchmarks=benchmarks):
  run_build("ninja clean")
  run_build("cmake -DCOMPILE_FOR_BOLT=ON -DPGO_INSTRUMENT=ON -UPGO_OPTIMIZE ..")
  run_build(f"ninja {"".join(benchmarks)} -j {num_cores}")
  move("lib/libhyrise_impl.so", "lib/libhyrise_impl.so.old")
  run_build("llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so")

def build_with_optimization(num_cores, target=""):
  run_build("ninja clean")
  run_build("cmake -DCOMPILE_FOR_BOLT=ON -DPGO_INSTRUMENT=OFF -DPGO_OPTIMIZE=libhyrise.profdata ..")
  run_build(f"ninja {target} -j {num_cores}")
  move("lib/libhyrise_impl.so", "lib/libhyrise_impl.so.old")
  run_build("llvm-bolt lib/libhyrise_impl.so.old -o lib/libhyrise_impl.so -data bolt.fdata -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold -split-eh -dyno-stats")
  run_build("strip -R .rela.text -R \".rela.text.*\" -R .rela.data -R \".rela.data.*\" lib/libhyrise_impl.so")

def profile(num_cores, time, benchmarks=benchmarks):
  for benchmark in benchmarks:
    run_root(f"{build_folder}/{benchmark} --scheduler --clients {num_cores} --cores {num_cores} -t {time} -m Shuffled")
    move("/tmp/prof.fdata", f"{benchmark}.fdata")
  run_root("mv *.profraw libhyrise.profraw")

def profile_in_ci(num_cores, time):
  run_root(f"{build_folder}/hyriseBenchmarkTPCH --scheduler --clients {num_cores} --cores {num_cores} -t {time} -m Shuffled -s .01")
  move("/tmp/prof.fdata", f"hyriseBenchmarkTPCH.fdata")
  run_root(f"{build_folder}/hyriseBenchmarkStarSchema --scheduler --clients {num_cores} --cores {num_cores} -t {time} -m Shuffled -s .01")
  move("/tmp/prof.fdata", f"hyriseBenchmarkStarSchema.fdata")
  run_root(f"{build_folder}/hyriseBenchmarkTPCDS --scheduler --clients {num_cores} --cores {num_cores} -t {time} -m Shuffled -s 1")
  move("/tmp/prof.fdata", f"hyriseBenchmarkTPCDS.fdata")
  run_root(f"{build_folder}/hyriseBenchmarkTPCC --scheduler --clients {num_cores} --cores {num_cores} -t {time} -m Shuffled -s 1")
  move("/tmp/prof.fdata", f"hyriseBenchmarkTPCC.fdata")
  run_root("mv *.profraw libhyrise.profraw")

def process_profiles():
  remove("bolt.fdata")
  run_build("merge-fdata *.fdata > bolt.fdata")
  run_build("llvm-profdata merge -output libhyrise.profdata libhyrise.profraw")

def reset_cmake():
  run_build("cmake -DCOMPILE_FOR_BOLT=OFF -DPGO_INSTRUMENT=OFF -UPGO_OPTIMIZE ..")

def export_profile():
  copy("bolt.fdata", "../resources/bolt.fdata")
  copy("libhyrise.profdata", "../resources/libhyrise.profdata")

def main():
  args = parser.parse_args()
  if args.ci:
    build_for_profiling(args.num_cores, ci_benchmarks)
    profile_in_ci(args.num_cores, args.time)
  else:
    build_for_profiling(args.num_cores)
    profile(args.num_cores, args.time)
  process_profiles()
  build_with_optimization(args.num_cores, "hyriseTest" if args.ci else "")
  reset_cmake()
  if args.export:
    export_profile()
