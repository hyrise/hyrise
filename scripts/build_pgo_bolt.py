#!/usr/bin/env python
# This script builds an optimized version of the libhyrise_impl binary using various optimization tools
# - Profile Guided Optimizations (https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)
# - Binary Optimization and Layouting Tool (https://github.com/llvm/llvm-project/tree/main/bolt)
# To use it, first create a cmake build folder and configure cmake. Then run this script from the build folder.
# You can run the script with -h to discover cli arguments.

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
from subprocess import run
from shutil import copy, move
from os import cpu_count, getcwd, remove
from os.path import exists

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-t",  "--time", type=int, default=1800, help="The time to run each benchmark in seconds.")
parser.add_argument("-n", "--num-cores", type=int, default=cpu_count(), help="The number of cpu cores to use for compiling and running.")
parser.add_argument("-c", "--ci", action=BooleanOptionalAction, default=False, help="Whether this script is run in the ci. Tries to safe time as much as possible at the expense of profile quality.")
parser.add_argument("-e", "--export-profile", action=BooleanOptionalAction, default=False, help="Export the profile to the resources folder for further use with benchmark_all.sh or --import-profile.")
parser.add_argument("-i", "--import-profile", action=BooleanOptionalAction, default=False, help="Don't run benchmarks, just import the profile data from the resources folder.")
args = parser.parse_args()

build_folder = getcwd()
benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkTPCC", "hyriseBenchmarkJoinOrder", "hyriseBenchmarkStarSchema"]
ci_benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkTPCC", "hyriseBenchmarkStarSchema"]
benchmarks_with_float_scaling = {"hyriseBenchmarkTPCH", "hyriseBenchmarkStarSchema"}

def run_root(cmd):
  print(f"python@{build_folder}/..: {cmd}")
  run(cmd, cwd=f"{build_folder}/..", shell=True)

def run_build(cmd):
  print(f"python@{build_folder}: {cmd}")
  run(cmd, cwd=build_folder, shell=True)

def build(targets=benchmarks, bolt_instrument=False, pgo_instrument=False, bolt_optimize=False, pgo_optimize=False):
  run_build("ninja clean")
  run_build(f"cmake -DCOMPILE_FOR_BOLT={"On" if bolt_instrument else "Off"} -DPGO_INSTRUMENT={"On" if pgo_instrument else "off"} {"-DPGO_OPTIMIZE=libhyrise.profdata" if pgo_optimize else "-UPGO_OPTIMIZE"} ..")
  run_build(f"ninja {" ".join(targets)} -j {args.num_cores}")
  if bolt_instrument:
    move("lib/libhyrise_impl.so", "lib/libhyrise_impl.so.old")
    run_build("llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so")
  if bolt_optimize:
    move("lib/libhyrise_impl.so", "lib/libhyrise_impl.so.old")
    run_build("llvm-bolt lib/libhyrise_impl.so.old -o lib/libhyrise_impl.so -data bolt.fdata -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold -split-eh -dyno-stats")
    run_build("strip -R .rela.text -R \".rela.text.*\" -R .rela.data -R \".rela.data.*\" lib/libhyrise_impl.so")

def profile(bolt_instrumented=False, pgo_instrumented=False):
  benchmarks_to_run = benchmarks if not args.ci else ci_benchmarks
  for benchmark in benchmarks_to_run:
    run_root(f"{build_folder}/{benchmark} --scheduler --clients {args.num_cores} --cores {args.num_cores} -t {args.time} -m Shuffled {"" if not args.ci else ("-s 0.01" if benchmark in benchmarks_with_float_scaling else "-s 1")}")
    if bolt_instrumented:
      move("/tmp/prof.fdata", f"{benchmark}.fdata")

  if bolt_instrumented:
    if exists("bolt.fdata"):
      remove("bolt.fdata")
    run_build("merge-fdata *.fdata > bolt.fdata")

  if pgo_instrumented:
    run_root(f"mv *.profraw {build_folder}/libhyrise.profraw")
    run_build("llvm-profdata merge -output libhyrise.profdata libhyrise.profraw")

def reset_cmake():
  run_build("cmake -DCOMPILE_FOR_BOLT=OFF -DPGO_INSTRUMENT=OFF -UPGO_OPTIMIZE ..")

def export_profile():
  copy("bolt.fdata", "../resources/bolt.fdata")
  copy("libhyrise.profdata", "../resources/libhyrise.profdata")

def import_profile():
  copy("../resources/bolt.fdata", "bolt.fdata")
  copy("../resources/libhyrise.profdata", "libhyrise.profdata")

def ci_main():
  build(ci_benchmarks, bolt_instrument=True, pgo_instrument=True)
  profile(bolt_instrumented=True, pgo_instrumented=True)
  build("hyriseTest", bolt_optimize=True, pgo_optimize=True)
  reset_cmake()

def main():
  if args.import_profile:
    import_profile()
  else:
    build(bolt_instrument=True, pgo_instrument=True)
    profile(bolt_instrumented=True, pgo_instrumented=True)
  build(bolt_optimize=True, pgo_optimize=True)
  reset_cmake()
  if args.export_profile:
    export_profile()

if args.ci:
  ci_main()
else:
  main()
