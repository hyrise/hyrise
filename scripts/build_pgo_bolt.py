#!/usr/bin/env python
# This script builds an optimized version of the libhyrise_impl binary using various optimization tools
# - Profile Guided Optimizations (https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)
# - Binary Optimization and Layouting Tool (https://github.com/llvm/llvm-project/tree/main/bolt)
# To use it, first create a cmake build folder and configure cmake. Then run this script from the build folder.
# You can run the script with -h for a detailed explanation of cli arguments.

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
from subprocess import run
from os import cpu_count, getcwd

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-t", "--time", type=int, default=1800, help="The time to run each benchmark in seconds.")
parser.add_argument(
    "-n",
    "--num-cores",
    type=int,
    default=cpu_count(),
    help="The number of cpu cores to use for compiling and benchmarking.",
)
parser.add_argument(
    "-c",
    "--ci",
    action=BooleanOptionalAction,
    default=False,
    help="Whether this script is run in the ci. Improves runtime at the expense of profile quality.",
)
parser.add_argument(
    "-e",
    "--export-profile",
    action=BooleanOptionalAction,
    default=False,
    help="Don't build an optimized binary, just export the profile to the resources folder.",
)
parser.add_argument(
    "-i",
    "--import-profile",
    action=BooleanOptionalAction,
    default=False,
    help="Don't run benchmarks, just import the profile data from the resources folder.",
)
parser.add_argument(
    "-p",
    "--pgo",
    action=BooleanOptionalAction,
    default=True,
    help="Use PGO for profiling / optimization"
)
parser.add_argument(
    "-b",
    "--bolt",
    action=BooleanOptionalAction,
    default=True,
    help="Use BOLT for profiling / optimization"
)
args = parser.parse_args()

build_folder = getcwd()
benchmarks = [
    "hyriseBenchmarkTPCH",
    "hyriseBenchmarkTPCDS",
    "hyriseBenchmarkTPCC",
    "hyriseBenchmarkJoinOrder",
    "hyriseBenchmarkStarSchema",
]
ci_benchmarks = ["hyriseBenchmarkTPCH", "hyriseBenchmarkTPCDS", "hyriseBenchmarkTPCC", "hyriseBenchmarkStarSchema"]
benchmarks_with_float_scaling = {"hyriseBenchmarkTPCH", "hyriseBenchmarkStarSchema"}


def run_root(*cmd, check=True):
    cmd = " ".join(cmd)
    print(f"python@root: {cmd}", flush=True)
    run(cmd, cwd=f"{build_folder}/..", shell=True, check=check)


def run_build(*cmd, check=True):
    cmd = " ".join(cmd)
    print(f"python@build: {cmd}", flush=True)
    run(cmd, cwd=build_folder, shell=True, check=check)


def build(*targets, bolt_instrument=False, pgo_instrument=False, bolt_optimize=False, pgo_optimize=False):
    run_build("ninja clean")
    run_build(
        "cmake",
        f"-DCOMPILE_FOR_BOLT={"On" if bolt_instrument or bolt_optimize else "Off"}",
        f"-DPGO_INSTRUMENT={"On" if pgo_instrument else "Off"}",
        "-DPGO_OPTIMIZE=libhyrise.profdata" if pgo_optimize else "-UPGO_OPTIMIZE",
        "..",
    )
    run_build(f"ninja {" ".join(targets)} -j {args.num_cores}")
    if bolt_instrument:
        run_build("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
        run_build("llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so")
    if bolt_optimize:
        run_build("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
        run_build(
            "llvm-bolt",
            "lib/libhyrise_impl.so.old",
            "-o lib/libhyrise_impl.so",
            "--data bolt.fdata",
            "--reorder-blocks=ext-tsp",
            "--reorder-functions=cdsort",
            "--split-functions",
            "--split-all-cold",
            "--split-eh",
            "--dyno-stats",
        )
        run_build('strip -R .rela.text -R ".rela.text.*" -R .rela.data -R ".rela.data.*" lib/libhyrise_impl.so')

# This function can be used if the previous build has already build libhyrise_impl and moved it to .old. Then bolt can
# just use the old compilation output. If a source file changed, then this will trigger a full rebuild of libhyrise.
# So this function should only be called by this script, if it can be sure that itself build the proper binary and did
# not change anything.
def build_just_bolt(*targets):
    run_build("mv lib/libhyrise_impl.so.old lib/libhyrise_impl.so")
    run_build(f"ninja {" ".join(targets)} -j {args.num_cores}")
    run_build("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
    run_build(
        "llvm-bolt",
        "lib/libhyrise_impl.so.old",
        "-o lib/libhyrise_impl.so",
        "--data bolt.fdata",
        "--reorder-blocks=ext-tsp",
        "--reorder-functions=cdsort",
        "--split-functions",
        "--split-all-cold",
        "--split-eh",
        "--dyno-stats",
    )
    run_build('strip -R .rela.text -R ".rela.text.*" -R .rela.data -R ".rela.data.*" lib/libhyrise_impl.so')

def profile(bolt_instrumented=False, pgo_instrumented=False):
    benchmarks_to_run = benchmarks if not args.ci else ci_benchmarks
    for benchmark in benchmarks_to_run:
        run_root(
            f"{build_folder}/{benchmark}",
            "--scheduler",
            f"--clients {args.num_cores}",
            f"--cores {args.num_cores}",
            f"-t {args.time}",
            "-m Shuffled",
            "" if not args.ci else ("-s 0.01" if benchmark in benchmarks_with_float_scaling else "-s 1"),
        )
        if bolt_instrumented:
            run_build(f"mv /tmp/prof.fdata {benchmark}.fdata")

    if bolt_instrumented:
        run_build("merge-fdata *.fdata > bolt.fdata")

    if pgo_instrumented:
        run_root(f"mv *.profraw {build_folder}/libhyrise.profraw")
        run_build("llvm-profdata merge -output libhyrise.profdata libhyrise.profraw")


def cleanup():
    run_build("cmake -DCOMPILE_FOR_BOLT=Off -DPGO_INSTRUMENT=Off -UPGO_OPTIMIZE ..")
    run_build("rm *.fdata", check=False)
    run_build("rm *.profraw", check=False)
    run_build("rm *.profdata", check=False)
    run_root("rm *.profraw", check=False)


def export_profile():
    if args.bolt:
        run_build("cp bolt.fdata ../resources/bolt.fdata")
    if args.pgo:
        run_build("cp libhyrise.profdata ../resources/libhyrise.profdata")


def import_profile():
    if args.bolt:
        run_build("cp ../resources/bolt.fdata bolt.fdata")
    if args.pgo:
        run_build("cp ../resources/libhyrise.profdata libhyrise.profdata")


def ci_main():
    build(*ci_benchmarks, pgo_instrument=True)
    profile(pgo_instrumented=True)
    build(*ci_benchmarks, pgo_optimize=True, bolt_instrument=True)
    profile(bolt_instrumented=True)
    build_just_bolt("hyriseTest")


def main():
    try:
        if args.import_profile:
            import_profile()
        else:
            if args.pgo:
                build(*benchmarks, pgo_instrument=True)
                profile(pgo_instrumented=True)
            if args.bolt:
                build(*benchmarks, pgo_optimize=args.pgo, bolt_instrument=True)
                profile(bolt_instrumented=True)

        if args.export_profile:
            export_profile()
        else:
            if not args.import_profile and args.bolt:
                build_just_bolt()
            else:
                build(bolt_optimize=args.bolt, pgo_optimize=args.pgo)
    finally:
        cleanup()


if args.ci:
    ci_main()
else:
    main()
