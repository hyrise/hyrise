#!/usr/bin/env python

# This script builds an optimized version of the libhyrise_impl library using the following optimizations:
#   - Profile Guided Optimizations (PGO; https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)
#   - Binary Optimization and Layouting Tool (BOLT; https://github.com/llvm/llvm-project/tree/main/bolt)
#
# To use it, first create a cmake build folder and configure cmake for a release build. Then run this script from the
# build folder. The script takes care of adapting the cmake parameters for PGO/BOLT and reverts its changes after
# it is finished, even when it crashed. You can run the script with -h for a detailed explanation of cli arguments.
# There is a GiHhub comment summarizing the PGO options we evaluated:
#   https://github.com/hyrise/hyrise/pull/2724#issuecomment-3734286523

import platform
import sys

from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    BooleanOptionalAction,
)
from os import cpu_count, getcwd
from subprocess import run

if platform.system() != 'Linux':
    # Note: macOS support is possible but currently out of scope.
    print("This script has only been tested on Linux.")
    sys.exit(1)


parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "-t",
    "--time",
    type=int,
    default=3600,
    help="The time to run each benchmark in seconds. The default value has been used in our evaluation and has "
    "generated noticable optimization results. Keep in mind that this is the time for each of the five benchmarks, "
    "which are first run for PGO and then another time for BOLT. The runtime of the script is, therefore, at least "
    "10x this value, in practice longer.",
)
parser.add_argument(
    "-n",
    "--num-cores",
    type=int,
    default=cpu_count(),
    help="The number of CPU cores to use for compiling and benchmarking.",
)
parser.add_argument(
    "-c",
    "--ci",
    action=BooleanOptionalAction,
    default=False,
    help="Whether this script is run in the CI. Improves runtime while reducing profile quality to an absolute "
    "minimum. This is not intended for actual optimization, just to test the script.",
)
parser.add_argument(
    "-e",
    "--export-profile",
    action=BooleanOptionalAction,
    default=False,
    help="Do not build an optimized library, just benchmark and export the profile to the resources folder. Useful if "
    "you want to reuse your profiles, or even store them in git.",
)
parser.add_argument(
    "-i",
    "--import-profile",
    action=BooleanOptionalAction,
    default=False,
    help="Do not run benchmarks, just import the profile data from the resources folder and build an optimized library."
)
parser.add_argument(
    "-p", "--pgo", action=BooleanOptionalAction, default=True, help="Use PGO for profiling / optimization."
)
parser.add_argument(
    "-b", "--bolt", action=BooleanOptionalAction, default=True, help="Use BOLT for profiling / optimization."
)
parser.add_argument("-s", "--build-system", type=str, default="ninja", help="The build system used by this script")
args = parser.parse_args()
assert not (
    args.export_profile and args.import_profile
), "You cannot export and import at the same time. This would result in no benchmarks or library built."
assert args.pgo or args.bolt, "You should either specify --pgo or --bolt for any optimization."

build_folder = getcwd()
benchmarks = [
    "hyriseBenchmarkTPCH",
    "hyriseBenchmarkTPCDS",
    "hyriseBenchmarkTPCC",
    "hyriseBenchmarkJoinOrder",
    "hyriseBenchmarkStarSchema",
]
ci_benchmarks = [
    "hyriseBenchmarkTPCH",
    "hyriseBenchmarkTPCDS",
    "hyriseBenchmarkTPCC",
    "hyriseBenchmarkStarSchema",
]
benchmarks_with_float_scaling = {"hyriseBenchmarkTPCH", "hyriseBenchmarkStarSchema"}


def run_in_hyrise_folder(*cmd, check=True):
    cmd = " ".join(cmd)
    print(f"python@root: {cmd}", flush=True)
    run(cmd, cwd=f"{build_folder}/..", shell=True, check=check)


def run_in_build_folder(*cmd, check=True):
    cmd = " ".join(cmd)
    print(f"python@build: {cmd}", flush=True)
    run(cmd, cwd=build_folder, shell=True, check=check)


def build(
    *targets,
    bolt_instrument=False,
    pgo_instrument=False,
    bolt_optimize=False,
    pgo_optimize=False,
):
    run_in_build_folder(f"{args.build_system} clean")
    run_in_build_folder(
        "cmake",
        f"-DCOMPILE_FOR_BOLT={"On" if bolt_instrument or bolt_optimize else "Off"}",
        f"-DPGO_INSTRUMENT={"On" if pgo_instrument else "Off"}",
        "-DPGO_OPTIMIZE=libhyrise.profdata" if pgo_optimize else "-UPGO_OPTIMIZE",
        "..",
    )
    run_in_build_folder(f"{args.build_system} {" ".join(targets)} -j {args.num_cores}")
    if bolt_instrument:
        run_in_build_folder("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
        run_in_build_folder("llvm-bolt lib/libhyrise_impl.so.old -instrument -o lib/libhyrise_impl.so")
    if bolt_optimize:
        run_in_build_folder("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
        run_in_build_folder(
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
        run_in_build_folder(
            'strip -R .rela.text -R ".rela.text.*" -R .rela.data -R ".rela.data.*" lib/libhyrise_impl.so'
        )


# This function can be used if the previous build has already built libhyrise_impl and moved it to '.old'. Then bolt can
# just use the old compilation output, which is significantly faster than rebuilding the binary. If a source file
# changed, then this will trigger a full rebuild of libhyrise. So this function should only be called by this script,
# if it can be sure that itself build the proper library and no input files changed.
def build_with_bolt_from_previous_build(*targets):
    run_in_build_folder("mv lib/libhyrise_impl.so.old lib/libhyrise_impl.so")
    run_in_build_folder(f"{args.build_system} {" ".join(targets)} -j {args.num_cores}")
    run_in_build_folder("mv lib/libhyrise_impl.so lib/libhyrise_impl.so.old")
    run_in_build_folder(
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
    run_in_build_folder('strip -R .rela.text -R ".rela.text.*" -R .rela.data -R ".rela.data.*" lib/libhyrise_impl.so')


def profile(bolt_instrumented=False, pgo_instrumented=False):
    benchmarks_to_run = benchmarks if not args.ci else ci_benchmarks
    for benchmark in benchmarks_to_run:
        run_in_hyrise_folder(
            f"{build_folder}/{benchmark}",
            "--scheduler",
            f"--clients {args.num_cores}",
            f"--cores {args.num_cores}",
            f"-t {args.time}",
            "-m Shuffled",
            ("" if not args.ci else ("-s 0.01" if benchmark in benchmarks_with_float_scaling else "-s 1")),
        )
        if bolt_instrumented:
            run_in_build_folder(f"mv /tmp/prof.fdata {benchmark}.fdata")

    if bolt_instrumented:
        run_in_build_folder("merge-fdata *.fdata > bolt.fdata")

    if pgo_instrumented:
        run_in_hyrise_folder(f"mv *.profraw {build_folder}/libhyrise.profraw")
        run_in_build_folder("llvm-profdata merge -output libhyrise.profdata libhyrise.profraw")


def cleanup():
    run_in_build_folder("cmake -DCOMPILE_FOR_BOLT=Off -DPGO_INSTRUMENT=Off -UPGO_OPTIMIZE ..")
    run_in_build_folder("rm *.fdata", check=False)
    run_in_build_folder("rm *.profraw", check=False)
    run_in_build_folder("rm *.profdata", check=False)
    run_in_hyrise_folder("rm *.profraw", check=False)


def export_profile():
    if args.bolt:
        run_in_build_folder("cp bolt.fdata ../resources/bolt.fdata")
    if args.pgo:
        run_in_build_folder("cp libhyrise.profdata ../resources/libhyrise.profdata")


def import_profile():
    if args.bolt:
        run_in_build_folder("cp ../resources/bolt.fdata bolt.fdata")
    if args.pgo:
        run_in_build_folder("cp ../resources/libhyrise.profdata libhyrise.profdata")


def ci_main():
    build(*ci_benchmarks, pgo_instrument=True)
    profile(pgo_instrumented=True)
    build(*ci_benchmarks, pgo_optimize=True, bolt_instrument=True)
    profile(bolt_instrumented=True)
    build_with_bolt_from_previous_build("hyriseTest")


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
                # If we know that we have previously built the library during benchmarks and we want to optimize with
                # BOLT, then we can reuse the benchmark library and just apply BOLT on that. This works with and
                # without PGO. If PGO was active, then BOLT already instrumented the PGO optimized binary during
                # benchmarks, which means that we can reuse the already optimized binary here and optimize it with BOLT.
                build_with_bolt_from_previous_build()
            else:
                build(bolt_optimize=args.bolt, pgo_optimize=args.pgo)
    finally:
        cleanup()


if args.ci:
    ci_main()
else:
    main()
