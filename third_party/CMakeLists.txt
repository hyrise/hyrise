# Build google benchmark
add_library(
    benchmark
    STATIC
    benchmark_fix/dummy.cc
    benchmark/src/timers.cc
    benchmark/src/sleep.cc
    benchmark/src/reporter.cc
    benchmark/src/complexity.cc
    benchmark/src/colorprint.cc
    benchmark/src/sysinfo.cc
    benchmark/src/json_reporter.cc
    benchmark/src/commandlineflags.cc
    benchmark/src/benchmark.cc
    benchmark/src/string_util.cc
    benchmark/src/benchmark_register.cc
    benchmark/src/console_reporter.cc
    benchmark/src/csv_reporter.cc
)

target_compile_options(
    benchmark
    PRIVATE
    -O3 -std=c++11 -DHAVE_STD_REGEX -DNDEBUG
)

target_include_directories(
    benchmark
    PRIVATE
    benchmark/include
)