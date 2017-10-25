#include <benchmark/benchmark.h>

/**
 * We don't use BENCHMARK_MAIN() - and here is why:
 *
 * The BENCHMARK*() macros generate
 */


int main(int argc, char** argv) {
  // BENCHMARK_MAIN begin
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  // BENCHMARK_MAIN end

  // Destroy all benchmark::Fixture objects
  ::benchmark::ClearRegisteredBenchmarks();
}
