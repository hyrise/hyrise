#include <benchmark/benchmark.h>

int main(int argc, char** argv) {
  // mostly taken from the BENCHMARK_MAIN macro
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  ::benchmark::RunSpecifiedBenchmarks();

  /*
   * Cleanup benchmarks right away so we dont get into race conditions
   * with the NUMA memory resources.
   */
  ::benchmark::ClearRegisteredBenchmarks();
}
