#include <benchmark/benchmark.h>

/**
 * We don't use BENCHMARK_MAIN() - and here is why:
 *
 * 1. The BENCHMARK*() macros generate a line that will instanciate the specified Fixture class as a static object and
 * register it with BenchmarkFamilies::GetInstance(). This will create the BenchmarkFamilies instance that manages
 * the fixture instances.
 * 2. Only afterwards will somebody create the default_memory_resource by calling get_default_resource().
 * 3. <BenchmarkFixture>::SetUp() will be called and save data that was allocated via the default_memory_resource in the
 * BenchmarkFixture object. (This could e.g. be a PosList held in a ReferenceSegment held in a Chunk held in a Table.)
 * 4. main() finishes.
 * 5. The default_memory_resource will be torn down before the BenchmarkFamilies instance, because it was created after
 * it.
 * 6. The BenchmarkFamilies instance is torn down and tries to deallocate the aforementioned PosList - *but the memory
 * resource it tries to hand the memory back to is already destroyed*
 *
 *
 * ---> Major C++ globals mess! And no satisfyingly clean way to solve it... <---
 *
 *
 * clang doesn't seem to produce a visible error atm, on gcc, this might result in the following error:
 * '''
 * pure virtual method called
 * terminate called without an active exception
 * '''
 *
 *
 * For the benchmarks we "solve" this by calling ::benchmark::ClearRegisteredBenchmarks() which destroys all fixtures
 * before the end of main().
 *
 *
 * ---> But similar things could happen anytime you call, e.g. StorageManager::get() before calling
 * get_default_memory_resource() <---
 *
 *
 * NOTE: One might think that Tests suffer from the same problem, since they both come from google - but they don't
 * seem to be keeping the fixtures around and instead create and destroy them when running the tests.
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
