#include <memory>

#include "benchmark/benchmark.h"
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

/**
 * Welcome to the benchmark playground. Here, you can quickly compare two
 * approaches in a minimal setup. Of course you can also use it to just benchmark
 * one single thing.
 *
 * In this example, a minimal TableScan-like operation is used to evaluate the
 * performance impact of pre-allocating the result vector (PosList in hyrise).
 *
 * A few tips:
 * * The optimizer is not your friend. If you do a bunch of calculations and
 *   don't actually use the result, it will optimize your code out and you will
 *   benchmark only noise.
 * * benchmark::DoNotOptimize(<expression>); marks <expression> as "globally
 *   aliased", meaning that the compiler has to assume that any operation that
 *   *could* access this memory location will do so.
 *   However, despite the name, this will not prevent the compiler from
 *   optimizing this expression itself!
 * * benchmark::ClobberMemory(); can be used to force calculations to be written
 *   to memory. It acts as a memory barrier. In combination with DoNotOptimize(e),
 *   this function effectively declares that it could touch any part of memory,
 *   in particular globally aliased memory.
 * * More information on that: https://stackoverflow.com/questions/40122141/
 */

using ValueT = int32_t;

class BenchmarkPlaygroundFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    MicroBenchmarkBasicFixture::SetUp(state);

    _clear_cache();

    // Fill the vector with 1M values in the pattern 0, 1, 2, 3, 0, 1, 2, 3, ...
    // The "TableScan" will scan for one value (2), so it will select 25%.
    _vec.resize(1'000'000);
    std::generate(_vec.begin(), _vec.end(), []() {
      static ValueT value = 0;
      value = (value + 1) % 4;
      return value;
    });
  }

  void TearDown(::benchmark::State& state) override {
    MicroBenchmarkBasicFixture::TearDown(state);
  }

 protected:
  std::vector<ValueT> _vec;
};

/**
 * Reference implementation, growing the vector on demand
 */
BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_Reference)(benchmark::State& state) {
  // Add some benchmark-specific setup here

  for (auto _ : state) {
    std::vector<size_t> result;
    benchmark::DoNotOptimize(result.data());  // Do not optimize out the vector
    const auto size = _vec.size();
    for (auto index = size_t{0}; index < size; ++index) {
      if (_vec[index] == 2) {
        result.push_back(index);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

/**
 * Alternative implementation, pre-allocating the vector
 */
BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_PreAllocate)(benchmark::State& state) {
  // Add some benchmark-specific setup here

  for (auto _ : state) {
    std::vector<size_t> result;
    benchmark::DoNotOptimize(result.data());  // Do not optimize out the vector
    // pre-allocate result vector
    result.reserve(250'000);
    const auto size = _vec.size();
    for (auto index = size_t{0}; index < size; ++index) {
      if (_vec[index] == 2) {
        result.push_back(index);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

}  // namespace hyrise
