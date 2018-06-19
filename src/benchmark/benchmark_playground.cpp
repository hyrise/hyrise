#include <memory>

#include "benchmark/benchmark.h"

#include "benchmark_basic_fixture.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

using ValueT = int32_t;

class BenchmarkPlaygroundFixture : public BenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    BenchmarkBasicFixture::SetUp(state);

    _vec.resize(1'000'000);
    std::generate(_vec.begin(), _vec.end(), []() {
      static ValueT v = 0;
      v = ++v % 4;
      return v;
    });
  }
  void TearDown(::benchmark::State& state) override { BenchmarkBasicFixture::TearDown(state); }

 protected:
  std::vector<ValueT> _vec;
};

BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_Reference)(benchmark::State& state) {
  clear_cache();

  // warm up
  for (const auto& element : _vec) {
    benchmark::DoNotOptimize(element);
  }

  while (state.KeepRunning()) {
    std::vector<size_t> result;
    benchmark::DoNotOptimize(result.data());
    auto size = _vec.size();
    for (size_t i = 0; i < size; ++i) {
      if (_vec[i] == 2) {
        result.push_back(i);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_PreAllocate)(benchmark::State& state) {
  clear_cache();

  // warm up
  for (const auto& element : _vec) {
    benchmark::DoNotOptimize(element);
  }

  while (state.KeepRunning()) {
    std::vector<size_t> result;
    // pre-allocate result vector
    result.reserve(250'000);
    benchmark::DoNotOptimize(result.data());
    auto size = _vec.size();
    for (size_t i = 0; i < size; ++i) {
      if (_vec[i] == 2) {
        result.push_back(i);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

}  // namespace opossum
