#include "buffer_benchmark_utils.hpp"
#include "benchmark/benchmark.h"

namespace hyrise {
void micro_benchmark_clear_cache() {
  constexpr auto ITEM_COUNT = 750 * 1024 * 1024;
  auto clear = std::vector<int>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += rand();
  }

  auto data = clear.size();
  benchmark::DoNotOptimize(data);
}
}  // namespace hyrise