#include "micro_benchmark_utils.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hyrise {

void micro_benchmark_clear_cache() {
  // We create a vector of 2 GiB and iterate it once to process more data than the largest current last-level cache
  // (i.e., ~1.1 GB for an AMD EPYC 9684X) can hold.
  constexpr auto ITEM_COUNT = size_t{2} * 1024 * 1024 * 1024 / sizeof(int32_t);
  auto clear = std::vector<int32_t>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += 1;
  }
}

}  // namespace hyrise
