#include "micro_benchmark_utils.hpp"

#include <cstddef>
#include <vector>

namespace hyrise {

void micro_benchmark_clear_cache() {
  constexpr auto ITEM_COUNT = size_t{2} * 1024 * 1024 * 1024 / sizeof(int);
  auto clear = std::vector<int>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += 1;
  }
}

}  // namespace hyrise
