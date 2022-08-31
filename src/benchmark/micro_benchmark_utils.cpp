#include "micro_benchmark_utils.hpp"

#include <stddef.h>

namespace hyrise {

void micro_benchmark_clear_cache() {
  constexpr auto ITEM_COUNT = 500 * 1000 * 1000;
  auto clear = std::vector<int>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += 1;
  }
}

}  // namespace hyrise
