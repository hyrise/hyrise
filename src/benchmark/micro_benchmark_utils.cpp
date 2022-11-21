#include "micro_benchmark_utils.hpp"

#include <stddef.h>
#include <unistd.h>
#include <fstream>

#include <algorithm>
#include <random>

namespace hyrise {

void micro_benchmark_clear_cache() {
  constexpr auto ITEM_COUNT = 500 * 1000 * 1000;
  auto clear = std::vector<int>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += 1;
  }
}

void micro_benchmark_clear_disk_cache() {
  // TODO(phoenix): better documentation of which caches we are clearing
  sync();
  std::ofstream ofs("/proc/sys/vm/drop_caches");
  ofs << "3" << std::endl;
}

/**
 * Generates a vector containing random indexes between 0 and number.
*/
std::vector<uint> generate_random_indexes(uint number) {
  std::vector<uint> sequence(number);
  std::iota(std::begin(sequence), std::end(sequence), 0);
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(sequence), std::end(sequence), rng);

  return sequence;
}

}  // namespace hyrise
