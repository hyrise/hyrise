#pragma once

#include <stdint.h>
#include <vector>

namespace hyrise {

void micro_benchmark_clear_cache();
void micro_benchmark_clear_disk_cache();
std::vector<uint32_t> generate_random_indexes(uint32_t number);

}  // namespace hyrise
