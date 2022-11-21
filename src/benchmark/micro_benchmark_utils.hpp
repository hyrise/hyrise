#pragma once

#include <vector>

namespace hyrise {

void micro_benchmark_clear_cache();
void micro_benchmark_clear_disk_cache();
std::vector<uint> generate_random_indexes(uint number);

}  // namespace hyrise
