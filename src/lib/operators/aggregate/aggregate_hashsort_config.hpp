#pragma once

namespace opossum {

struct AggregateHashSortConfig {
  size_t hash_table_size{100'000};
  float hash_table_max_load_factor{0.25f};
  size_t max_partitioning_counter{100'000};
};

}  // namespace opossum
