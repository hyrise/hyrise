#pragma once

namespace opossum {

struct AggregateHashSortConfig {
  size_t hash_table_size{10'000};
  float hash_table_max_load_factor{0.25f};
  size_t max_partitioning_counter{10'000};

  size_t min_initial_run_row_count{500};
  size_t max_initial_run_row_count{100'000};
};

}  // namespace opossum
