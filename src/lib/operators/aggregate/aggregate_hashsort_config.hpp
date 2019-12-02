#pragma once

#include <tuple>

namespace opossum {

struct AggregateHashSortConfig {
  size_t hash_table_size{100'000};
  float hash_table_max_load_factor{0.25f};
  size_t max_partitioning_counter{100'000};

  size_t buffer_flush_threshold{255};

  // Number of rows to fetch from a table to fill an initial run
  size_t task_group_count_target{300'000};

  // Once a hashtable is full, AggregateHashSort can either build a next one (if the local density of groups is
  // determined to be high enough) or switch to partitioning
  float continue_hashing_density_threshold{3};

  auto to_tuple() const {
    return std::tuple{hash_table_size, hash_table_max_load_factor, max_partitioning_counter, buffer_flush_threshold, task_group_count_target, continue_hashing_density_threshold};
  }
};

inline std::ostream& operator<<(std::ostream& stream, const AggregateHashSortConfig& config) {
  stream << "hash_table_size: " << config.hash_table_size << "; "
      << "hash_table_max_load_factor: " << config.hash_table_max_load_factor << "; "
      << "max_partitioning_counter: " << config.max_partitioning_counter << "; "
      << "buffer_flush_threshold: " << config.buffer_flush_threshold << "; "
      << "task_group_count_target: " << config.task_group_count_target << "; "
      << "continue_hashing_density_threshold: " << config.continue_hashing_density_threshold << ";";
  return stream;
}

// For gtest
inline bool operator==(const AggregateHashSortConfig& lhs, const AggregateHashSortConfig& rhs) {
  return lhs.to_tuple() == rhs.to_tuple();
}

// For gtest
inline bool operator<(const AggregateHashSortConfig& lhs, const AggregateHashSortConfig& rhs) {
  return lhs.to_tuple() < rhs.to_tuple();
}

}  // namespace opossum
