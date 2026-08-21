#include "aggregate_dyod_config.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "operators/aggregate_dyod/cache_info.hpp"

namespace hyrise {

size_t keys_budget_for(const CacheSizes& sizes) {
  return sizes.l3_bytes / sizes.llc_sharing_cpus / MERGE_MAP_BYTES_PER_KEY;
}

size_t keys_budget() {
  return keys_budget_for(cache_sizes());
}

size_t estimate_sample_stride(const size_t chunk_count) {
  return std::max(size_t{1}, chunk_count / ESTIMATE_SAMPLE_CHUNKS);
}

size_t scale_sampled_estimate(const size_t estimate, const size_t half_sample_estimate, const size_t total_row_count,
                              const size_t stride) {
  if (stride == 1) {
    return estimate;
  }
  if (estimate == 0) {
    return total_row_count;
  }
  const auto growth = std::clamp(
      static_cast<double>(estimate) / static_cast<double>(std::max(half_sample_estimate, size_t{1})), 1.0, 2.0);
  const auto scaled = static_cast<double>(estimate) * std::pow(static_cast<double>(stride), std::log2(growth));
  const auto ceiling = std::min(static_cast<double>(total_row_count), static_cast<double>(estimate * stride));
  return static_cast<size_t>(std::llround(std::clamp(scaled, static_cast<double>(estimate), ceiling)));
}

PartitionCount max_partition_count_for(const CacheSizes& sizes, const size_t stream_count) {
  const auto staging_bytes_per_partition = std::max(size_t{1}, stream_count) * (SWWC_LINE_BYTES + sizeof(size_t));
  const auto fitting_partitions = std::bit_floor(std::max(size_t{1}, sizes.l2_bytes / staging_bytes_per_partition));
  return static_cast<PartitionCount>(std::min(fitting_partitions, static_cast<size_t>(MAX_PARTITION_COUNT)));
}

PartitionCount max_partition_count(const size_t stream_count) {
  return max_partition_count_for(cache_sizes(), stream_count);
}

size_t merge_tile_rows_for(const CacheSizes& sizes) {
  return sizes.l1d_bytes / MERGE_SCRATCH_L1_DIVISOR / sizeof(uint32_t);
}

size_t merge_tile_rows() {
  return merge_tile_rows_for(cache_sizes());
}

size_t merge_split_ways_for(const size_t partition_rows, const size_t mean_partition_rows,
                            const size_t expected_keys_per_partition, const size_t store_count,
                            const size_t worker_limit) {
  const auto split_limit = std::min(store_count, worker_limit);
  if (split_limit < 2 || mean_partition_rows == 0) {
    return 1;
  }
  if (partition_rows < MERGE_SPLIT_MEAN_ROW_FACTOR * mean_partition_rows ||
      partition_rows < MERGE_SPLIT_ROWS_PER_KEY * expected_keys_per_partition) {
    return 1;
  }
  return std::min(partition_rows / mean_partition_rows, split_limit);
}

size_t low_cardinality_threshold() {
  return keys_budget() / 2;
}

size_t key_piece_width(const size_t key_width) {
  if (key_width % 16 == 0) {
    return 16;
  }
  return key_width % 8 == 0 ? 8 : 4;
}

size_t morsel_count_for(const size_t chunk_rows, const size_t rows_per_morsel) {
  return std::max(size_t{1}, (chunk_rows + rows_per_morsel - 1) / rows_per_morsel);
}

size_t worker_limit_for(const bool is_multi_threaded, const size_t num_cpus) {
  return is_multi_threaded ? std::max(size_t{1}, num_cpus) : size_t{1};
}

}  // namespace hyrise
