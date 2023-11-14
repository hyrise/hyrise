#pragma once

#include <memory>
#include <unordered_set>

#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

namespace hyrise {

class Chunk;
class Table;

template <typename T>
void create_pruning_statistics_for_segment(AttributeStatistics<T>& segment_statistics,
                                           const pmr_vector<T>& dictionary) {
  if constexpr (std::is_arithmetic_v<T>) {
    segment_statistics.set_statistics_object(RangeFilter<T>::build_filter(dictionary));
  } else {
    if (!dictionary.empty()) {
      segment_statistics.set_statistics_object(
          std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back()));
    }
  }

  segment_statistics.set_statistics_object(std::make_shared<DistinctValueCount>(dictionary.size()));
}

/**
 * Generate Pruning Filters for an immutable Chunk
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk);

/**
 * Generate Pruning Filters for all immutable Chunks in this Table
 */
void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table);

}  // namespace hyrise
