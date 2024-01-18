#include "validation_utils.hpp"

#include <map>

#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"

namespace hyrise {

template <typename T>
typename ValidationUtils<T>::ColumnStatistics ValidationUtils<T>::gather_segment_statistics(
    const std::shared_ptr<const Chunk>& chunk, const ColumnID column_id) {
  const auto& segment = chunk->get_segment(column_id);
  auto statistics = typename ValidationUtils<T>::ColumnStatistics{};

  if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment)) {
    const auto& dictionary = *dictionary_segment->dictionary();
    statistics.all_segments_dictionary = true;
    if (dictionary.empty()) {
      statistics.contains_only_nulls = true;
      return statistics;
    }
    statistics.all_segments_unique = dictionary.size() == dictionary_segment->size();
    statistics.min = dictionary.front();
    statistics.max = dictionary.back();
    return statistics;
  }

  const auto& pruning_statistics = chunk->pruning_statistics();
  if (!pruning_statistics) {
    return statistics;
  }

  const auto& segment_statistics = std::dynamic_pointer_cast<AttributeStatistics<T>>(pruning_statistics->at(column_id));
  if (!segment_statistics) {
    return statistics;
  }

  if (segment_statistics->distinct_value_count) {
    statistics.all_segments_unique = segment_statistics->distinct_value_count->count == segment->size();
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (segment_statistics->range_filter) {
      const auto& ranges = segment_statistics->range_filter->ranges;
      statistics.min = ranges.front().first;
      statistics.max = ranges.back().second;
      return statistics;
    }
  }

  if (segment_statistics->min_max_filter) {
    const auto& min_max_filter = *segment_statistics->min_max_filter;
    statistics.min = min_max_filter.min;
    statistics.max = min_max_filter.max;
    return statistics;
  }

  // Pruning statistics available, but no statistics objects set: Everything is null!
  Assert(segment_statistics->null_value_ratio && segment_statistics->null_value_ratio->ratio == 0.0f,
         "Expected segment completely with NULLs");
  statistics.contains_only_nulls = true;
  return statistics;
}

template <typename T>
typename ValidationUtils<T>::ColumnStatistics ValidationUtils<T>::collect_column_statistics(
    const std::shared_ptr<const Table>& table, const ColumnID column_id, bool early_out) {
  auto min_max_ordered = std::map<T, ChunkID>{};
  auto column_statistics = ValidationUtils<T>::ColumnStatistics{};
  column_statistics.contains_only_nulls = true;
  column_statistics.all_segments_unique = true;
  column_statistics.segments_disjoint = true;
  column_statistics.all_segments_dictionary = true;
  auto continue_index_build = true;

  const auto chunk_count = table->chunk_count();
  auto previous_it = min_max_ordered.begin();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) {
      continue;
    }

    const auto& segment_statistics = gather_segment_statistics(chunk, column_id);

    column_statistics.all_segments_dictionary &= segment_statistics.all_segments_dictionary;

    column_statistics.all_segments_unique &= segment_statistics.all_segments_unique;
    if (!column_statistics.all_segments_unique && early_out) {
      return column_statistics;
    }

    if (segment_statistics.contains_only_nulls) {
      continue;
    }

    column_statistics.contains_only_nulls = false;
    if (!segment_statistics.min || !segment_statistics.max) {
      return column_statistics;
    }

    const auto& min = *segment_statistics.min;
    const auto& max = *segment_statistics.max;

    if (!continue_index_build) {
      continue;
    }

    const auto min_it = min_max_ordered.emplace_hint(previous_it, min, chunk_id);
    const auto max_it = min_max_ordered.emplace_hint(min_it, max, chunk_id);
    previous_it = max_it;

    if (!column_statistics.segments_disjoint) {
      continue;
    }

    // Not disjoint if key already exists or if there is another value between min and max.
    const auto min_max_is_duplicate = min_it->second != chunk_id || max_it->second != chunk_id;
    if (min_max_is_duplicate && early_out) {
      column_statistics.segments_disjoint = false;
      column_statistics.all_segments_unique = false;
      return column_statistics;
    }

    if (min_max_is_duplicate || std::next(min_it) != max_it) {
      column_statistics.segments_disjoint = false;
      continue_index_build = !early_out;
    }
  }

  if (continue_index_build) {
    column_statistics.min = min_max_ordered.cbegin()->first;
    column_statistics.max = min_max_ordered.crbegin()->first;

    if constexpr (std::is_integral_v<T>) {
      column_statistics.segments_continuous =
          column_statistics.segments_disjoint &&
          *column_statistics.max - *column_statistics.min == static_cast<int64_t>(table->row_count() - 1);
    }
  }

  return column_statistics;
}

template <typename T>
std::optional<std::pair<T, T>> ValidationUtils<T>::get_column_min_max_value(const std::shared_ptr<const Table>& table,
                                                                            const ColumnID column_id) {
  auto min = T{};
  auto max = T{};
  bool is_initialized = false;
  const auto chunk_count = table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) {
      continue;
    }

    const auto& segment_statistics = gather_segment_statistics(chunk, column_id);

    if (segment_statistics.contains_only_nulls) {
      continue;
    }

    if (!segment_statistics.min || !segment_statistics.max) {
      return {};
    }

    if (!is_initialized) {
      min = *segment_statistics.min;
      max = *segment_statistics.max;
      is_initialized = true;
      continue;
    }

    min = std::min(*segment_statistics.min, min);
    max = std::max(*segment_statistics.max, max);
  }

  if (!is_initialized) {
    return {};
  }

  return std::make_pair(min, max);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValidationUtils);

}  // namespace hyrise
