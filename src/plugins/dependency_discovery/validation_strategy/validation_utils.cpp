#include "validation_utils.hpp"

#include <map>

#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
struct MinMaxContainer {
  MinMaxContainer(const T& init_min, const T& init_max) : min{init_min}, max{init_max} {}

  T min;
  T max;
};

// bool: Everything is null.
template <typename T>
typename ValidationUtils<T>::ColumnStatistics gather_segment_statistics(const std::shared_ptr<const Chunk>& chunk,
                                                                        const ColumnID column_id) {
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

}  // namespace

namespace hyrise {

template <typename T>
typename ValidationUtils<T>::ColumnStatistics ValidationUtils<T>::collect_column_statistics(
    const std::shared_ptr<const Table>& table, const ColumnID column_id, bool early_out) {
  auto min_max_ordered = std::map<T, std::vector<std::shared_ptr<MinMaxContainer<T>>>>{};
  auto is_unique = true;
  auto column_statistics = ValidationUtils<T>::ColumnStatistics{};
  column_statistics.contains_only_nulls = true;
  column_statistics.all_segments_unique = true;
  column_statistics.segments_continuous = true;

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) {
      continue;
    }

    const auto& segment_statistics = gather_segment_statistics<T>(chunk, column_id);

    column_statistics.all_segments_dictionary &= segment_statistics.all_segments_dictionary;

    column_statistics.all_segments_unique &= segment_statistics.all_segments_unique;
    if (!column_statistics.all_segments_unique && early_out) {
      return {};
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
    const auto& min_max_container = std::make_shared<MinMaxContainer<T>>(min, max);
    min_max_ordered[min].emplace_back(min_max_container);
    min_max_ordered[max].emplace_back(min_max_container);
  }

  column_statistics.min = min_max_ordered.cbegin()->first;
  column_statistics.max = min_max_ordered.crbegin()->first;
  column_statistics.all_segments_unique = is_unique;

  if (!column_statistics.all_segments_unique) {
    return column_statistics;
  }

  // check if everything is disjoint and continuous
  for (auto it = min_max_ordered.cbegin(); it != min_max_ordered.cend(); ++it) {
    // if list contains more than 1 value: not continuous, not disjoint.
    const auto& min_max_containers = it->second;
    if (min_max_containers.size() != 1) {
      return column_statistics;
    }

    // check if next or previous item is same --> no overlap.
    const auto& min_max_container = min_max_containers.front();
    // no overlap possible if only one value in segment
    if (min_max_container->min == min_max_container->max) {
      continue;
    }

    auto neighbor_identical = false;
    if (it != min_max_ordered.cbegin()) {
      // we already visited that list and ensured it is unique.
      neighbor_identical = min_max_container == std::prev(it)->second.front();
    }

    if (!neighbor_identical && std::next(it) != min_max_ordered.cend()) {
      // we will check that list in the next step and ensure it is unique.
      neighbor_identical = min_max_container == std::next(it)->second.front();
    }

    if (!neighbor_identical) {
      column_statistics.segments_continuous = false;
      return column_statistics;
    }

    if constexpr (std::is_integral_v<T>) {
      if (it->first == min_max_container->max && std::next(it) != min_max_ordered.cend()) {
        column_statistics.segments_continuous &= it->first + 1 == std::next(it)->first;
      }
    }
  }

  if constexpr (!std::is_integral_v<T>) {
    column_statistics.segments_continuous = false;
  }
  column_statistics.segments_disjoint = true;
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

    const auto& segment_statistics = gather_segment_statistics<T>(chunk, column_id);

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
