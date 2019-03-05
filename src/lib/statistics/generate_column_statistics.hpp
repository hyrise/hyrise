#pragma once

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Generate the statistics of a single column. Used by generate_table_statistics()
 */
template <typename ColumnDataType>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics(const Table& table, const ColumnID column_id) {
  // distinct_set is thrown away at the end of this method, so we don't want proper heap allocations for the strings
  // stored within. The initial size of the buffer is a completely random guess, but better than zero.

  auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(table.row_count() * sizeof(ColumnDataType));
  auto distinct_set =
      std::unordered_set<ColumnDataType, std::hash<ColumnDataType>, std::equal_to<ColumnDataType>,
                         PolymorphicAllocator<ColumnDataType>>(PolymorphicAllocator<ColumnDataType>{&temp_buffer});
  distinct_set.reserve(table.row_count());

  auto null_value_count = size_t{0};

  auto min = std::numeric_limits<ColumnDataType>::max();
  auto max = std::numeric_limits<ColumnDataType>::lowest();

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_segment = table.get_chunk(chunk_id)->get_segment(column_id);

    segment_iterate<ColumnDataType>(*base_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++null_value_count;
      } else {
        distinct_set.insert(position.value());
        min = std::min(min, position.value());
        max = std::max(max, position.value());
      }
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  if (distinct_count == 0.0f) {
    min = std::numeric_limits<ColumnDataType>::min();
    max = std::numeric_limits<ColumnDataType>::max();
  }

  return std::make_shared<ColumnStatistics<ColumnDataType>>(null_value_ratio, distinct_count, min, max);
}

template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<pmr_string>(const Table& table,
                                                                             const ColumnID column_id);

}  // namespace opossum
