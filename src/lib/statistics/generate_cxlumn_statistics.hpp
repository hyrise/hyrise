#pragma once

#include <unordered_set>

#include "base_cxlumn_statistics.hpp"
#include "cxlumn_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Generate the statistics of a single column. Used by generate_table_statistics()
 */
template <typename CxlumnDataType>
std::shared_ptr<BaseCxlumnStatistics> generate_cxlumn_statistics(const Table& table, const CxlumnID cxlumn_id) {
  std::unordered_set<CxlumnDataType> distinct_set;

  auto null_value_count = size_t{0};

  auto min = std::numeric_limits<CxlumnDataType>::max();
  auto max = std::numeric_limits<CxlumnDataType>::lowest();

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_segment = table.get_chunk(chunk_id)->get_segment(cxlumn_id);

    resolve_cxlumn_type<CxlumnDataType>(*base_segment, [&](auto& column) {
      auto iterable = create_iterable_from_column<CxlumnDataType>(column);
      iterable.for_each([&](const auto& column_value) {
        if (column_value.is_null()) {
          ++null_value_count;
        } else {
          distinct_set.insert(column_value.value());
          min = std::min(min, column_value.value());
          max = std::max(max, column_value.value());
        }
      });
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  if (distinct_count == 0.0f) {
    min = std::numeric_limits<CxlumnDataType>::min();
    max = std::numeric_limits<CxlumnDataType>::max();
  }

  return std::make_shared<CxlumnStatistics<CxlumnDataType>>(null_value_ratio, distinct_count, min, max);
}

template <>
std::shared_ptr<BaseCxlumnStatistics> generate_cxlumn_statistics<std::string>(const Table& table,
                                                                              const CxlumnID cxlumn_id);

}  // namespace opossum
