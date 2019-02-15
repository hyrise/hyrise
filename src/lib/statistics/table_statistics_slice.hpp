#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "cardinality.hpp"
#include "types.hpp"

namespace opossum {

class BaseSegmentStatistics2;

class TableStatisticsSlice {
 public:
  TableStatisticsSlice() = default;
  explicit TableStatisticsSlice(const Cardinality row_count);

  /**
   * Tries to determine a columns null value ratio, either by retrieving it from the NullValueRatio statistics object,
   * or by combining another statistics object with this chunk's row_count
   */
  std::optional<float> estimate_column_null_value_ratio(const ColumnID column_id) const;

  Cardinality row_count{0};
  ChunkOffset approx_invalid_row_count{0};
  std::vector<std::shared_ptr<BaseSegmentStatistics2>> segment_statistics;
};

std::ostream& operator<<(std::ostream& stream, const TableStatisticsSlice& chunk_statistics);

}  // namespace opossum
