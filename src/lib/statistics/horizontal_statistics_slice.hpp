#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "cardinality.hpp"
#include "types.hpp"

namespace opossum {

class BaseVerticalStatisticsSlice;

class HorizontalStatisticsSlice {
 public:
  HorizontalStatisticsSlice() = default;
  explicit HorizontalStatisticsSlice(const Cardinality row_count);

  /**
   * Tries to determine a columns null value ratio, either by retrieving it from the NullValueRatio statistics object,
   * or by combining another statistics object with this chunk's row_count
   */
  std::optional<float> estimate_column_null_value_ratio(const ColumnID column_id) const;

  Cardinality row_count{0};
  std::vector<std::shared_ptr<BaseVerticalStatisticsSlice>> vertical_slices;
};

std::ostream& operator<<(std::ostream& stream, const HorizontalStatisticsSlice& chunk_statistics);

}  // namespace opossum
