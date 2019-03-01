#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "base_column_statistics.hpp"
#include "resolve_type.hpp"

namespace opossum {

/**
 * @tparam ColumnDataType   the DataType of the values in the Column that these statistics represent
 */
template <typename ColumnDataType>
class ColumnStatistics : public BaseColumnStatistics {
 public:
  ColumnStatistics(const float null_value_ratio, const float distinct_count, const ColumnDataType min,
                   const ColumnDataType max)
      : BaseColumnStatistics(data_type_from_type<ColumnDataType>(), null_value_ratio, distinct_count),
        _min(min),
        _max(max) {
    Assert(null_value_ratio >= 0.0f && null_value_ratio <= 1.0f, "NullValueRatio out of range");
  }

  // To be used for columns for which ColumnStatistics can't be computed
  static ColumnStatistics dummy() {
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      return ColumnStatistics{1.0f, 1.0f, {}, {}};
    } else {
      return ColumnStatistics{1.0f, 1.0f, {0}, {0}};
    }
  }

  /**
   * @defgroup Member access
   * @{
   */
  ColumnDataType min() const;
  ColumnDataType max() const;
  /** @} */

  /**
   * @defgroup Implementations for BaseColumnStatistics
   * @{
   */
  std::shared_ptr<BaseColumnStatistics> clone() const override;
  FilterByValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  FilterByValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  FilterByColumnComparisonEstimate estimate_predicate_with_column(
      const PredicateCondition predicate_condition,
      const BaseColumnStatistics& base_right_column_statistics) const override;

  std::string description() const override;
  /** @} */

  /**
   * @defgroup Cardinality Estimation helpers
   * @{
   */

  /**
   * @return the ratio of rows of this Column that are in the range [minimum, maximum]
   */
  float estimate_range_selectivity(const ColumnDataType minimum, const ColumnDataType maximum) const;

  /**
   * @return estimate the predicate `column BETWEEN minimum AND maximum`
   */
  FilterByValueEstimate estimate_range(const ColumnDataType minimum, const ColumnDataType maximum) const;

  /**
   * @return estimate the predicate `column = value`
   */
  FilterByValueEstimate estimate_equals_with_value(const ColumnDataType value) const;

  /**
   * @return estimate the predicate `column != value`
   */
  FilterByValueEstimate estimate_not_equals_with_value(const ColumnDataType value) const;
  /** @} */

 private:
  ColumnDataType _min;
  ColumnDataType _max;
};

}  // namespace opossum
