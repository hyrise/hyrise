#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "abstract_column_statistics2.hpp"

namespace opossum {

/**
 * @tparam ColumnDataType   the DataType of the values in the Column that these statistics represent
 */
template <typename ColumnDataType>
class ColumnStatistics2 : public AbstractColumnStatistics2 {
 public:
  ColumnStatistics2(const float null_value_ratio,
                    const float distinct_count,
                    const ColumnDataType min,
                    const ColumnDataType max);

  /**
   * @defgroup Member access
   * @{
   */
  ColumnDataType min() const;
  ColumnDataType max() const;
  /** @} */

  /**
   * @defgroup Implementations for AbstractColumnStatistics2
   * @{
   */
  std::shared_ptr<AbstractColumnStatistics2> clone() const override;
  ColumnValueEstimate estimate_predicate_with_value(
    const PredicateCondition predicate_condition,
    const AllTypeVariant& value,
    const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  ColumnValueEstimate estimate_predicate_with_value_placeholder(
    const PredicateCondition predicate_condition,
    const ValuePlaceholder& value,
    const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  ColumnColumnEstimate estimate_predicate_with_column(
    const PredicateCondition predicate_condition,
    const AbstractColumnStatistics2& right_column_statistics) const override;

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
  ColumnValueEstimate estimate_range(const ColumnDataType minimum, const ColumnDataType maximum) const;

  /**
   * @return estimate the predicate `column = value`
   */
  ColumnValueEstimate estimate_equals_with_value(const ColumnDataType value) const;

  /**
   * @return estimate the predicate `column != value`
   */
  ColumnValueEstimate estimate_not_equals_with_value(const ColumnDataType value) const;
  /** @} */



 private:
  ColumnDataType _min;
  ColumnDataType _max;
};

}  // namespace opossum
