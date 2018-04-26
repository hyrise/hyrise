#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractColumnStatistics;

struct ColumnValueEstimate {
  float selectivity{0.0f};
  std::shared_ptr<AbstractColumnStatistics> column_statistics;
};

struct ColumnColumnEstimate {
  float selectivity{0.0f};
  std::shared_ptr<AbstractColumnStatistics> left_column_statistics;
  std::shared_ptr<AbstractColumnStatistics> right_column_statistics;
};

class AbstractColumnStatistics {
 public:
  AbstractColumnStatistics(const DataType data_type, const float null_value_ratio, const float distinct_count);
  virtual ~AbstractColumnStatistics() = default;

  /**
   * @defgroup Member access
   * @{
   */
  DataType data_type() const;
  float null_value_ratio() const;
  float non_null_value_ratio() const;
  float distinct_count() const;

  void set_null_value_ratio(const float null_value_ratio);
  /** @} */

  /**
   * @return a clone of the concrete ColumnStatistics object
   */
  virtual std::shared_ptr<AbstractColumnStatistics> clone() const = 0;

  /**
   * @return a clone() of this, with the null_value_ratio set to 0
   */
  std::shared_ptr<AbstractColumnStatistics> without_null_values() const;

  /**
   * @defgroup Cardinality estimation
   * @{
   */
  /**
   * Estimate a Column-Value Predicate, e.g. "a > 5"
   */
  virtual ColumnValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-ValuePlacehoder Predicate, e.g. "a > ?"
   * Since the value of the ValuePlaceholder (naturally) isn't known, has to resort to magic values.
   */
  virtual ColumnValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition, const ValuePlaceholder& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-Column Predicate, e.g. "a > b"
   */
  virtual ColumnColumnEstimate estimate_predicate_with_column(
      const PredicateCondition predicate_condition, const AbstractColumnStatistics& right_column_statistics) const = 0;
  /** @} */

  virtual std::string description() const = 0;

 protected:
  const DataType _data_type;
  float _null_value_ratio;
  float _distinct_count;
};

}  // namespace opossum
