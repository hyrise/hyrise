#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseCxlumnStatistics;

// Result of a cardinality estimation of filtering by value
struct FilterByValueEstimate final {
  float selectivity{0.0f};
  std::shared_ptr<BaseCxlumnStatistics> cxlumn_statistics;
};

// Result of a cardinality estimation of filtering by comparing two columns
struct FilterByColumnComparisonEstimate {
  float selectivity{0.0f};
  std::shared_ptr<BaseCxlumnStatistics> left_cxlumn_statistics;
  std::shared_ptr<BaseCxlumnStatistics> right_cxlumn_statistics;
};

class BaseCxlumnStatistics {
 public:
  BaseCxlumnStatistics(const DataType data_type, const float null_value_ratio, const float distinct_count);
  virtual ~BaseCxlumnStatistics() = default;

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
   * @return a clone of the concrete CxlumnStatistics object
   */
  virtual std::shared_ptr<BaseCxlumnStatistics> clone() const = 0;

  /**
   * @return a clone() of this, with the null_value_ratio set to 0
   */
  std::shared_ptr<BaseCxlumnStatistics> without_null_values() const;

  /**
   * @defgroup Cardinality estimation
   * @{
   */
  /**
   * Estimate a Column-Value Predicate, e.g. "a > 5"
   */
  virtual FilterByValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-ValuePlaceholder Predicate, e.g. "a > ?"
   * Since the value of the ValuePlaceholder (naturally) isn't known, has to resort to magic values.
   */
  virtual FilterByValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-Column Predicate, e.g. "a > b"
   */
  virtual FilterByColumnComparisonEstimate estimate_predicate_with_column(
      const PredicateCondition predicate_condition, const BaseCxlumnStatistics& base_right_cxlumn_statistics) const = 0;
  /** @} */

  virtual std::string description() const = 0;

 protected:
  const DataType _data_type;
  float _null_value_ratio;
  float _distinct_count;
};

}  // namespace opossum
