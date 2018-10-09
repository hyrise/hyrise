#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "base_column_statistics.hpp"
#include "chunk_statistics/histograms/abstract_histogram.hpp"

namespace opossum {

/**
 * @tparam ColumnDataType   the DataType of the values in the Column that these statistics represent
 */
template <typename ColumnDataType>
class HistogramColumnStatistics : public BaseColumnStatistics {
 public:
  HistogramColumnStatistics(const std::shared_ptr<AbstractHistogram<ColumnDataType>>& histogram,
                            const float null_value_ratio);

  /**
   * @defgroup Member access
   * @{
   */
  const std::shared_ptr<const AbstractHistogram<ColumnDataType>> histogram() const;
  /** @} */

  /**
   * @defgroup Implementations for BaseColumnStatistics
   * @{
   */
  std::shared_ptr<BaseColumnStatistics> clone() const override;
  FilterByValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  FilterByValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  FilterByColumnComparisonEstimate estimate_predicate_with_column(
      const PredicateCondition predicate_condition,
      const std::shared_ptr<const BaseColumnStatistics>& right_column_statistics) const override;

  float estimate_range_selectivity(const AllTypeVariant& variant_minimum,
                                   const AllTypeVariant& variant_maximum) const override;

  AllTypeVariant min() const override;
  AllTypeVariant max() const override;
  float distinct_count() const override;

  /** @} */

  /**
   * @defgroup Cardinality Estimation helpers
   * @{
   */

  /**
   * @return estimate the predicate `column = value`
   */
  FilterByValueEstimate estimate_equals(const float selectivity, const bool can_prune, const ColumnDataType value,
                                        const bool update_min_max = true) const;

  /**
   * @return estimate the predicate `column != value`
   */
  FilterByValueEstimate estimate_not_equals(const float selectivity, const bool can_prune, const ColumnDataType value,
                                            const bool update_min_max = true) const;

  /**
   * @return estimate the predicate `column BETWEEN minimum AND maximum`
   */
  FilterByValueEstimate estimate_range(const AllTypeVariant& variant_minimum,
                                       const AllTypeVariant& variant_maximum) const override;
  FilterByValueEstimate estimate_range(const float selectivity, const bool can_prune, const ColumnDataType min,
                                       const ColumnDataType max) const;

  float estimate_distinct_count(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                const std::optional<AllTypeVariant>& variant_value2) const override;
  /** @} */

 protected:
  std::string _description() const override;

 private:
  const std::shared_ptr<AbstractHistogram<ColumnDataType>> _histogram;
};

}  // namespace opossum
