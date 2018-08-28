#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "base_cxlumn_statistics.hpp"

namespace opossum {

/**
 * @tparam CxlumnDataType   the DataType of the values in the Cxlumn that these statistics represent
 */
template <typename CxlumnDataType>
class CxlumnStatistics : public BaseCxlumnStatistics {
 public:
  // To be used for cxlumns for which CxlumnStatistics can't be computed
  static CxlumnStatistics dummy();

  CxlumnStatistics(const float null_value_ratio, const float distinct_count, const CxlumnDataType min,
                   const CxlumnDataType max);

  /**
   * @defgroup Member access
   * @{
   */
  CxlumnDataType min() const;
  CxlumnDataType max() const;
  /** @} */

  /**
   * @defgroup Implementations for BaseCxlumnStatistics
   * @{
   */
  std::shared_ptr<BaseCxlumnStatistics> clone() const override;
  FilterByValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  FilterByValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const override;

  FilterByCxlumnComparisonEstimate estimate_predicate_with_cxlumn(
      const PredicateCondition predicate_condition,
      const BaseCxlumnStatistics& base_right_cxlumn_statistics) const override;

  std::string description() const override;
  /** @} */

  /**
   * @defgroup Cardinality Estimation helpers
   * @{
   */

  /**
   * @return the ratio of rows of this Cxlumn that are in the range [minimum, maximum]
   */
  float estimate_range_selectivity(const CxlumnDataType minimum, const CxlumnDataType maximum) const;

  /**
   * @return estimate the predicate `cxlumn BETWEEN minimum AND maximum`
   */
  FilterByValueEstimate estimate_range(const CxlumnDataType minimum, const CxlumnDataType maximum) const;

  /**
   * @return estimate the predicate `cxlumn = value`
   */
  FilterByValueEstimate estimate_equals_with_value(const CxlumnDataType value) const;

  /**
   * @return estimate the predicate `cxlumn != value`
   */
  FilterByValueEstimate estimate_not_equals_with_value(const CxlumnDataType value) const;
  /** @} */

 private:
  CxlumnDataType _min;
  CxlumnDataType _max;
};

}  // namespace opossum
