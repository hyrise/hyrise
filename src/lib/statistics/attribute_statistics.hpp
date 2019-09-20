#pragma once

#include <iostream>
#include <memory>

#include "base_attribute_statistics.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class AbstractStatisticsObject;
template <typename T>
class MinMaxFilter;
template <typename T>
class RangeFilter;
template <typename T>
class CountingQuotientFilter;

/**
 * For docs, see BaseAttributeStatistics
 */
template <typename T>
class AttributeStatistics : public BaseAttributeStatistics {
 public:
  AttributeStatistics();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;

  std::shared_ptr<BaseAttributeStatistics> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<BaseAttributeStatistics> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<BaseAttributeStatistics> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractHistogram<T>> histogram;
  std::shared_ptr<MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<RangeFilter<T>> range_filter;
  std::shared_ptr<CountingQuotientFilter<T>> counting_quotient_filter;
  std::shared_ptr<NullValueRatioStatistics> null_value_ratio;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const AttributeStatistics<T>& attribute_statistics) {
  stream << "{" << std::endl;

  if (attribute_statistics.histogram) {
    stream << attribute_statistics.histogram->description() << std::endl;
  }

  if (attribute_statistics.min_max_filter) {
    // TODO(anybody) implement printing of MinMaxFilter if ever required
    stream << "Has MinMaxFilter" << std::endl;
  }

  if (attribute_statistics.range_filter) {
    // TODO(anybody) implement printing of RangeFilter if ever required
    stream << "Has RangeFilter" << std::endl;
  }

  if (attribute_statistics.counting_quotient_filter) {
    // TODO(anybody) implement printing of CQD if ever required
    stream << "Has CQF" << std::endl;
  }

  if (attribute_statistics.null_value_ratio) {
    stream << "NullValueRatio: " << attribute_statistics.null_value_ratio->ratio << std::endl;
  }

  stream << "}" << std::endl;

  return stream;
}

}  // namespace opossum
