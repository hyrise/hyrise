#pragma once

#include <iostream>
#include <memory>

#include "base_attribute_statistics.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractStatisticsObject;

/**
 * For documentation, see BaseAttributeStatistics.
 */
template <typename T>
class AttributeStatistics : public BaseAttributeStatistics,
                            public std::enable_shared_from_this<AttributeStatistics<T>> {
 public:
  AttributeStatistics();

  void set_statistics_object(const std::shared_ptr<const AbstractStatisticsObject>& statistics_object) override;

  std::shared_ptr<const BaseAttributeStatistics> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<const BaseAttributeStatistics> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<const BaseAttributeStatistics> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<const AbstractHistogram<T>> histogram;
  std::shared_ptr<const MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<const RangeFilter<T>> range_filter;
  std::shared_ptr<const NullValueRatioStatistics> null_value_ratio;
  std::shared_ptr<const DistinctValueCount> distinct_value_count;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const AttributeStatistics<T>& attribute_statistics) {
  stream << "{\n";

  if (attribute_statistics.histogram) {
    stream << attribute_statistics.histogram->description() << '\n';
  }

  if (attribute_statistics.min_max_filter) {
    stream << "MinMaxFilter: " << *attribute_statistics.min_max_filter << '\n';
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (attribute_statistics.range_filter) {
      stream << "RangeFilter: " << *attribute_statistics.range_filter << '\n';
    }
  }

  if (attribute_statistics.null_value_ratio) {
    stream << "NullValueRatio: " << attribute_statistics.null_value_ratio->ratio << '\n';
  }

  if (attribute_statistics.distinct_value_count) {
    stream << "DistinctValueCount: " << attribute_statistics.distinct_value_count->count << '\n';
  }

  stream << "}\n";

  return stream;
}

EXPLICITLY_DECLARE_DATA_TYPES(AttributeStatistics);

}  // namespace hyrise
