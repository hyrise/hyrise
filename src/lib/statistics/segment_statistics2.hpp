#pragma once

#include <iostream>
#include <memory>

#include "base_segment_statistics2.hpp"
#include "histograms/equal_distinct_count_histogram.hpp"
#include "histograms/generic_histogram.hpp"
#include "histograms/single_bin_histogram.hpp"
#include "selectivity.hpp"
#include "statistics_objects/null_value_ratio.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class AbstractStatisticsObject;
template <typename T>
class MinMaxFilter;
template <typename T>
class RangeFilter;

template <typename T>
class SegmentStatistics2 : public BaseSegmentStatistics2 {
 public:
  SegmentStatistics2();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;
  std::shared_ptr<BaseSegmentStatistics2> scaled(const Selectivity selectivity) const override;
  std::shared_ptr<BaseSegmentStatistics2> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractHistogram<T>> histogram;
  std::shared_ptr<MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<RangeFilter<T>> range_filter;
  std::shared_ptr<NullValueRatio> null_value_ratio;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const SegmentStatistics2<T>& segment_statistics) {
  stream << "{";

  if (segment_statistics.histogram) {
    stream << segment_statistics.histogram->description(true) << std::endl;
  }

  if (segment_statistics.min_max_filter) {
    stream << "MinMaxFilter" << std::endl;
  }

  if (segment_statistics.range_filter) {
    stream << "RangeFilter" << std::endl;
  }
  if (segment_statistics.null_value_ratio) {
    stream << "NullValueRatio: " << segment_statistics.null_value_ratio->null_value_ratio << std::endl;
  }

  stream << "}";

  return stream;
}

}  // namespace opossum
