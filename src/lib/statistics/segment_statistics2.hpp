#pragma once

#include <memory>

#include "base_segment_statistics2.hpp"
#include "selectivity.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class AbstractStatisticsObject;
template <typename T>
class EqualDistinctCountHistogram;
template <typename T>
class GenericHistogram;
template <typename T>
class SingleBinHistogram;
template <typename T>
class MinMaxFilter;
template <typename T>
class RangeFilter;

template <typename T>
class SegmentStatistics2 : public BaseSegmentStatistics2 {
 public:
  SegmentStatistics2();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;
  std::shared_ptr<BaseSegmentStatistics2> scaled_with_selectivity(const Selectivity selectivity) const override;
  std::shared_ptr<BaseSegmentStatistics2> sliced_with_predicate(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractHistogram<T>> get_best_available_histogram() const;

  std::shared_ptr<EqualDistinctCountHistogram<T>> equal_distinct_count_histogram;
  std::shared_ptr<GenericHistogram<T>> generic_histogram;
  std::shared_ptr<SingleBinHistogram<T>> single_bin_histogram;
  std::shared_ptr<MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<RangeFilter<T>> range_filter;
};

}  // namespace opossum
