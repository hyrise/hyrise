#include "vertical_statistics_slice.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/single_bin_histogram.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

namespace opossum {

template <typename T>
VerticalStatisticsSlice<T>::VerticalStatisticsSlice() : BaseVerticalStatisticsSlice(data_type_from_type<T>()) {}

template <typename T>
void VerticalStatisticsSlice<T>::set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) {
  if (const auto histogram_object = std::dynamic_pointer_cast<AbstractHistogram<T>>(statistics_object)) {
    histogram = histogram_object;
  } else if (const auto min_max_object = std::dynamic_pointer_cast<MinMaxFilter<T>>(statistics_object)) {
    min_max_filter = min_max_object;
  } else if (std::dynamic_pointer_cast<EmptyStatisticsObject>(statistics_object)) {
    // EmptyStatisticsObjects are simply dropped
  } else if (const auto null_value_ratio_object = std::dynamic_pointer_cast<NullValueRatio>(statistics_object)) {
    null_value_ratio = null_value_ratio_object;
  } else {
    if constexpr (std::is_arithmetic_v<T>) {
      if (const auto range_object = std::dynamic_pointer_cast<RangeFilter<T>>(statistics_object)) {
        range_filter = range_object;
        return;
      }
    }

    Fail("Statistics object type not yet supported.");
  }
}

template <typename T>
bool VerticalStatisticsSlice<T>::does_not_contain(const PredicateCondition predicate_type,
                                             const AllTypeVariant& variant_value,
                                             const std::optional<AllTypeVariant>& variant_value2) const {
  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      const auto estimate = range_filter->estimate_cardinality(predicate_type, variant_value, variant_value2);
      if (estimate.type == EstimateType::MatchesNone) return true;
    }
  }

  if (min_max_filter) {
    const auto estimate = min_max_filter->estimate_cardinality(predicate_type, variant_value, variant_value2);
    if (estimate.type == EstimateType::MatchesNone) return true;
  }

  return false;
}

template <typename T>
std::shared_ptr<BaseVerticalStatisticsSlice> VerticalStatisticsSlice<T>::scaled(const Selectivity selectivity) const {
  const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<T>>();

  if (histogram) {
    vertical_slices->set_statistics_object(histogram->scaled(selectivity));
  }

  if (null_value_ratio) {
    vertical_slices->set_statistics_object(null_value_ratio->scaled(selectivity));
  }

  return vertical_slices;
}

template <typename T>
std::shared_ptr<BaseVerticalStatisticsSlice> VerticalStatisticsSlice<T>::sliced(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<T>>();

  if (histogram) {
    vertical_slices->set_statistics_object(histogram->sliced(predicate_type, variant_value, variant_value2));
  }
  if (null_value_ratio) {
    vertical_slices->set_statistics_object(null_value_ratio->sliced(predicate_type, variant_value, variant_value2));
  }

  return vertical_slices;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(VerticalStatisticsSlice);

}  // namespace opossum
