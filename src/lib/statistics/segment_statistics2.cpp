#include "segment_statistics2.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics/histograms/single_bin_histogram.hpp"
#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/chunk_statistics/range_filter.hpp"
#include "statistics/empty_statistics_object.hpp"

namespace opossum {

template <typename T>
SegmentStatistics2<T>::SegmentStatistics2() : BaseSegmentStatistics2(data_type_from_type<T>()) {}

template <typename T>
void SegmentStatistics2<T>::set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) {
  if (const auto histogram_object = std::dynamic_pointer_cast<AbstractHistogram<T>>(statistics_object)) {
    switch (histogram_object->histogram_type()) {
      case HistogramType::EqualDistinctCount:
        equal_distinct_count_histogram = std::static_pointer_cast<EqualDistinctCountHistogram<T>>(histogram_object);
        break;
      case HistogramType::EqualWidth:
        equal_width_histogram = std::static_pointer_cast<EqualWidthHistogram<T>>(histogram_object);
        break;
      case HistogramType::Generic:
        generic_histogram = std::static_pointer_cast<GenericHistogram<T>>(histogram_object);
        break;
      case HistogramType::SingleBin:
        single_bin_histogram = std::static_pointer_cast<SingleBinHistogram<T>>(histogram_object);
        break;
      default:
        Fail("Histogram type not yet supported.");
    }
  } else if (const auto min_max_object = std::dynamic_pointer_cast<MinMaxFilter<T>>(statistics_object)) {
    min_max_filter = min_max_object;
  } else if (std::dynamic_pointer_cast<EmptyStatisticsObject>(statistics_object)) {
    // EmptyStatisticsObjects are simply dropped
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
bool SegmentStatistics2<T>::does_not_contain(const PredicateCondition predicate_type,
                                             const AllTypeVariant& variant_value,
                                             const std::optional<AllTypeVariant>& variant_value2) const {
  //  if (equal_distinct_count_histogram) {
  //    const auto estimate = equal_distinct_count_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
  //    if (estimate.type == EstimateType::MatchesNone) return true;
  //  }
  //
  //  if (equal_width_histogram) {
  //    const auto estimate = equal_width_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
  //    if (estimate.type == EstimateType::MatchesNone) return true;
  //  }
  //
  //  if (generic_histogram) {
  //    const auto estimate = generic_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
  //    if (estimate.type == EstimateType::MatchesNone) return true;
  //  }
  //
  //  if (single_bin_histogram) {
  //    const auto estimate = generic_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
  //    if (estimate.type == EstimateType::MatchesNone) return true;
  //  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter && range_filter->is_derived_from_complete_chunk) {
      const auto estimate = range_filter->estimate_cardinality(predicate_type, variant_value, variant_value2);
      if (estimate.type == EstimateType::MatchesNone) return true;
    }
  }

  if (min_max_filter && min_max_filter->is_derived_from_complete_chunk) {
    const auto estimate = min_max_filter->estimate_cardinality(predicate_type, variant_value, variant_value2);
    if (estimate.type == EstimateType::MatchesNone) return true;
  }

  return false;
}

template <typename T>
std::shared_ptr<BaseSegmentStatistics2> SegmentStatistics2<T>::scale_with_selectivity(
    const Selectivity selectivity) const {
  const auto segment_statistics = std::make_shared<SegmentStatistics2<T>>();

  if (generic_histogram) {
    segment_statistics->set_statistics_object(generic_histogram->scale_with_selectivity(selectivity));
  }

  if (equal_width_histogram) {
    segment_statistics->set_statistics_object(equal_width_histogram->scale_with_selectivity(selectivity));
  }

  if (equal_distinct_count_histogram) {
    segment_statistics->set_statistics_object(equal_distinct_count_histogram->scale_with_selectivity(selectivity));
  }

  if (single_bin_histogram) {
    segment_statistics->set_statistics_object(single_bin_histogram->scale_with_selectivity(selectivity));
  }

  return segment_statistics;
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> SegmentStatistics2<T>::get_best_available_histogram() const {
  if (equal_distinct_count_histogram) {
    return equal_distinct_count_histogram;
  } else if (equal_width_histogram) {
    return equal_width_histogram;
  } else if (generic_histogram) {
    return generic_histogram;
  } else if (single_bin_histogram) {
    return single_bin_histogram;
  } else {
    return nullptr;
  }
}

template <typename T>
std::shared_ptr<BaseSegmentStatistics2> SegmentStatistics2<T>::slice_with_predicate(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto segment_statistics = std::make_shared<SegmentStatistics2<T>>();

  if (generic_histogram) {
    segment_statistics->set_statistics_object(
        generic_histogram->slice_with_predicate(predicate_type, variant_value, variant_value2));
  }

  if (equal_width_histogram) {
    segment_statistics->set_statistics_object(
        equal_width_histogram->slice_with_predicate(predicate_type, variant_value, variant_value2));
  }

  if (equal_distinct_count_histogram) {
    segment_statistics->set_statistics_object(
        equal_distinct_count_histogram->slice_with_predicate(predicate_type, variant_value, variant_value2));
  }

  if (single_bin_histogram) {
    segment_statistics->set_statistics_object(
        single_bin_histogram->slice_with_predicate(predicate_type, variant_value, variant_value2));
  }

  return segment_statistics;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SegmentStatistics2);

}  // namespace opossum
