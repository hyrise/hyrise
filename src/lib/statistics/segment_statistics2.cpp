#include "segment_statistics2.hpp"

#include <memory>

#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"

namespace opossum {

template <typename T>
void SegmentStatistics2<T>::set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) {
  const auto histogram_object = std::dynamic_pointer_cast<AbstractHistogram<T>>(statistics_object);
  Assert(histogram_object, "Can only handle histograms for now.");

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
    default:
      Fail("Statistics object type not yet supported.");
  }
}

template <typename T>
bool SegmentStatistics2<T>::does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                      const std::optional<AllTypeVariant>& variant_value2) const {
  if (equal_distinct_count_histogram) {
    const auto estimate = equal_distinct_count_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
    if (estimate.type == EstimateType::MatchesNone) return true;
  }

  if (equal_width_histogram) {
    const auto estimate = equal_width_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
    if (estimate.type == EstimateType::MatchesNone) return true;
  }

  if (generic_histogram) {
    const auto estimate = generic_histogram->estimate_cardinality(predicate_type, variant_value, variant_value2);
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

  return segment_statistics;
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

  return segment_statistics;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SegmentStatistics2);

}  // namespace opossum
