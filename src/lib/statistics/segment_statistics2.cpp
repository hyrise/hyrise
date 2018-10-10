#include "segment_statistics2.hpp"

#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"

namespace opossum {

template <typename T>
void SegmentStatistics2<T>::set_statistics_object(
  const std::shared_ptr<AbstractHistogram<T>>& statistics_object) {
  switch (statistics_object->histogram_type()) {
    case HistogramType::EqualDistinctCount:
      equal_distinct_count_histogram = std::static_pointer_cast<EqualDistinctCountHistogram<T>>(statistics_object);
      break;
    case HistogramType::Generic:
      generic_histogram = std::static_pointer_cast<GenericHistogram<T>>(statistics_object);
      break;
    default:
      Fail("Statistics object type not yet supported.");
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SegmentStatistics2);

}  // namespace opossum
