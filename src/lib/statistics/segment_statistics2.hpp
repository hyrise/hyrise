#pragma once

#include <memory>

#include "base_segment_statistics2.hpp"

namespace opossum {

template<typename T> class AbstractHistogram;
template<typename T> class EqualDistinctCountHistogram;
template<typename T> class GenericHistogram;

template<typename T>
class SegmentStatistics2 : public BaseSegmentStatistics2 {
 public:
  void set_statistics_object(const std::shared_ptr<AbstractHistogram<T>>& statistics_object);

  std::shared_ptr<EqualDistinctCountHistogram<T>> equal_distinct_count_histogram;
  std::shared_ptr<GenericHistogram<T>> generic_histogram;
};

}  // namespace opossum
