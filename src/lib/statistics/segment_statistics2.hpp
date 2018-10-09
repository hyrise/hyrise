#pragma once

#include <memory>

#include "base_segment_statistics2.hpp"

namespace opossum {

template<typename T> class EqualDistinctCountHistogram;

template<typename T>
class SegmentStatistics2 : public BaseSegmentStatistics2 {
 public:
  std::shared_ptr<EqualDistinctCountHistogram<T>> equal_distinct_count_histogram;
};

}  // namespace opossum
