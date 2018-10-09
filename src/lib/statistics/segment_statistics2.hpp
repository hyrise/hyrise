#pragma once

#include <memory>

namespace opossum {

class EqualDistinctCountHistogram;

class SegmentStatistics2 {
 public:
  std::shared_ptr<EqualDistinctCountHistogram> equal_distinct_count_histogram;
};

}  // namespace opossum
