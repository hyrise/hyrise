#pragma once

#include <memory>

#include "statistics/statistics_objects/scaled_histogram.hpp"

namespace hyrise {

class ScaledHistogramTestUtils {
 public:
  template <typename T>
  static const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram(
      const ScaledHistogram<T>& scaled_histogram) {
    return scaled_histogram._referenced_histogram;
  }

  template <typename T>
  static Selectivity selectivity(const ScaledHistogram<T>& scaled_histogram) {
    return scaled_histogram._selectivity;
  }
};
}  // namespace hyrise
