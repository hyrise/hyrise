#pragma once

#include "chunk_statistics/histograms/abstract_histogram.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
Cardinality CardinalityEstimator::estimate_cardinality_of_inner_equi_join_with_numeric_histograms(
    const std::shared_ptr<AbstractHistogram<T>>& left_histogram,
    const std::shared_ptr<AbstractHistogram<T>>& right_histogram) {
  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto total_cardinality = Cardinality{0.0f};
  auto left_bin_count = left_histogram->bin_count();
  auto right_bin_count = right_histogram->bin_count();

  for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
    const auto left_min = left_histogram->bin_minimum(left_idx);
    const auto right_min = right_histogram->bin_minimum(right_idx);

    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(left_histogram->bin_maximum(left_idx) == right_histogram->bin_maximum(right_idx),
                "Histogram bin boundaries do not match");

    const auto distinct_max = static_cast<float>(
        std::max(left_histogram->bin_distinct_count(left_idx), right_histogram->bin_distinct_count(right_idx)));

    const auto value_count_product = left_histogram->bin_height(left_idx) * right_histogram->bin_height(right_idx);

    total_cardinality += value_count_product / distinct_max;

    ++left_idx;
    ++right_idx;
  }

  return total_cardinality;
}

}  // namespace opossum
