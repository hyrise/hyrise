#pragma once

#include "chunk_statistics/histograms/abstract_histogram.hpp"
#include "chunk_statistics/histograms/generic_histogram.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
std::shared_ptr<GenericHistogram<T>> CardinalityEstimator::estimate_cardinality_of_inner_equi_join_with_arithmetic_histograms(
    const std::shared_ptr<AbstractHistogram<T>>& left_histogram,
    const std::shared_ptr<AbstractHistogram<T>>& right_histogram) {
  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = left_histogram->bin_count();
  auto right_bin_count = right_histogram->bin_count();

  std::vector<T> bin_minima;
  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_heights;
  std::vector<HistogramCountType> bin_distinct_counts;

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

    const auto [distinct_min, distinct_max] =
        std::minmax(left_histogram->bin_distinct_count(left_idx), right_histogram->bin_distinct_count(right_idx));
    const auto value_count_product = left_histogram->bin_height(left_idx) * right_histogram->bin_height(right_idx);

    bin_minima.emplace_back(left_min);
    bin_maxima.emplace_back(left_histogram->bin_maximum(left_idx));
    bin_heights.emplace_back(std::ceil(value_count_product / static_cast<float>(distinct_max)));
    bin_distinct_counts.emplace_back(distinct_min);

    ++left_idx;
    ++right_idx;
  }

  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), std::move(bin_distinct_counts));
}

}  // namespace opossum
