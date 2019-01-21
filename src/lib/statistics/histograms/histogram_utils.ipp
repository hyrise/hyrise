#include "abstract_histogram.hpp"

#include "generic_histogram.hpp"

namespace opossum {

namespace histogram {

template <typename T>
std::shared_ptr<AbstractHistogram<T>> merge_histograms(const AbstractHistogram<T>& histogram_a,
                                                       const AbstractHistogram<T>& histogram_b) {
  auto merged_minima = std::vector<T>{};
  auto merged_maxima = std::vector<T>{};
  auto merged_heights = std::vector<HistogramCountType>{};
  auto merged_distinct_counts = std::vector<HistogramCountType>{};

  const auto get_ratio_of_bin = [](const AbstractHistogram<T>& histogram, const size_t bin_idx, const T& min,
                                   const T& max) -> float {
    // For integers, 5->5 has a width of 1, for floats, a width of 0.
    if constexpr (std::is_floating_point_v<T>) {
      if (histogram.bin_minimum(bin_idx) == histogram.bin_maximum(bin_idx)) {
        return 1.0f;
      } else {
        return static_cast<float>(max - min) / static_cast<float>(histogram.bin_width(bin_idx));
      }
    } else {  // integral
      return static_cast<float>(next_value(max) - min) / static_cast<float>(histogram.bin_width(bin_idx));
    }
  };

  const auto add_merged_bin = [&](const T& min, const T& max, const auto height, const auto distinct_count) {
    if (height == 0.0f) {
      return;
    }

    DebugAssert(height >= distinct_count, "Bin height cannot be below distinct count");

    merged_minima.emplace_back(min);
    merged_maxima.emplace_back(max);
    merged_heights.emplace_back(std::ceil(height));
    merged_distinct_counts.emplace_back(std::ceil(distinct_count));
  };

  auto current_min = std::min(histogram_a.bin_minimum(BinID{0}), histogram_b.bin_minimum(BinID{0}));

  auto bin_idx_a = BinID{0};
  auto bin_idx_b = BinID{0};
  const auto bin_count_a = histogram_a.bin_count();
  const auto bin_count_b = histogram_b.bin_count();

  while (bin_idx_a < bin_count_a && bin_idx_b < bin_count_b) {
    const auto min_a = histogram_a.bin_minimum(bin_idx_a);
    const auto max_a = histogram_a.bin_maximum(bin_idx_a);
    const auto min_b = histogram_b.bin_minimum(bin_idx_b);
    const auto max_b = histogram_b.bin_maximum(bin_idx_b);

    current_min = std::max(current_min, std::min({min_a, min_b}));
    auto current_max = T{};
    auto height = float{};
    auto distinct_count = float{};

    if (current_min < min_b) {
      // Bin A only
      current_max = std::min(previous_value(min_b), max_a);

      const auto ratio = get_ratio_of_bin(histogram_a, bin_idx_a, current_min, current_max);
      height = histogram_a.bin_height(bin_idx_a) * ratio;
      distinct_count = histogram_a.bin_distinct_count(bin_idx_a) * ratio;

      add_merged_bin(current_min, current_max, height, distinct_count);

    } else if (current_min < min_a) {
      // Bin B only
      current_max = std::min(previous_value(min_a), max_b);

      const auto ratio = get_ratio_of_bin(histogram_b, bin_idx_b, current_min, current_max);
      height = histogram_b.bin_height(bin_idx_b) * ratio;
      distinct_count = histogram_b.bin_distinct_count(bin_idx_b) * ratio;

      add_merged_bin(current_min, current_max, height, distinct_count);

    } else {
      // From both
      current_max = std::min(max_a, max_b);

      const auto ratio_a = get_ratio_of_bin(histogram_a, bin_idx_a, current_min, current_max);
      const auto ratio_b = get_ratio_of_bin(histogram_b, bin_idx_b, current_min, current_max);
      height = histogram_a.bin_height(bin_idx_a) * ratio_a + histogram_b.bin_height(bin_idx_b) * ratio_b;
      distinct_count =
          histogram_a.bin_distinct_count(bin_idx_a) * ratio_a + histogram_b.bin_distinct_count(bin_idx_b) * ratio_b;

      add_merged_bin(current_min, current_max, height, distinct_count);
    }

    if (current_max == max_a) {
      ++bin_idx_a;
    }

    if (current_max == max_b) {
      ++bin_idx_b;
    }

    current_min = next_value(current_max);
  }

  for (; bin_idx_a < histogram_a.bin_count(); ++bin_idx_a) {
    current_min = std::max(current_min, histogram_a.bin_minimum(bin_idx_a));
    const auto current_max = histogram_a.bin_maximum(bin_idx_a);

    const auto ratio = get_ratio_of_bin(histogram_a, bin_idx_a, current_min, current_max);
    const auto height = histogram_a.bin_height(bin_idx_a) * ratio;
    const auto distinct_count = histogram_a.bin_distinct_count(bin_idx_a) * ratio;

    DebugAssert(height > 0, "");

    merged_minima.emplace_back(current_min);
    merged_maxima.emplace_back(current_max);
    merged_heights.emplace_back(std::ceil(height));
    merged_distinct_counts.emplace_back(std::ceil(distinct_count));
  }

  for (; bin_idx_b < histogram_b.bin_count(); ++bin_idx_b) {
    current_min = std::max(current_min, histogram_b.bin_minimum(bin_idx_b));
    const auto current_max = histogram_b.bin_maximum(bin_idx_b);

    const auto ratio = get_ratio_of_bin(histogram_b, bin_idx_b, current_min, current_max);
    const auto height = histogram_b.bin_height(bin_idx_b) * ratio;
    const auto distinct_count = histogram_b.bin_distinct_count(bin_idx_b) * ratio;

    DebugAssert(height > 0, "");

    merged_minima.emplace_back(current_min);
    merged_maxima.emplace_back(current_max);
    merged_heights.emplace_back(std::ceil(height));
    merged_distinct_counts.emplace_back(std::ceil(distinct_count));
  }

  return std::make_shared<GenericHistogram<T>>(std::move(merged_minima), std::move(merged_maxima),
                                               std::move(merged_heights), std::move(merged_distinct_counts));
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> reduce_histogram(const AbstractHistogram<T>& histogram,
                                                       const size_t max_bin_count) {
  auto reduced_minima = std::vector<T>{};
  auto reduced_maxima = std::vector<T>{};
  auto reduced_heights = std::vector<HistogramCountType>{};
  auto reduced_distinct_counts = std::vector<HistogramCountType>{};

  // Number of consecutive bins to merge into one
  const auto reduce_factor = static_cast<BinID>(std::ceil(static_cast<float>(histogram.bin_count()) / max_bin_count));

  if (reduce_factor <= 1) return histogram.clone();

  for (auto bin_idx = BinID{0}; bin_idx < histogram.bin_count(); bin_idx += reduce_factor) {
    const auto first_bin_idx = bin_idx;
    const auto last_bin_idx = std::min(bin_idx + reduce_factor - 1, histogram.bin_count() - 1);

    auto height = HistogramCountType{0};
    auto distinct_count = HistogramCountType{0};

    for (auto merge_bin_idx = bin_idx; merge_bin_idx <= last_bin_idx; ++merge_bin_idx) {
      height += histogram.bin_height(merge_bin_idx);
      distinct_count += histogram.bin_distinct_count(merge_bin_idx);
    }

    reduced_minima.emplace_back(histogram.bin_minimum(first_bin_idx));
    reduced_maxima.emplace_back(histogram.bin_maximum(last_bin_idx));
    reduced_heights.emplace_back(height);
    reduced_distinct_counts.emplace_back(distinct_count);
  }
  return std::make_shared<GenericHistogram<T>>(std::move(reduced_minima), std::move(reduced_maxima),
                                               std::move(reduced_heights), std::move(reduced_distinct_counts));
}

}  // namespace histogram

}  // namespace opossum