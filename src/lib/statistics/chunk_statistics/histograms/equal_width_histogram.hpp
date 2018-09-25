#pragma once

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "histogram_utils.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualWidthBinStats {
  T min;
  T max;
  std::vector<size_t> counts;
  std::vector<size_t> distinct_counts;
  size_t bin_count_with_larger_range;
};

/**
 * Width-balanced histogram.
 * Bins are of roughly equal width, i.e., cover the same share of the domain of values.
 * Bins are consecutive (no gaps between bins), but might be empty.
 */
template <typename T>
class EqualWidthHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  EqualWidthHistogram(const T min, const T max, const std::vector<size_t>& heights,
                      const std::vector<size_t>& distinct_counts, const size_t bin_count_with_larger_range);
  EqualWidthHistogram(const std::string& min, const std::string& max, const std::vector<size_t>& heights,
                      const std::vector<size_t>& distinct_counts, const size_t bin_count_with_larger_range,
                      const std::string& supported_characters, const size_t string_prefix_length);

  /**
   * Create a histogram based on the data in a given segment.
   * @param segment The segment containing the data.
   * @param max_bin_count The number of bins to create. The histogram might create fewer, but never more.
   * @param supported_characters A sorted, consecutive string of characters supported in case of string histograms.
   * Can be omitted and will be filled with default value.
   * @param string_prefix_length The prefix length used to calculate string ranges.
   * * Can be omitted and will be filled with default value.
   */
  static std::shared_ptr<EqualWidthHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const size_t max_bin_count,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<size_t>& string_prefix_length = std::nullopt);

  HistogramType histogram_type() const override;
  size_t total_distinct_count() const override;
  size_t total_count() const override;
  size_t bin_count() const override;

 protected:
  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  size_t _bin_height(const BinID index) const override;
  size_t _bin_distinct_count(const BinID index) const override;

  /**
   * Creates bins and their statistics.
   *
   * This method is implemented in the header file because of std::enable_if_t.
   * It is only enabled for numerical types.
   */
  template <typename Q = T>
  static std::enable_if_t<std::is_arithmetic_v<Q>, EqualWidthBinStats<T>> _get_bin_stats(
      const std::vector<std::pair<T, size_t>>& value_counts, const size_t max_bin_count) {
    // Bins shall have the same range.
    const auto min = value_counts.front().first;
    const auto max = value_counts.back().first;
    const T base_width = next_value(max - min);

    // Never have more bins than representable values.
    // TODO(anyone): fix for floats. This is more of a theoretical issue, however.
    auto bin_count = max_bin_count;
    if constexpr (std::is_integral_v<T>) {
      bin_count = max_bin_count <= static_cast<size_t>(base_width) ? max_bin_count : base_width;
    }

    const T bin_width = base_width / bin_count;
    size_t bin_count_with_larger_range;
    if constexpr (std::is_integral_v<T>) {
      bin_count_with_larger_range = base_width % bin_count;
    } else {
      bin_count_with_larger_range = 0ul;
    }

    std::vector<size_t> counts;
    std::vector<size_t> distinct_counts;
    counts.reserve(bin_count);
    distinct_counts.reserve(bin_count);

    T current_begin_value = min;
    auto current_begin_it = value_counts.cbegin();
    for (auto current_bin_id = 0ul; current_bin_id < bin_count; current_bin_id++) {
      T next_begin_value = current_begin_value + bin_width;

      if constexpr (std::is_integral_v<T>) {
        if (current_bin_id < bin_count_with_larger_range) {
          next_begin_value++;
        }
      }

      if constexpr (std::is_floating_point_v<T>) {
        // This is intended to compensate for the fact that floating point arithmetic is not exact.
        // Adding up floating point numbers adds an error over time.
        // So this is how we make sure that the last bin contains the rest of the values.
        if (current_bin_id == bin_count - 1) {
          next_begin_value = next_value(max);
        }
      }

      auto next_begin_it = current_begin_it;
      while (next_begin_it != value_counts.cend() && (*next_begin_it).first < next_begin_value) {
        next_begin_it++;
      }

      counts.emplace_back(std::accumulate(current_begin_it, next_begin_it, size_t{0},
                                          [](size_t a, const std::pair<T, size_t>& b) { return a + b.second; }));
      distinct_counts.emplace_back(std::distance(current_begin_it, next_begin_it));

      current_begin_value = next_begin_value;
      current_begin_it = next_begin_it;
    }

    return {min, max, counts, distinct_counts, bin_count_with_larger_range};
  }

  /**
   * Creates bins and their statistics for string histograms.
   *
   * This method is only implemented for strings.
   */
  static EqualWidthBinStats<std::string> _get_bin_stats(const std::vector<std::pair<std::string, size_t>>& value_counts,
                                                        const size_t max_bin_count,
                                                        const std::string& supported_characters,
                                                        const size_t string_prefix_length);

  /**
   * Overriding for numeric data types because it would otherwise recursively call itself.
   * For strings simply call the method in AbstractHistogram.
   */
  typename AbstractHistogram<T>::HistogramWidthType _bin_width(const BinID index) const override;

 private:
  // Minimum value of the histogram.
  T _min;

  // Maximum value of the histogram.
  T _max;

  // Number of values on a per-bin basis.
  std::vector<size_t> _heights;

  // Number of distinct values on a per-bin basis.
  std::vector<size_t> _distinct_counts;

  // Number of bins that are one element wider (only used for integral and string histograms).
  BinID _bin_count_with_larger_range;
};

}  // namespace opossum
