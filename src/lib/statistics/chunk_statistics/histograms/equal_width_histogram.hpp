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

/**
 * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
 */
template <typename T>
struct EqualWidthBinData {
  // Minimum value of the histogram.
  T minimum;

  // Maximum value of the histogram.
  T maximum;

  // Number of values on a per-bin basis.
  std::vector<HistogramCountType> bin_heights;

  // Number of distinct values on a per-bin basis.
  std::vector<HistogramCountType> bin_distinct_counts;

  // Number of bins that are one element wider (only used for integral and string histograms).
  BinID bin_count_with_larger_range;
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

  EqualWidthHistogram(const T minimum, const T maximum, std::vector<HistogramCountType>&& bin_heights,
                      std::vector<HistogramCountType>&& bin_distinct_counts, const BinID bin_count_with_larger_range);
  EqualWidthHistogram(const pmr_string& minimum, const pmr_string& maximum,
                      std::vector<HistogramCountType>&& bin_heights,
                      std::vector<HistogramCountType>&& bin_distinct_counts, const BinID bin_count_with_larger_range,
                      const pmr_string& supported_characters, const size_t string_prefix_length);

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
      const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
      const std::optional<pmr_string>& supported_characters = std::nullopt,
      const std::optional<uint32_t>& string_prefix_length = std::nullopt);

  HistogramType histogram_type() const override;
  std::string histogram_name() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  /**
   * Returns the number of bins actually present in the histogram.
   * This number can be smaller than the number of bins requested when creating a histogram.
   * The number of bins is capped at the number of possible values in the range of the segment for integers and strings.
   * That is, if the minimum and maximum value in an integer segment are 1 and 100, there will be at most 100 bins.
   * Otherwise, there would be values which belonged to more than one bin.
   * This is a theoretical problem for floating point numbers as well, but not solved,
   * because the number of representable floating point numbers in a range is not trivial to determine.
   */
  BinID bin_count() const override;

 protected:
  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

  T _bin_minimum(const BinID index) const override;
  T _bin_maximum(const BinID index) const override;
  HistogramCountType _bin_height(const BinID index) const override;
  HistogramCountType _bin_distinct_count(const BinID index) const override;

  /**
   * Creates bins and their statistics.
   *
   * This method is implemented in the header file because of std::enable_if_t.
   * It is only enabled for numerical types.
   */
  template <typename Q = T>
  static std::enable_if_t<std::is_arithmetic_v<Q>, EqualWidthBinData<T>> _build_bins(
      const std::vector<std::pair<T, HistogramCountType>>& value_counts, const BinID max_bin_count) {
    // Bins shall have the same range.
    const auto min = value_counts.front().first;
    const auto max = value_counts.back().first;
    const auto base_width = next_value(max - min);

    // Never have more bins than representable values.
    // TODO(anyone): fix for floats. This is more of a theoretical issue, however.
    auto bin_count = max_bin_count;
    if constexpr (std::is_integral_v<T>) {
      bin_count = max_bin_count <= static_cast<BinID>(base_width) ? max_bin_count : base_width;
    }

    const auto bin_width = base_width / bin_count;
    BinID bin_count_with_larger_range;
    if constexpr (std::is_integral_v<T>) {
      bin_count_with_larger_range = base_width % bin_count;
    } else {
      bin_count_with_larger_range = 0ul;
    }

    std::vector<HistogramCountType> bin_heights(bin_count);
    std::vector<HistogramCountType> bin_distinct_counts(bin_count);

    auto current_bin_begin_it = value_counts.cbegin();
    for (auto current_bin_id = BinID{0}; current_bin_id < bin_count; current_bin_id++) {
      auto next_bin_begin_value = static_cast<T>(min + bin_width * (current_bin_id + 1u));

      if constexpr (std::is_integral_v<T>) {
        next_bin_begin_value += std::min(current_bin_id + 1, bin_count_with_larger_range);
      }

      if constexpr (std::is_floating_point_v<T>) {
        // This is intended to compensate for the fact that floating point arithmetic is not exact.
        // Adding up floating point numbers adds an error over time.
        // So this is how we make sure that the last bin contains the rest of the values.
        if (current_bin_id == bin_count - 1) {
          next_bin_begin_value = next_value(max);
        }
      }

      auto next_bin_begin_it = current_bin_begin_it;
      // This could be a binary search,
      // but we decided that for the relatively small number of values and bins it is not worth it,
      // as linear search tends to be just as fast or faster due to hardware optimizations for small vectors.
      // Feel free to change if this comes up in a profiler.
      while (next_bin_begin_it != value_counts.cend() && (*next_bin_begin_it).first < next_bin_begin_value) {
        next_bin_begin_it++;
      }

      bin_heights[current_bin_id] =
          std::accumulate(current_bin_begin_it, next_bin_begin_it, HistogramCountType{0},
                          [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });
      bin_distinct_counts[current_bin_id] =
          static_cast<HistogramCountType>(std::distance(current_bin_begin_it, next_bin_begin_it));

      current_bin_begin_it = next_bin_begin_it;
    }

    return {min, max, std::move(bin_heights), std::move(bin_distinct_counts), bin_count_with_larger_range};
  }

  /**
   * Creates bins and their statistics for string histograms.
   *
   * This method is only implemented for strings.
   */
  static EqualWidthBinData<pmr_string> _build_bins(
      const std::vector<std::pair<pmr_string, HistogramCountType>>& value_counts, const BinID max_bin_count,
      const pmr_string& supported_characters, const size_t string_prefix_length);

  /**
   * Overriding for numeric data types because it would otherwise recursively call itself.
   * For strings simply call the method in AbstractHistogram.
   */
  typename AbstractHistogram<T>::HistogramWidthType _bin_width(const BinID index) const override;

 private:
  const EqualWidthBinData<T> _bin_data;
};

}  // namespace opossum
