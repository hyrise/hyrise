#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualWidthBinStats {
  T min;
  T max;
  std::vector<uint64_t> counts;
  std::vector<uint64_t> distinct_counts;
  uint64_t num_bins_with_larger_range;
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
  EqualWidthHistogram(const T min, const T max, const std::vector<uint64_t>& counts,
                      const std::vector<uint64_t>& distinct_counts, const uint64_t num_bins_with_larger_range);
  EqualWidthHistogram(const std::string& min, const std::string& max, const std::vector<uint64_t>& counts,
                      const std::vector<uint64_t>& distinct_counts, const uint64_t num_bins_with_larger_range,
                      const std::string& supported_characters, const uint64_t string_prefix_length);

  /**
   * Create a histogram based on the data in a given segment.
   * @param segment The segment containing the data.
   * @param max_num_bins The number of bins to create. The histogram might create fewer, but never more.
   * @param supported_characters A sorted, consecutive string of characters supported in case of string histograms.
   * Can be omitted and will be filled with default value.
   * @param string_prefix_length The prefix length used to calculate string ranges.
   * * Can be omitted and will be filled with default value.
   */
  static std::shared_ptr<EqualWidthHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const size_t max_num_bins,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint64_t>& string_prefix_length = std::nullopt);

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_bins() const override;

 protected:
  /**
   * Creates bins and their statistics.
   * This method is overloaded with more parameters for strings.
   */
  static EqualWidthBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                              const size_t max_num_bins);
  static EqualWidthBinStats<std::string> _get_bin_stats(
      const std::vector<std::pair<std::string, uint64_t>>& value_counts, const size_t max_num_bins,
      const std::string& supported_characters, const uint64_t string_prefix_length);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;
  uint64_t _bin_count_distinct(const BinID index) const override;

  // Overriding because it would otherwise recursively call itself.
  T _bin_width(const BinID index) const override;

 private:
  // Minimum value of the histogram.
  T _min;

  // Maximum value of the histogram.
  T _max;

  // Number of values on a per-bin basis.
  std::vector<uint64_t> _counts;

  // Number of distinct values on a per-bin basis.
  std::vector<uint64_t> _distinct_counts;

  // Number of bins that are one element wider (only used for integral and string histograms).
  uint64_t _num_bins_with_larger_range;
};

}  // namespace opossum
