#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualHeightBinStats {
  std::vector<T> maxs;
  std::vector<uint64_t> distinct_counts;
  T min;
  uint64_t total_count;
};

/**
 * Height-balanced histogram.
 * Bins contain roughly the same number of elements actually occuring in the represented data.
 * Bins are consecutive (no gaps between bins), and cannot be empty.
 */
template <typename T>
class EqualHeightHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<uint64_t>& distinct_counts, const T min,
                       const uint64_t total_count);
  EqualHeightHistogram(const std::vector<std::string>& maxs, const std::vector<uint64_t>& distinct_counts,
                       const std::string& min, const uint64_t total_count, const std::string& supported_characters,
                       const uint64_t string_prefix_length);

  /**
   * Create a histogram based on the data in a given segment.
   * @param segment The segment containing the data.
   * @param max_num_bins The number of bins to create. The histogram might create fewer, but never more.
   * @param supported_characters A sorted, consecutive string of characters supported in case of string histograms.
   * Can be omitted and will be filled with default value.
   * @param string_prefix_length The prefix length used to calculate string ranges.
   * * Can be omitted and will be filled with default value.
   */
  static std::shared_ptr<EqualHeightHistogram<T>> from_segment(
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
   */
  static EqualHeightBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                               const size_t max_num_bins);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;
  uint64_t _bin_count_distinct(const BinID index) const override;

 private:
  // Max values on a per-bin basis.
  std::vector<T> _maxs;

  // Number of distinct values on a per-bin basis.
  std::vector<uint64_t> _distinct_counts;

  // Minimum value of the histogram.
  T _min;

  // Total number of values in the histogram.
  uint64_t _total_count;
};

}  // namespace opossum
