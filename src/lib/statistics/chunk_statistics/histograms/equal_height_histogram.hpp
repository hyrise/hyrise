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
  std::vector<HistogramCountType> distinct_counts;
  T min;
  HistogramCountType total_count;
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
  EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<HistogramCountType>& distinct_counts, const T min,
                       const HistogramCountType total_count);
  EqualHeightHistogram(const std::vector<std::string>& maxs, const std::vector<HistogramCountType>& distinct_counts,
                       const std::string& min, const HistogramCountType total_count,
                       const std::string& supported_characters, const uint32_t string_prefix_length);

  /**
   * Create a histogram based on the data in a given segment.
   * @param segment The segment containing the data.
   * @param max_bin_count The number of bins to create. The histogram might create fewer, but never more.
   * @param supported_characters A sorted, consecutive string of characters supported in case of string histograms.
   * Can be omitted and will be filled with default value.
   * @param string_prefix_length The prefix length used to calculate string ranges.
   * * Can be omitted and will be filled with default value.
   */
  static std::shared_ptr<EqualHeightHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint32_t>& string_prefix_length = std::nullopt);

  HistogramType histogram_type() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;
  BinID bin_count() const override;

 protected:
  /**
   * Creates bins and their statistics.
   */
  static EqualHeightBinStats<T> _get_bin_stats(const std::vector<std::pair<T, HistogramCountType>>& value_counts,
                                               const BinID max_bin_count);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  HistogramCountType _bin_height(const BinID index) const override;
  HistogramCountType _bin_distinct_count(const BinID index) const override;

 private:
  // Max values on a per-bin basis.
  std::vector<T> _maxs;

  // Number of distinct values on a per-bin basis.
  std::vector<HistogramCountType> _distinct_counts;

  // Minimum value of the histogram.
  T _min;

  // Total number of values in the histogram.
  HistogramCountType _total_count;
};

}  // namespace opossum
