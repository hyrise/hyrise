#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

/**
 * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
 */
template <typename T>
struct EqualHeightBinData {
  // Minimum value of the histogram.
  T minimum;

  // Max values on a per-bin basis.
  std::vector<T> bin_maxima;

  // Total number of values in the histogram.
  HistogramCountType total_count;

  // Number of distinct values on a per-bin basis.
  std::vector<HistogramCountType> bin_distinct_counts;
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
  EqualHeightHistogram(const T minimum, std::vector<T>&& bin_maxima, const HistogramCountType total_count,
                       std::vector<HistogramCountType>&& bin_distinct_counts);
  EqualHeightHistogram(const pmr_string& minimum, std::vector<pmr_string>&& bin_maxima,
                       const HistogramCountType total_count, std::vector<HistogramCountType>&& bin_distinct_counts,
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
  static std::shared_ptr<EqualHeightHistogram<T>> from_segment(
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
   * The number of bins is capped at the number of distinct values in the segment.
   * Otherwise, there would be empty bins without any benefit.
   */
  BinID bin_count() const override;

 protected:
  /**
   * Creates bins and their statistics.
   */
  static EqualHeightBinData<T> _build_bins(const std::vector<std::pair<T, HistogramCountType>>& value_counts,
                                           const BinID max_bin_count);

  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

  T _bin_minimum(const BinID index) const override;
  T _bin_maximum(const BinID index) const override;
  HistogramCountType _bin_height(const BinID index) const override;
  HistogramCountType _bin_distinct_count(const BinID index) const override;

 private:
  const EqualHeightBinData<T> _bin_data;
};

}  // namespace opossum
