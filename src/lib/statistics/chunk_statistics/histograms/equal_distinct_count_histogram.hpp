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
struct EqualDistinctCountBinData {
  // Min values on a per-bin basis.
  std::vector<T> bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<HistogramCountType> bin_heights;

  // Number of distinct values per bin.
  HistogramCountType distinct_count_per_bin;

  // The first bin_count_with_extra_value bins have an additional distinct value.
  BinID bin_count_with_extra_value;
};

/**
 * Distinct-balanced histogram.
 * Bins contain roughly the same number of distinct values actually occurring in the data.
 * There might be gaps between bins.
 */
template <typename T>
class EqualDistinctCountHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                              std::vector<HistogramCountType>&& bin_heights,
                              const HistogramCountType distinct_count_per_bin, const BinID bin_count_with_extra_value);
  EqualDistinctCountHistogram(std::vector<pmr_string>&& bin_minima, std::vector<pmr_string>&& bin_maxima,
                              std::vector<HistogramCountType>&& bin_heights,
                              const HistogramCountType distinct_count_per_bin, const BinID bin_count_with_extra_value,
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
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_segment(
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
  static EqualDistinctCountBinData<T> _build_bins(const std::vector<std::pair<T, HistogramCountType>>& value_counts,
                                                  const BinID max_bin_count);

  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

  T _bin_minimum(const BinID index) const override;
  T _bin_maximum(const BinID index) const override;
  HistogramCountType _bin_height(const BinID index) const override;
  HistogramCountType _bin_distinct_count(const BinID index) const override;

 private:
  const EqualDistinctCountBinData<T> _bin_data;
};

}  // namespace opossum
