#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "statistics/selectivity.hpp"
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
  EqualDistinctCountHistogram(std::vector<std::string>&& bin_minima, std::vector<std::string>&& bin_maxima,
                              std::vector<HistogramCountType>&& bin_heights,
                              const HistogramCountType distinct_count_per_bin, const BinID bin_count_with_extra_value,
                              const StringHistogramDomain& string_domain);

  /**
   * Create a histogram based on a value distribution.
   * @param value_distribution      For each value, the number of occurrences. Must be sorted
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_distribution(
      const std::vector<std::pair<T, HistogramCountType>>& value_distribution, const BinID max_bin_count,
      const std::optional<StringHistogramDomain>& string_domain = std::nullopt);
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_segment(
      const std::shared_ptr<BaseSegment>& segment, const BinID max_bin_count,
      const std::optional<StringHistogramDomain>& string_domain = std::nullopt);

  std::string histogram_name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  /**
   * Returns the number of bins actually present in the histogram.
   * This number can be smaller than the number of bins requested when creating a histogram.
   * The number of bins is capped at the number of distinct values in the segment.
   * Otherwise, there would be empty bins without any benefit.
   */
  BinID bin_count() const override;

  T bin_minimum(const BinID index) const override;
  T bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

 protected:
  /**
   * Creates bins and their statistics.
   */
  static EqualDistinctCountBinData<T> _build_bins(const std::vector<std::pair<T, HistogramCountType>>& value_counts,
                                                  const BinID max_bin_count,
                                                  const std::optional<StringHistogramDomain>& string_domain);

  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

 private:
  const EqualDistinctCountBinData<T> _bin_data;
};

}  // namespace opossum
