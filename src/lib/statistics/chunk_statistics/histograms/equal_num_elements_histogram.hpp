#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualNumElementsBinStats {
  std::vector<T> mins;
  std::vector<T> maxs;
  std::vector<uint64_t> counts;
  uint64_t distinct_count_per_bin;
  uint64_t num_bins_with_extra_value;
};

/**
 * Distinct-balanced histogram.
 * Bins contain roughly the same number of distinct values actually occuring in the data.
 * There might be gaps between bins.
 */
template <typename T>
class EqualNumElementsHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualNumElementsHistogram(const std::vector<T>& mins, const std::vector<T>& maxs, const std::vector<uint64_t>& counts,
                            const uint64_t distinct_count_per_bin, const uint64_t num_bins_with_extra_value);
  EqualNumElementsHistogram(const std::vector<std::string>& mins, const std::vector<std::string>& maxs,
                            const std::vector<uint64_t>& counts, const uint64_t distinct_count_per_bin,
                            const uint64_t num_bins_with_extra_value, const std::string& supported_characters,
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
  static std::shared_ptr<EqualNumElementsHistogram<T>> from_segment(
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
  static EqualNumElementsBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                                    const size_t max_num_bins);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;
  uint64_t _bin_count_distinct(const BinID index) const override;

 private:
  // Min values on a per-bin basis.
  std::vector<T> _mins;

  // Max values on a per-bin basis.
  std::vector<T> _maxs;

  // Number of values on a per-bin basis.
  std::vector<uint64_t> _counts;

  // Number of distinct values per bin.
  uint64_t _distinct_count_per_bin;

  // Number of bins which have an additional distinct value.
  uint64_t _num_bins_with_extra_value;
};

}  // namespace opossum
