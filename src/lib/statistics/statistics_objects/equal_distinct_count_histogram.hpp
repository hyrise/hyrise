#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest_prod.h"

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/**
 * Distinct-balanced histogram.
 * The first `bin_count_with_extra_value` contain each contain `distinct_count_per_bin + 1` distinct values,
 * all other bins contain `distinct_count_per_bin` distinct values.
 * There might be gaps between bins.
 */
template <typename T>
class EqualDistinctCountHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                              std::vector<HistogramCountType>&& bin_heights,
                              const HistogramCountType distinct_count_per_bin, const BinID bin_count_with_extra_value,
                              const HistogramDomain<T>& domain = {});

  /**
   * Create an EqualDistinctCountHistogram for a column (spanning all Segments) of a table
   * @param max_bin_count   Desired number of bins. Less might be created, but never more. Must not be zero.
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_column(const Table& table, const ColumnID column_id,
                                                                     const BinID max_bin_count,
                                                                     const HistogramDomain<T>& domain = {});
  /**
   * Create an EqualDistinctCountHistogram for a segment of a table
   * @param max_bin_count   Desired number of bins. Less might be created, but never more. Must not be zero.
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_segment(const Table& table, const ColumnID column_id,
                                                                      const ChunkID chunk_id, const BinID max_bin_count,
                                                                      const HistogramDomain<T>& domain = {});

  /**
   * Merge all histograms in @param histograms into one new EqualDistinctCountHistogram with at
   * most @param max_bin_count bins.
   * Returns the merged histogram and the max_estimation_error. 
   * Returns (nullptr, 0.0) if no valid histograms are given.
   * The max_estimation_error is an upper bound on the difference between the total_distinct_count of the histogram
   * calculated via merging chunk histograms and the total_distinct_count of the histogram calculated based on the
   * whole column.
   */
  static std::pair<std::shared_ptr<EqualDistinctCountHistogram<T>>, HistogramCountType> merge_histograms(
      const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms, BinID max_bin_count);

  std::string name() const override;
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

  const T& bin_minimum(const BinID index) const override;
  const T& bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

 protected:
  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

  /**
   * Generates an EqualDistinctCountHistogram from the @param value_distribution.
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> _from_value_distribution(
      const std::vector<std::pair<T, HistogramCountType>>& value_distribution, const BinID max_bin_count);

  FRIEND_TEST(EqualDistinctCountHistogramTest, CombineBounds);
  /**
   * Splits the @param histograms and returns the combined bin_minima and bin_maxima.
   * The result should be a fine grained set of bounds
   * such that each bin edge of the input histograms is a bin edge of the combined bins.
   * Note that this introduces some bins that are empty in all input histograms (e.g. (4; 9) in the example below).
   *
   * Example: We have two histograms with different bounds:
   *    Histogram 1: (0; 3), (10; 20)
   *    Histogram 2: (15; 30), (31; 40) 
   *    Combined:    (0; 3), (4; 9), (10; 14), (15; 20), (21; 30), (31; 40)
   */
  static std::pair<std::vector<T>, std::vector<T>> _combine_bounds(
      const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms);

  /**
   * Takes the @param histograms and their combined_bounds, adds up their heights 
   * and estimates their combined distinct_counts in each of the intervals given by the combined_bounds.
   * Also filters out all empty intervals contained in the combined_bounds.
   */
  static std::tuple<std::vector<HistogramCountType>, std::vector<HistogramCountType>, std::vector<T>, std::vector<T>,
                    HistogramCountType>
  _create_merged_intervals(const std::vector<T>& combined_bounds_minima, const std::vector<T>& combined_bounds_maxima,
                           const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms,
                           const HistogramDomain<T>& domain);

  /**
   * Creates one bin with @param distinct_count_target many distinct values by combining as many intervals
   * (given in the form of iterators) as necessary. It splits intervals if necessary and possible. If there are 
   * unsplittable intervals (interval from 1 to 1) or intervals that cannot be split in arbitrary ratios (integers),
   * the distinct_count of the bin might differ from the specified distinct_count_target.
   * After calling this method, all iterators are advanced to the next unused interval and splitted intervals are
   * adapted, such that their remaining part can be used by the next bin.
   * If @param is_last_bin is set, the method will add all remaining intervals, regardless of size.
   */
  static std::tuple<T, T, HistogramCountType> _create_one_bin(
      typename std::vector<T>::iterator& interval_minima_begin, typename std::vector<T>::iterator& interval_minima_end,
      typename std::vector<T>::iterator& interval_maxima_begin, typename std::vector<T>::iterator& interval_maxima_end,
      typename std::vector<HistogramCountType>::iterator& interval_heights_begin,
      typename std::vector<HistogramCountType>::iterator& interval_heights_end,
      typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_begin,
      typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_end, const int distinct_count_target,
      const HistogramDomain<T> domain, const bool is_last_bin);

  FRIEND_TEST(EqualDistinctCountHistogramTest, IntBalanceBins);
  FRIEND_TEST(EqualDistinctCountHistogramTest, FloatBalanceBins);
  /**
   * Takes the values making up a histogram and reduces its bin count to the defined value. 
   * The distinct counts are balanced afterwards, when possible (unsplittable intervals might cause imbalance).
   *
   * Note that the input vectors might change during the balancing process.
   *
   * @param interval_distinct_counts    vector that contains the distinct counts of the input histogram
   * @param interval_heights            vector that contains the bin heights of the input histogram
   * @param interval_minima             vector that contains the bin minima of the input histogram
   * @param interval_maxima             vector that contains the bin maxima of the input histogram
   * @param total_distinct_count        sum of all bins distinct counts
   * @param max_bin_count               maximum number of bins the output histogram should have
   * @param bin_count_with_extra_value  the first bins might get an additional value to prevent a smaller last bin
   * @param domain                      the domain of the histogram
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> _balance_bins_into_histogram(
      std::vector<HistogramCountType>& interval_distinct_counts, std::vector<HistogramCountType>& interval_heights,
      std::vector<T>& interval_minima, std::vector<T>& interval_maxima, const HistogramCountType total_distinct_count,
      const BinID max_bin_count, const HistogramDomain<T> domain);

 private:
  /**
   * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
   */

  // Min values on a per-bin basis.
  std::vector<T> _bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> _bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<HistogramCountType> _bin_heights;

  // Number of distinct values per bin.
  HistogramCountType _distinct_count_per_bin;

  // The first bin_count_with_extra_value bins have an additional distinct value.
  BinID _bin_count_with_extra_value;

  // Aggregated counts over all bins, to avoid redundant computation
  HistogramCountType _total_count;
  HistogramCountType _total_distinct_count;
};

}  // namespace opossum
