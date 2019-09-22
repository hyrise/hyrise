#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "histogram_domain.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "storage/base_segment.hpp"
#include "types.hpp"

namespace opossum {

using BinID = size_t;
constexpr BinID INVALID_BIN_ID{std::numeric_limits<BinID>::max()};

/**
 * Used for HistogramBin heights and distinct counts
 */
using HistogramCountType = Cardinality;

template <typename T>
struct HistogramBin {
  HistogramBin(const T& min, const T& max, const HistogramCountType height, const HistogramCountType distinct_count)
      : min(min), max(max), height(height), distinct_count(distinct_count) {}

  T min{};
  T max{};
  HistogramCountType height{};
  HistogramCountType distinct_count{};
};

// For googletest
template <typename T>
bool operator==(const HistogramBin<T>& bin_a, const HistogramBin<T>& bin_b) {
  return bin_a.min == bin_b.min && bin_a.max == bin_b.max && bin_a.height == bin_b.height &&
         bin_a.distinct_count == bin_b.distinct_count;
}

// For googletest
template <typename T>
std::ostream& operator<<(std::ostream& stream, const HistogramBin<T>& bin) {
  if constexpr (std::is_same_v<T, std::string>) {
    stream << "['" << bin.min << "' -> '" << bin.max << "'] ";
  } else {
    stream << "[" << bin.min << " -> " << bin.max << "] ";
  }

  stream << "Height: " << bin.height << "; DistinctCount: " << bin.distinct_count;
  return stream;
}

/**
 * Base class for histogram types.
 *
 * Deriving classes implement the bin storage and provide a general interface (`bin_minimum`, ...) for it. The
 * AbstractHistogram class builds all estimation logic based on this interface.
 *
 * A histogram consists of a collection of bins.
 * A bin is made up of:
 *  - a lower bound
 *  - a upper bound
 *  - a number of values
 *  - a number of distinct values
 *
 * Histograms are supported for all data column types Hyrise supports.
 *
 * String histograms, however, are implemented slightly different because of their non-numerical property. While lower
 * and upper bin bounds are stored as strings, they are converted to integers for cardinality estimation purposes
 * (see StringHistogramDomain).
 */
template <typename T>
class AbstractHistogram : public AbstractStatisticsObject {
 public:
  /**
   * Strings are internally transformed to a number, such that a bin can have a numerical width.
   * This transformation is based on uint64_t.
   */
  using HistogramWidthType = std::conditional_t<std::is_same_v<T, pmr_string>, StringHistogramDomain::IntegralType, T>;

  explicit AbstractHistogram(const HistogramDomain<T>& domain = {});

  /**
   * @return name of the histogram type, e.g., "EqualDistinctCount"
   */
  virtual std::string name() const = 0;

  /**
   * @return a deep copy of this histogram
   */
  virtual std::shared_ptr<AbstractHistogram<T>> clone() const = 0;

  /**
   * @return an interface to perform operations (next_value(), ...) in the domain of the type T. See HistogramDomain<T>
   *         for details.
   */
  const HistogramDomain<T>& domain() const;

  /**
   * @returns detailed information about the histogram, including the properties of the individual bins.
   */
  std::string description() const;

  Cardinality estimate_cardinality(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                   const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  std::pair<Cardinality, DistinctCount> estimate_cardinality_and_distinct_count(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  /**
   * Returns whether a given predicate type and its parameter(s) can belong to a bin or not.
   * Still an estimation, do not use for pruning decisions!
   *
   * Specifically, if this method returns true, the predicate does not yield any results.
   * If this method returns false, the predicate might yield results.
   * This method is specialized for strings to handle predicates uniquely applicable to string columns.
   */
  bool does_not_contain(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Derive a Histogram from this histograms splitting its bins so that both the current bounds as well as the bounds
   * specified in @param additional_bin_edges are present.
   *
   * E.g., for a histogram with bins {[0, 10], [15, 20]}, split_at_bin_bounds({{-4, 5}, {16, 18}}) returns
   * a histogram with bins {[0, 5], [6, 10], [15, 15], [16, 18], [19, 20]}
   *
   * @param additional_bin_edges    Pair of minima and maxima of new bins
   */
  std::shared_ptr<AbstractHistogram<T>> split_at_bin_bounds(
      const std::vector<std::pair<T, T>>& additional_bin_edges) const;

  /**
   * @return [{bin_minimum(BinID{0}), bin_maximum(BinID{0})}, {bin_minimum(BinID{1}), bin_maximum(BinID{1})}, ...]
   */
  std::vector<std::pair<T, T>> bin_bounds() const;

  /**
   * Returns the number of bins actually present in the histogram.
   * This number can be smaller than the number of bins requested when creating a histogram.
   * See implementations of this method in specific histograms for details.
   */
  virtual BinID bin_count() const = 0;

  /**
   * Returns the number of values represented in the histogram.
   * This is equal to the number of rows in the segment during the generation of the bins for the histogram,
   * without null values.
   */
  virtual HistogramCountType total_count() const = 0;

  /**
   * Returns the number of distinct values represented in the histogram.
   * This is equal to the number of distinct values in the segment during creation.
   */
  virtual HistogramCountType total_distinct_count() const = 0;

  /**
   * Returns the smallest value in the bin.
   */
  virtual T bin_minimum(const BinID index) const = 0;

  /**
   * Returns the largest value in a bin.
   */
  virtual T bin_maximum(const BinID index) const = 0;

  /**
   * Returns whether the value is contained in a given bin
   */
  bool bin_contains(const BinID index, const T& value) const;

  /**
   * Returns the number of values in a bin.
   */
  virtual HistogramCountType bin_height(const BinID index) const = 0;

  /**
   * Returns the number of distinct values in a bin.
   */
  virtual HistogramCountType bin_distinct_count(const BinID index) const = 0;

  /**
   * Returns the width of a bin.
   * This method is specialized for strings to return a numerical width.
   */
  virtual HistogramWidthType bin_width(const BinID index) const;

  /**
   * Helper function to retrieve all properties of a bin in a single struct. Convenience method, as histogram
   * implementations do store bins in different ways (EqualDistinctCountHistogram, e.g., has a single member holding
   * the distinct count of all bins).
   * @return {bin_minimum(index), bin_maximum(index), bin_height(index), bin_distinct_count(index)}
   */
  HistogramBin<T> bin(const BinID index) const;

  /**
   * Returns the share of the value range of a bin that are smaller (or equals) than `value`.
   * This method is specialized for strings.
   * @{
   */
  float bin_ratio_less_than(const BinID bin_id, const T& value) const;
  float bin_ratio_less_than_equals(const BinID bin_id, const T& value) const;
  /** @} */

  /**
   * Returns the share of the value range of a bin that is within [value, value2], i.e., BetweenInclusive
   */
  float bin_ratio_between(const BinID bin_id, const T& value, const T& value2) const;

 protected:
  // Call after constructor of the derived histogram has finished to check whether the bins are valid
  // (e.g. do not overlap).
  void _assert_bin_validity();

  /**
   * Given a Bin with @param value_count total values and @param distinct_count, estimate the resulting distinct count
   * if a subset of the total values (@param selectivity) is taken.
   * Currently, this is just a dummy heuristic that ensures the resulting distinct count does not exceed the resulting
   * value count.
   *
   * E.g. _scale_distinct_count(100, 5, 0.1) -> 5
   *      _scale_distinct_count(100, 5, 0.01) -> 1
   */
  static Cardinality _scale_distinct_count(Cardinality value_count, Cardinality distinct_count,
                                           Selectivity selectivity);

 private:
  /**
   * Helper for implementing, e.g., LessThan as an inversion of GreaterThanEquals.
   * @return total_[distinct_]count() - estimate
   */
  std::pair<Cardinality, DistinctCount> _invert_estimate(const std::pair<Cardinality, DistinctCount>& estimate) const;

  /**
   * Returns the id of the bin that holds the given `value`.
   * Returns INVALID_BIN_ID if `value` does not belong to any bin.
   */
  virtual BinID _bin_for_value(const T& value) const = 0;

  /**
   * Returns the id of the bin after the one that holds the given `value`.
   * If `value` does not belong to any bin but is smaller than max(), it is in a gap.
   * In that case return the bin right after the gap.
   * If the bin that holds the value is the last bin or it is greater than max, return INVALID_BIN_ID.
   */
  virtual BinID _next_bin_for_value(const T& value) const = 0;

  HistogramDomain<T> _domain;
};

}  // namespace opossum
