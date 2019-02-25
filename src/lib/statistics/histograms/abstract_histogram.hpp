#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "statistics/abstract_statistics_object.hpp"
#include "statistics/cardinality_estimate.hpp"
#include "storage/base_segment.hpp"
#include "histogram_domain.hpp"
#include "types.hpp"

namespace opossum {

using BinID = size_t;
constexpr BinID INVALID_BIN_ID{std::numeric_limits<BinID>::max()};

/**
 * Used for HistogramBin heights and distinct counts
 */
using HistogramCountType = float;

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

// Often both cardinality and distinct count of a estimate are required
struct CardinalityAndDistinctCountEstimate {
  Cardinality cardinality{};
  EstimateType type{};
  float distinct_count{};
};

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
 * Histograms are supported for all five data column types Hyrise supports.
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
  using HistogramWidthType = std::conditional_t<std::is_same_v<T, std::string>, StringHistogramDomain::IntegralType, T>;

  AbstractHistogram(const HistogramDomain<T>& domain = {});

  ~AbstractHistogram() override = default;

  // Non-copyable
  AbstractHistogram(AbstractHistogram&&) = default;
  AbstractHistogram& operator=(AbstractHistogram&&) = default;
  AbstractHistogram(const AbstractHistogram&) = delete;
  const AbstractHistogram& operator=(const AbstractHistogram&) = delete;

  /**
   * @return name of the histogram type, e.g., "EqualDistinctCount"
   */
  virtual std::string histogram_name() const = 0;

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

  CardinalityEstimate estimate_cardinality(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                           const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  CardinalityAndDistinctCountEstimate estimate_cardinality_and_distinct_count(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;


  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  /**
   * Derive a Histogram from this histograms splitting its bins so that both the current bounds as well as the bounds
   * specified in @param additional_bin_edges are present.
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
   * Helper function
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

 protected:
  CardinalityEstimate _invert_estimate(const CardinalityEstimate& estimate) const;
  CardinalityAndDistinctCountEstimate _invert_estimate(const CardinalityAndDistinctCountEstimate& estimate) const;

  /**
   * Returns whether a given predicate type and its parameter(s) can belong to a bin or not.
   * Specifically, if this method returns true, the predicate does not yield any results.
   * If this method returns false, the predicate might yield results.
   * This method is specialized for strings to handle predicates uniquely applicable to string columns.
   */
  bool _does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Returns whether a given predicate type and its parameter(s) can belong to a bin or not
   * for predicate types supported by all data types.
   */
  bool _general_does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

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

  // Call after constructor of the derived histogram has finished to check whether the bins are valid
  // (e.g. do not overlap).
  void _assert_bin_validity();

  HistogramDomain<T> _domain;
};

}  // namespace opossum
