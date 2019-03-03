#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "storage/base_segment.hpp"
#include "types.hpp"

namespace opossum {

using BinID = size_t;
constexpr BinID INVALID_BIN_ID{std::numeric_limits<BinID>::max()};

/**
 * Every chunk can hold at most ChunkOffset values.
 * Consequently, there can be at most be ChunkOffset distinct values in a bin,
 * and a bin can have a maximum height of ChunkOffset.
 * We use this alias to automatically adapt if the type of ChunkOffset is changed.
 */
using HistogramCountType = ChunkOffset;

/**
 * Abstract class for various histogram types.
 * Provides logic for estimating cardinality and making pruning decisions.
 *
 * A histogram consists of a collection of bins.
 * These bins are responsible for a certain range of values, and the histogram stores,
 * in one way or another, the following information about the bins:
 *  - lower edge
 *  - upper edge
 *  - number of values
 *  - number of distinct values
 *
 * Histograms are supported for all five data column types we support.
 * String histograms, however, are implemented slightly different because of their non-numerical property.
 * Strings are converted to a numerical representation. This is only possible for strings of a fixed length.
 * The conversion is done in histogram_utils::convert_string_to_number_representation().
 * This length is stored in the member `_string_prefix_length`.
 * Additionally, as of now, we only support a range of ASCII characters, that is stored as a string
 * in `_supported_characters`. This range must not include gaps and the string has to be sorted.
 * The possible maximum length of the prefix depends on the number of supported characters.
 * It must not overflow the uint64_t data type used to represent strings as numbers.
 * The formula used to verify the prefix length is:
 * string_prefix_length < std::log(std::numeric_limits<uint64_t>::max()) / std::log(supported_characters.length() + 1)
 */
template <typename T>
class AbstractHistogram : public AbstractFilter {
 public:
  AbstractHistogram();
  AbstractHistogram(const pmr_string& supported_characters, const size_t string_prefix_length);
  ~AbstractHistogram() override = default;

  // Non-copyable
  AbstractHistogram(AbstractHistogram&&) = default;
  AbstractHistogram& operator=(AbstractHistogram&&) = default;
  AbstractHistogram(const AbstractHistogram&) = delete;
  const AbstractHistogram& operator=(const AbstractHistogram&) = delete;

  /**
   * Strings are internally transformed to a number, such that a bin can have a numerical width.
   * This transformation is based on uint64_t.
   */
  using HistogramWidthType = std::conditional_t<std::is_same_v<T, pmr_string>, uint64_t, T>;

  virtual HistogramType histogram_type() const = 0;
  virtual std::string histogram_name() const = 0;

  /**
   * Returns a string with detailed information about the histogram, including the edges of the individual bins.
   */
  std::string description() const;

  /**
   * Returns the estimated selectivity, given a predicate type and its parameter(s).
   * It will always be between 0 and 1.
   */
  float estimate_selectivity(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                             const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Returns the estimated cardinality, given a predicate type and its parameter(s).
   * It will always be between 0 and total_count().
   * This method is specialized for strings to handle predicates uniquely applicable to string columns.
   */
  float estimate_cardinality(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                             const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Returns whether a given predicate type and its parameter(s) can be pruned.
   * This method is specialized for strings to handle predicates uniquely applicable to string columns.
   */
  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  /**
   * Returns the lower bound (minimum value) of the histogram.
   * This is equal to the smallest value in the segment.
   */
  T minimum() const;

  /**
   * Returns the upper bound (maximum value) of the histogram.
   * This is equal to the largest value in the segment.
   */
  T maximum() const;

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

 protected:
  /**
   * Returns a list of pairs of distinct values and their respective number of occurrences in a given segment.
   * The list is sorted by distinct value from lowest to highest.
   */
  static std::vector<std::pair<T, HistogramCountType>> _gather_value_distribution(
      const std::shared_ptr<const BaseSegment>& segment);

  /**
   * Calculates the estimated cardinality for predicate types supported by all data types.
   */
  float _estimate_cardinality(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                              const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Makes pruning decisions for predicate types supported by all data types.
   */
  bool _can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                  const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Given a value, returns the next representable value.
   * This method is a wrapper for the functions in histogram_utils.
   */
  T _get_next_value(const T value) const;

  /**
   * Given a value, returns its numerical representation.
   * This method is a wrapper for the functions in histogram_utils.
   */
  uint64_t _convert_string_to_number_representation(const pmr_string& value) const;

  /**
   * Given a numerical representation of a string, returns the string.
   * This method is a wrapper for the functions in histogram_utils.
   */
  pmr_string _convert_number_representation_to_string(const uint64_t value) const;

  /**
   * Returns the share of values in a bin that are smaller than `value`.
   * This method is specialized for strings.
   */
  double _share_of_bin_less_than_value(const BinID bin_id, const T value) const;

  /**
   * Returns the width of a bin.
   * This method is specialized for strings to return a numerical width.
   */
  virtual HistogramWidthType _bin_width(const BinID index) const;

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

  /**
   * Returns the smallest value in the bin.
   */
  virtual T _bin_minimum(const BinID index) const = 0;

  /**
   * Returns the largest value in a bin.
   */
  virtual T _bin_maximum(const BinID index) const = 0;

  /**
   * Returns the number of values in a bin.
   */
  virtual HistogramCountType _bin_height(const BinID index) const = 0;

  /**
   * Returns the number of distinct values in a bin.
   */
  virtual HistogramCountType _bin_distinct_count(const BinID index) const = 0;

  // String histogram-specific members.
  // See general explanation for details.
  pmr_string _supported_characters;
  size_t _string_prefix_length;
};

}  // namespace opossum
