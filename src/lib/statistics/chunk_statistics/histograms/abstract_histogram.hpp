#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "storage/base_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Abstract class for various histogram types.
 * Provides logic for estimating cardinality and making pruning decisions.
 * Inheriting classes have to implement a variety of helper functions.
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
  AbstractHistogram(const std::string& supported_characters, const uint64_t string_prefix_length);
  virtual ~AbstractHistogram() = default;

  virtual HistogramType histogram_type() const = 0;

  /**
   * Returns a string with detailed information about the histogram, including the edges of the individual bins.
   */
  std::string description() const;

  /**
   * Returns a CSV formatted string with information about the bins.
   *
   * @param print_header Whether a header should be printed or not.
   * @param column_name The histogram does not know the column name of the data it represents.
   * If this is to be included it has to be passed as an argument.
   * @param requested_num_bins The histogram might create fewer bins than requested because, e.g., there are fewer
   * distinct values, and it does not store the originally requested number. That number is useful for comparison
   * and can thus be provided to be included.
   */
  std::string bins_to_csv(const bool print_header = true, const std::optional<std::string>& column_name = std::nullopt,
                          const std::optional<uint64_t>& requested_num_bins = std::nullopt) const;

  /**
   * Returns the estimated selectivity given a predicate type and its parameter(s).
   * It will always be between 0 and 1.
   */
  float estimate_selectivity(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                             const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Returns the estimated cardinality given a predicate type and its parameter(s).
   * It will always be between 0 and total_count().
   * This method is specialized for strings to handle predicates uniquely applicable to string columns.
   */
  float estimate_cardinality(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                             const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * Returns the estimated distinct count given a predicate type and its parameter(s).
   */
  float estimate_distinct_count(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
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
  T min() const;

  /**
   * Returns the upper bound (maximum value) of the histogram.
   * This is equal to the largest value in the segment.
   */
  T max() const;

  /**
   * Given a value return the next representable value.
   * This method is a wrapper for the functions in histogram_utils.
   */
  T get_next_value(const T value) const;

  /**
   * Returns the number of bins actually present in the histogram.
   * This number can differ from the number of bins requested when creating a histogram.
   */
  virtual size_t num_bins() const = 0;

  /**
   * Returns the number of values represented in the histogram.
   * This is equal to the length of the segment during creation, without null values.
   */
  virtual uint64_t total_count() const = 0;

  /**
   * Returns the number of distinct values represented in the histogram.
   * This is equal to the number of distinct values in the segment during creation.
   */
  virtual uint64_t total_count_distinct() const = 0;

 protected:
  /**
   * Returns a list of pairs of distinct values and their respective number of occurrences in a given segment.
   * The list is sorted by distinct value from lowest to highest.
   */
  static std::vector<std::pair<T, uint64_t>> _calculate_value_counts(const std::shared_ptr<const BaseSegment>& segment);

  /**
   * Given a map of distinct values to their respective number of occurrences,
   * return a list sorted by distinct value from lowest to highest and return it.
   */
  static std::vector<std::pair<T, uint64_t>> _sort_value_counts(const std::unordered_map<T, uint64_t>& value_counts);

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
   * Given a value return its numerical representation.
   * This method is a wrapper for the functions in histogram_utils.
   */
  uint64_t _convert_string_to_number_representation(const std::string& value) const;

  /**
   * Given a numerical representation of a string return the string.
   * This method is a wrapper for the functions in histogram_utils.
   */
  std::string _convert_number_representation_to_string(const uint64_t value) const;

  /**
   * Returns the share of values in a bin that are lower than `value`.
   * This method is specialized for strings.
   */
  float _bin_share(const BinID bin_id, const T value) const;

  /**
   * Returns the width of a bin.
   * This method does not work for strings. Use _string_bin_width instead.
   */
  virtual T _bin_width(const BinID index) const;

  /**
   * Returns the width of a bin in a string histogram.
   */
  uint64_t _string_bin_width(const BinID index) const;

  /**
   * Returns the id of the bin that holds the given `value`.
   * Returns INVALID_BIN_ID if `value` does not belong to any bin.
   */
  virtual BinID _bin_for_value(const T value) const = 0;

  /**
   * Returns the id of the bin after the one that holds the given `value`.
   * If `value` does not belong to any bin but is smaller than max(), it is in a gap.
   * In that case return the bin right after the gap.
   * If the bin that holds the value is the last bin or it is greater than max, return INVALID_BIN_ID.
   */
  virtual BinID _upper_bound_for_value(const T value) const = 0;

  /**
   * Returns the smallest value in the bin.
   */
  virtual T _bin_min(const BinID index) const = 0;

  /**
   * Returns the largest value in a bin.
   */
  virtual T _bin_max(const BinID index) const = 0;

  /**
   * Returns the number of values in a bin.
   */
  virtual uint64_t _bin_count(const BinID index) const = 0;

  /**
   * Returns the number of distinct values in a bin.
   */
  virtual uint64_t _bin_count_distinct(const BinID index) const = 0;

  // String histogram-specific members.
  // See general explanation for details.
  std::string _supported_characters;
  uint64_t _string_prefix_length;
};

}  // namespace opossum
