#include "abstract_histogram.hpp"

#include <cmath>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "expression/evaluation/like_matcher.hpp"
#include "generic_histogram.hpp"
#include "histogram_utils.hpp"
#include "single_bin_histogram.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "statistics/statistics_utils.hpp"
#include "storage/create_iterable_from_segment.hpp"

#include "resolve_type.hpp"

namespace opossum {

using namespace opossum::histogram;  // NOLINT

template <typename T>
AbstractHistogram<T>::AbstractHistogram()
    : AbstractStatisticsObject(data_type_from_type<T>()), _supported_characters(""), _string_prefix_length(0ul) {}

template <>
AbstractHistogram<std::string>::AbstractHistogram() : AbstractStatisticsObject(DataType::String) {
  const auto pair = get_default_or_check_string_histogram_prefix_settings();
  _supported_characters = pair.first;
  _string_prefix_length = pair.second;
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::string& supported_characters,
                                                  const size_t string_prefix_length)
    : AbstractStatisticsObject(DataType::String),
      _supported_characters(supported_characters),
      _string_prefix_length(string_prefix_length) {
  Assert(check_prefix_settings(_supported_characters, _string_prefix_length), "Invalid prefix settings.");
}

template <typename T>
std::string AbstractHistogram<T>::description(const bool include_bin_info) const {
  std::stringstream stream;
  stream << histogram_name();
  stream << " distinct: " << total_distinct_count();
  stream << " min: " << minimum();
  stream << " max: " << maximum();
  stream << " bins: " << bin_count();

  if (include_bin_info) {
    stream << "  edges / counts " << std::endl;
    for (BinID bin = 0u; bin < bin_count(); bin++) {
      stream << "              [" << bin_minimum(bin) << " -> " << bin_maximum(bin) << "]: ";
      stream << "Height: " << bin_height(bin) << "; DistinctCount: " << bin_distinct_count(bin) << std::endl;
    }
  }

  return stream.str();
}

template <typename T>
std::vector<std::pair<T, HistogramCountType>> AbstractHistogram<T>::_gather_value_distribution(
    const std::shared_ptr<const BaseSegment>& segment) {
  std::map<T, HistogramCountType> value_counts;

  resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
    auto iterable = create_iterable_from_segment<T>(typed_segment);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        value_counts[value.value()]++;
      }
    });
  });

  std::vector<std::pair<T, HistogramCountType>> result(value_counts.cbegin(), value_counts.cend());
  return result;
}

template <typename T>
T AbstractHistogram<T>::minimum() const {
  return bin_minimum(0u);
}

template <typename T>
T AbstractHistogram<T>::maximum() const {
  return bin_maximum(bin_count() - 1u);
}

template <>
uint64_t AbstractHistogram<std::string>::_convert_string_to_number_representation(const std::string& value) const {
  return convert_string_to_number_representation(value, _supported_characters, _string_prefix_length);
}

template <>
std::string AbstractHistogram<std::string>::_convert_number_representation_to_string(const uint64_t value) const {
  return convert_number_representation_to_string(value, _supported_characters, _string_prefix_length);
}

template <typename T>
typename AbstractHistogram<T>::HistogramWidthType AbstractHistogram<T>::_bin_width(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _get_next_value(bin_maximum(index) - bin_minimum(index));
}

template <>
AbstractHistogram<std::string>::HistogramWidthType AbstractHistogram<std::string>::_bin_width(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  const auto repr_min = _convert_string_to_number_representation(bin_minimum(index));
  const auto repr_max = _convert_string_to_number_representation(bin_maximum(index));
  return repr_max - repr_min + 1u;
}

template <typename T>
T AbstractHistogram<T>::_get_next_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    return next_value(value, _supported_characters);
  } else {
    return next_value(value);
  }
}

template <typename T>
double AbstractHistogram<T>::_share_of_bin_less_than_value(const BinID bin_id, const T value) const {
  /**
   * Returns the share of values smaller than `value` in the given bin.
   *
   * We need to convert strings to their numerical representation to calculate a share.
   * This conversion is done based on prefixes because strings of arbitrary length cannot be converted to a numerical
   * representation that satisfies the following requirements:
   *  1. For two strings s1 and s2: s1 < s2 -> repr(s1) < repr(s2)
   *  2. For two strings s1 and s2: dist(s1, s2) == repr(s2) - repr(s1)
   *  repr(s) is the numerical representation for a string s, and dist(s1, s2) returns the number of strings between
   *  s1 and s2 in the domain of strings with at most length `string_prefix_length`
   *  and the set of supported characters `supported_characters`.
   *
   * Thus, we calculate the range based only on a domain of strings with a maximum length of `string_prefix_length`
   * characters.
   * However, we make use of a trick: if the bin edges share a common prefix, we strip that common prefix and
   * take the substring starting after that prefix.
   *
   * Example:
   *  - bin: ["intelligence", "intellij"]
   *  - supported_characters: [a-z]
   *  - string_prefix_length: 4
   *  - value: intelligent
   *
   *  Traditionally, if we did not strip the common prefix, we would calculate the range based on the
   *  substring of length `string_prefix_length`, which is "inte" for both lower and upper edge of the bin.
   *  We could not make a reasonable assumption how large the share is.
   *  Instead, we strip the common prefix ("intelli") and calculate the share based on the numerical representation
   *  of the substring after the common prefix.
   *  That is, what is the share of values smaller than "gent" in the range ["gence", "j"]?
   */
  if constexpr (!std::is_same_v<T, std::string>) {
    return static_cast<double>(value - bin_minimum(bin_id)) / _bin_width(bin_id);
  } else {
    const auto bin_min = bin_minimum(bin_id);
    const auto bin_max = bin_maximum(bin_id);
    const auto common_prefix_len = common_prefix_length(bin_min, bin_max);

    DebugAssert(value.substr(0, common_prefix_len) == bin_min.substr(0, common_prefix_len),
                "Value does not belong to bin");

    const auto value_repr = _convert_string_to_number_representation(value.substr(common_prefix_len));
    const auto min_repr = _convert_string_to_number_representation(bin_min.substr(common_prefix_len));
    const auto max_repr = _convert_string_to_number_representation(bin_max.substr(common_prefix_len));
    const auto bin_share = static_cast<double>(value_repr - min_repr) / (max_repr - min_repr + 1);

    // bin_share == 1.0f can only happen due to floating point arithmetic inaccuracies
    if (bin_share == 1.0f) {
      return previous_value(1.0f);
    } else {
      return bin_share;
    }
  }
}

template <typename T>
bool AbstractHistogram<T>::_general_does_not_contain(const PredicateCondition predicate_type,
                                                     const AllTypeVariant& variant_value,
                                                     const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast_variant<T>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto bin_id = _bin_for_value(value);
      // It is possible for EqualWidthHistograms to have empty bins.
      return bin_id == INVALID_BIN_ID || bin_height(bin_id) == 0ul;
    }
    case PredicateCondition::NotEquals:
      return minimum() == value && maximum() == value;
    case PredicateCondition::LessThan:
      return value <= minimum();
    case PredicateCondition::LessThanEquals:
      return value < minimum();
    case PredicateCondition::GreaterThanEquals:
      return value > maximum();
    case PredicateCondition::GreaterThan:
      return value >= maximum();
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");

      if (_does_not_contain(PredicateCondition::GreaterThanEquals, value)) {
        return true;
      }

      const auto value2 = type_cast_variant<T>(*variant_value2);
      if (_does_not_contain(PredicateCondition::LessThanEquals, value2) || value2 < value) {
        return true;
      }

      const auto value_bin = _bin_for_value(value);
      const auto value2_bin = _bin_for_value(value2);

      // In an EqualDistinctCountHistogram, if both values fall into the same gap, we can prune the predicate.
      // We need to have at least two bins to rule out pruning if value < min and value2 > max.
      if (value_bin == INVALID_BIN_ID && value2_bin == INVALID_BIN_ID && bin_count() > 1ul &&
          _next_bin_for_value(value) == _next_bin_for_value(value2)) {
        return true;
      }

      // In an EqualWidthHistogram, if both values fall into a bin that has no elements,
      // and there are either no bins in between or none of them have any elements, we can also prune the predicate.
      if (value_bin != INVALID_BIN_ID && value2_bin != INVALID_BIN_ID && bin_height(value_bin) == 0 &&
          bin_height(value2_bin) == 0) {
        for (auto current_bin = value_bin + 1; current_bin < value2_bin; current_bin++) {
          if (bin_height(current_bin) > 0ul) {
            return false;
          }
        }
        return true;
      }

      return false;
    }
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      Fail("Predicate (NOT) LIKE is not supported for non-string columns.");
    default:
      // Do not prune predicates we cannot (yet) handle.
      return false;
  }
}

template <typename T>
bool AbstractHistogram<T>::_does_not_contain(const PredicateCondition predicate_type,
                                             const AllTypeVariant& variant_value,
                                             const std::optional<AllTypeVariant>& variant_value2) const {
  return _general_does_not_contain(predicate_type, variant_value, variant_value2);
}

template <>
bool AbstractHistogram<std::string>::_does_not_contain(const PredicateCondition predicate_type,
                                                       const AllTypeVariant& variant_value,
                                                       const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast_variant<std::string>(variant_value);

  // Only allow supported characters in search value.
  // If predicate is (NOT) LIKE additionally allow wildcards.
  const auto allowed_characters =
      _supported_characters +
      (predicate_type == PredicateCondition::Like || predicate_type == PredicateCondition::NotLike ? "_%" : "");
  Assert(value.find_first_not_of(allowed_characters) == std::string::npos, "Unsupported characters.");

  switch (predicate_type) {
    case PredicateCondition::Like: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return _does_not_contain(PredicateCondition::Equals, value);
      }

      // If the pattern starts with a MatchAll, we can not prune it.
      if (value.front() == '%') {
        return false;
      }

      /**
       * We can prune prefix searches iff the domain of values captured by a prefix pattern is prunable.
       *
       * Example:
       * bins: [a, b], [d, e]
       * predicate: col LIKE 'c%'
       *
       * With the same argument we can also prune predicates in the form of 'c%foo',
       * where foo can be any pattern itself.
       * We only have to consider the pattern up to the first AnyChars wildcard.
       */
      const auto match_all_index = value.find('%');
      if (match_all_index != std::string::npos) {
        const auto search_prefix = value.substr(0, match_all_index);
        if (_does_not_contain(PredicateCondition::GreaterThanEquals, search_prefix)) {
          return true;
        }

        const auto search_prefix_next_value = next_value(search_prefix, _supported_characters, search_prefix.length());

        // If the next value is the same as the prefix, it means that there is no larger value in the domain
        // of substrings. In that case we cannot prune, because otherwise the previous check would already return true.
        if (search_prefix == search_prefix_next_value) {
          return false;
        }

        if (_does_not_contain(PredicateCondition::LessThan, search_prefix_next_value)) {
          return true;
        }

        const auto search_prefix_bin = _bin_for_value(search_prefix);
        const auto search_prefix_next_value_bin = _bin_for_value(search_prefix_next_value);

        if (search_prefix_bin == INVALID_BIN_ID) {
          const auto search_prefix_next_bin = _next_bin_for_value(search_prefix);

          // In an EqualDistinctCountHistogram, if both values fall into the same gap, we can prune the predicate.
          // We need to have at least two bins to rule out pruning if search_prefix < min
          // and search_prefix_next_value > max.
          if (search_prefix_next_value_bin == INVALID_BIN_ID && bin_count() > 1ul &&
              search_prefix_next_bin == _next_bin_for_value(search_prefix_next_value)) {
            return true;
          }

          // In an EqualDistinctCountHistogram, if the search_prefix_next_value is exactly the lower bin edge of
          // the upper bound of search_prefix, we can also prune.
          // That's because search_prefix_next_value does not belong to the range covered by the pattern,
          // but is the next value after it.
          if (search_prefix_next_value_bin != INVALID_BIN_ID &&
              search_prefix_next_bin == search_prefix_next_value_bin &&
              bin_minimum(search_prefix_next_value_bin) == search_prefix_next_value) {
            return true;
          }
        }

        // In an EqualWidthHistogram, if both values fall into a bin that has no elements,
        // and there are either no bins in between or none of them have any elements, we can also prune the predicate.
        // If the count of search_prefix_next_value_bin is not 0 but search_prefix_next_value is the lower bin edge,
        // we can still prune, because search_prefix_next_value is not part of the range (same as above).
        if (search_prefix_bin != INVALID_BIN_ID && search_prefix_next_value_bin != INVALID_BIN_ID &&
            bin_height(search_prefix_bin) == 0u &&
            (bin_height(search_prefix_next_value_bin) == 0u ||
             bin_minimum(search_prefix_next_value_bin) == search_prefix_next_value)) {
          for (auto current_bin = search_prefix_bin + 1; current_bin < search_prefix_next_value_bin; current_bin++) {
            if (bin_height(current_bin) > 0u) {
              return false;
            }
          }
          return true;
        }

        return false;
      }

      return false;
    }
    case PredicateCondition::NotLike: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return _does_not_contain(PredicateCondition::NotEquals, variant_value);
      }

      // If the pattern starts with a MatchAll, we can only prune it if it matches all values.
      if (value.front() == '%') {
        return value == "%";
      }

      /**
       * We can also prune prefix searches iff the domain of values captured by the histogram is less than or equal to
       * the domain of strings captured by a prefix pattern.
       *
       * Example:
       * min: car
       * max: crime
       * predicate: col NOT LIKE 'c%'
       *
       * With the same argument we can also prune predicates in the form of 'c%foo',
       * where foo can be any pattern itself.
       * We only have to consider the pattern up to the first MatchAll character.
       */
      const auto match_all_index = value.find('%');
      if (match_all_index != std::string::npos) {
        const auto search_prefix = value.substr(0, match_all_index);
        if (search_prefix == minimum().substr(0, search_prefix.length()) &&
            search_prefix == maximum().substr(0, search_prefix.length())) {
          return true;
        }
      }

      return false;
    }
    default:
      return _general_does_not_contain(predicate_type, variant_value, variant_value2);
  }
}

template <typename T>
CardinalityEstimate AbstractHistogram<T>::_estimate_cardinality(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {Cardinality{0}, EstimateType::MatchesNone};
  }

  const auto value = type_cast_variant<T>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto index = _bin_for_value(value);
      const auto bin_count_distinct = bin_distinct_count(index);

      // This should never be false because does_not_contain should have been true further up if this was the case.
      DebugAssert(bin_count_distinct > 0u, "0 distinct values in bin.");

      return {static_cast<Cardinality>(bin_height(index)) / bin_count_distinct,
              bin_count_distinct == 1u ? EstimateType::MatchesExactly : EstimateType::MatchesApproximately};
    }
    case PredicateCondition::NotEquals:
      return invert_estimate(_estimate_cardinality(PredicateCondition::Equals, variant_value));
    case PredicateCondition::LessThan: {
      if (value > maximum()) {
        return {static_cast<Cardinality>(total_count()), EstimateType::MatchesAll};
      }

      // This should never be false because does_not_contain should have been true further up if this was the case.
      DebugAssert(value >= minimum(), "Value smaller than min of histogram.");

      auto cardinality = Cardinality{0};
      auto estimate_type = EstimateType::MatchesApproximately;
      auto index = _bin_for_value(value);

      if (index == INVALID_BIN_ID) {
        // The value is within the range of the histogram, but does not belong to a bin.
        // Therefore, we need to sum up the counts of all bins with a max < value.
        index = _next_bin_for_value(value);
        estimate_type = EstimateType::MatchesExactly;
      } else if (value == bin_minimum(index) || bin_height(index) == 0u) {
        // If the value is exactly the lower bin edge or the bin is empty,
        // we do not have to add anything of that bin and know the cardinality exactly.
        estimate_type = EstimateType::MatchesExactly;
      } else {
        cardinality += static_cast<float>(_share_of_bin_less_than_value(index, value)) * bin_height(index);
      }

      DebugAssert(index != INVALID_BIN_ID, "Should have been caught by _does_not_contain().");

      // Sum up all bins before the bin (or gap) containing the value.
      for (BinID bin = 0u; bin < index; bin++) {
        cardinality += bin_height(bin);
      }

      /**
       * The cardinality is capped at total_count().
       * It is possible for a value that is smaller than or equal to the max of the EqualHeightHistogram
       * to yield a calculated cardinality higher than total_count.
       * This is due to the way EqualHeightHistograms store the count for a bin,
       * which is in a single value (count_per_bin) for all bins rather than a vector (one value for each bin).
       * Consequently, this value is the desired count for all bins.
       * In practice, _bin_count(n) >= _count_per_bin for n < bin_count() - 1,
       * because bins are filled up until the count is at least _count_per_bin.
       * The last bin typically has a count lower than _count_per_bin.
       * Therefore, if we calculate the share of the last bin based on _count_per_bin
       * we might end up with an estimate higher than total_count(), which is then capped.
       */
      return {std::min(cardinality, static_cast<Cardinality>(total_count())), estimate_type};
    }
    case PredicateCondition::LessThanEquals:
      return estimate_cardinality(PredicateCondition::LessThan, _get_next_value(value));
    case PredicateCondition::GreaterThanEquals:
      return invert_estimate(estimate_cardinality(PredicateCondition::LessThan, variant_value));
    case PredicateCondition::GreaterThan:
      return invert_estimate(estimate_cardinality(PredicateCondition::LessThanEquals, variant_value));
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast_variant<T>(*variant_value2);

      if (value2 < value) {
        return {Cardinality{0}, EstimateType::MatchesNone};
      }

      const auto estimate_lt_value = estimate_cardinality(PredicateCondition::LessThan, variant_value);
      const auto estimate_lte_value2 = estimate_cardinality(PredicateCondition::LessThanEquals, *variant_value2);

      if (estimate_lt_value.type == EstimateType::MatchesAll) {
        DebugAssert(estimate_lte_value2.type == EstimateType::MatchesAll, "Estimate types do not match.");
        return {static_cast<Cardinality>(total_count()), EstimateType::MatchesAll};
      }

      if (estimate_lt_value.type == EstimateType::MatchesNone) {
        DebugAssert(estimate_lte_value2.type != EstimateType::MatchesNone,
                    "Should have been caught by _does_not_contain().");
        return {estimate_lte_value2.cardinality, estimate_lte_value2.type};
      }

      if (estimate_lte_value2.type == EstimateType::MatchesNone) {
        DebugAssert(estimate_lt_value.type == EstimateType::MatchesNone, "Estimate types do not match.");
        return {Cardinality{0}, EstimateType::MatchesNone};
      }

      /**
       * If either estimate type is approximate, the whole result is approximate.
       * All other estimate types are exact (including MatchesNone and MatchesAll),
       * so the result estimate type is exact as well.
       */
      const auto estimate_type = estimate_lt_value.type == EstimateType::MatchesApproximately ||
                                         estimate_lte_value2.type == EstimateType::MatchesApproximately
                                     ? EstimateType::MatchesApproximately
                                     : EstimateType::MatchesExactly;

      return {estimate_lte_value2.cardinality - estimate_lt_value.cardinality, estimate_type};
    }
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      Fail("Predicate NOT LIKE is not supported for non-string columns.");
    default:
      // TODO(anyone): implement more meaningful things here
      return {static_cast<Cardinality>(total_count()), EstimateType::MatchesApproximately};
  }
}

// Specialization for numbers.
template <typename T>
CardinalityEstimate AbstractHistogram<T>::estimate_cardinality(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  return _estimate_cardinality(predicate_type, variant_value, variant_value2);
}

// Specialization for strings.
template <>
CardinalityEstimate AbstractHistogram<std::string>::estimate_cardinality(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast_variant<std::string>(variant_value);

  // Only allow supported characters in search value.
  // If predicate is (NOT) LIKE additionally allow wildcards.
  const auto allowed_characters =
      _supported_characters +
      (predicate_type == PredicateCondition::Like || predicate_type == PredicateCondition::NotLike ? "_%" : "");
  Assert(value.find_first_not_of(allowed_characters) == std::string::npos, "Unsupported characters.");

  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {Cardinality{0}, EstimateType::MatchesNone};
  }

  switch (predicate_type) {
    case PredicateCondition::Like: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return estimate_cardinality(PredicateCondition::Equals, variant_value);
      }

      // We don't deal with this for now because it is not worth the effort.
      // TODO(anyone): think about good way to handle SingleChar wildcard in patterns.
      const auto single_char_count = std::count(value.cbegin(), value.cend(), '_');
      if (single_char_count > 0u) {
        return {static_cast<Cardinality>(total_count()), EstimateType::MatchesApproximately};
      }

      const auto any_chars_count = std::count(value.cbegin(), value.cend(), '%');
      DebugAssert(any_chars_count > 0u,
                  "contains_wildcard() should not return true if there is neither a '%' nor a '_' in the string.");

      // Match everything.
      if (value == "%") {
        return {static_cast<Cardinality>(total_count()), EstimateType::MatchesAll};
      }

      if (value.front() != '%') {
        /**
         * We know now we have some sort of prefix search, because there is at least one AnyChars wildcard,
         * and it is not at the start of the pattern.
         *
         * We differentiate two cases:
         *  1. Simple prefix searches, e.g., 'foo%', where there is exactly one AnyChars wildcard in the pattern,
         *  and it is at the end of the pattern.
         *  2. All others, e.g., 'foo%bar' or 'foo%bar%'.
         *
         *  The way we handle these cases is we only estimate simple prefix patterns and assume uniform distribution
         *  for additional fixed characters for the second case.
         *  Note: this is obviously far from great because not only do characters not appear with equal probability,
         *  they also appear with different probability depending on characters around them.
         *  The combination 'ing' in English is far more likely than 'qzy'.
         *  One improvement would be to have a frequency table for characters and take the probability from there,
         *  but it only gets you so far. It does not help with the second property.
         *  Nevertheless, it could be helpful especially if the number of actually occurring characters in a column are
         *  small compared to the supported characters and the frequency table would be not static but built during
         *  histogram generation.
         *  TODO(anyone): look into that in more detail.
         *
         *  That is, to estimate the first case ('foo%'), we calculate
         *  estimate_cardinality(LessThan, fop) - estimate_cardinaliy(LessThan, foo).
         *  That covers all strings starting with foo.
         *
         *  In the second case we assume that all characters in _supported_characters are equally likely to appear in
         *  a string, and therefore divide the above cardinality by the number of supported characters for each
         *  additional character that is fixed in the string after the prefix.
         *
         *  Example for 'foo%bar%baz', if we only supported the 26 lowercase latin characters:
         *  (estimate_cardinality(LessThan, fop) - estimate_cardinality(LessThan, foo)) / 26^6
         *  There are six additional fixed characters in the string ('b', 'a', 'r', 'b', 'a', and 'z').
         */
        const auto search_prefix = value.substr(0, value.find('%'));
        auto additional_characters = value.length() - search_prefix.length() - any_chars_count;

        // If there are too many fixed characters for the power to be calculated without overflow, cap the exponent.
        const auto maximum_exponent =
            std::log(std::numeric_limits<uint64_t>::max()) / std::log(_supported_characters.length());
        if (additional_characters > maximum_exponent) {
          additional_characters = static_cast<uint64_t>(maximum_exponent);
        }

        const auto search_prefix_next_value = next_value(search_prefix, _supported_characters, search_prefix.length());

        // If the next value is the same as the prefix, it means that there is no larger value in the domain
        // of substrings. In that case all values (total_count()) are smaller than search_prefix_next_value.
        const auto count_smaller_next_value =
            search_prefix == search_prefix_next_value
                ? total_count()
                : estimate_cardinality(PredicateCondition::LessThan, search_prefix_next_value).cardinality;

        return {
            (count_smaller_next_value - estimate_cardinality(PredicateCondition::LessThan, search_prefix).cardinality) /
                ipow(_supported_characters.length(), additional_characters),
            EstimateType::MatchesApproximately};
      }

      /**
       * If we do not have a prefix search, but a suffix or contains search, the prefix histograms do not help us.
       * We simply assume uniform distribution for all supported characters and divide the total number of rows
       * by the number of supported characters for each additional character that is fixed (see comment above).
       *
       * Example for '%foo%b%a%', if we only supported the 26 lowercase latin characters:
       * total_count() / 26^5
       * There are five fixed characters in the string ('f', 'o', 'o', 'b', and 'a').
       */
      const auto fixed_characters = value.length() - any_chars_count;
      return {static_cast<Cardinality>(total_count()) / ipow(_supported_characters.length(), fixed_characters),
              EstimateType::MatchesApproximately};
    }
    case PredicateCondition::NotLike: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return estimate_cardinality(PredicateCondition::NotEquals, variant_value);
      }

      // We don't deal with this for now because it is not worth the effort.
      // TODO(anyone): think about good way to handle SingleChar wildcard in patterns.
      const auto single_char_count = std::count(value.cbegin(), value.cend(), '_');
      if (single_char_count > 0u) {
        return {static_cast<Cardinality>(total_count()), EstimateType::MatchesApproximately};
      }

      return invert_estimate(estimate_cardinality(PredicateCondition::Like, variant_value));
    }
    default:
      return _estimate_cardinality(predicate_type, variant_value, variant_value2);
  }
}

template <typename T>
float AbstractHistogram<T>::estimate_distinct_count(const PredicateCondition predicate_type,
                                                    const AllTypeVariant& variant_value,
                                                    const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return 0.f;
  }

  const auto value = type_cast_variant<T>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      return 1.f;
    }
    case PredicateCondition::NotEquals: {
      if (_bin_for_value(value) == INVALID_BIN_ID) {
        return total_distinct_count();
      }

      return total_distinct_count() - 1.f;
    }
    case PredicateCondition::LessThan: {
      if (value > maximum()) {
        return total_distinct_count();
      }

      auto distinct_count = 0.f;
      auto bin_id = _bin_for_value(value);
      if (bin_id == INVALID_BIN_ID) {
        // The value is within the range of the histogram, but does not belong to a bin.
        // Therefore, we need to sum up the distinct counts of all bins with a max < value.
        bin_id = _next_bin_for_value(value);
      } else {
        distinct_count += _share_of_bin_less_than_value(bin_id, value) * bin_distinct_count(bin_id);
      }

      // Sum up all bins before the bin (or gap) containing the value.
      for (BinID bin = 0u; bin < bin_id; bin++) {
        distinct_count += bin_distinct_count(bin);
      }

      return distinct_count;
    }
    case PredicateCondition::LessThanEquals:
      return estimate_distinct_count(PredicateCondition::LessThan, _get_next_value(value));
    case PredicateCondition::GreaterThanEquals:
      return total_distinct_count() - estimate_distinct_count(PredicateCondition::LessThan, value);
    case PredicateCondition::GreaterThan:
      return total_distinct_count() - estimate_distinct_count(PredicateCondition::LessThanEquals, value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast_variant<T>(*variant_value2);

      if (value2 < value) {
        return 0.f;
      }

      return estimate_distinct_count(PredicateCondition::LessThanEquals, value2) -
             estimate_distinct_count(PredicateCondition::LessThan, value);
    }
    // TODO(tim): implement like
    // case PredicateCondition::Like:
    // case PredicateCondition::NotLike:
    // TODO(anyone): implement more meaningful things here
    default:
      return total_distinct_count();
  }
}

template <typename T>
CardinalityEstimate AbstractHistogram<T>::invert_estimate(const CardinalityEstimate& estimate) const {
  switch (estimate.type) {
    case EstimateType::MatchesNone:
      return {static_cast<Cardinality>(total_count()), EstimateType::MatchesAll};
    case EstimateType::MatchesAll:
      return {Cardinality{0}, EstimateType::MatchesNone};
    case EstimateType::MatchesExactly:
    case EstimateType::MatchesApproximately:
      return {total_count() - estimate.cardinality, estimate.type};
    default:
      Fail("EstimateType not supported.");
  }
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::slice_with_predicate(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return std::make_shared<EmptyStatisticsObject>(data_type);
  }

  const auto value = type_cast_variant<T>(variant_value);

  std::vector<T> bin_minima;
  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_heights;
  std::vector<HistogramCountType> bin_distinct_counts;

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      bin_minima.emplace_back(value);
      bin_maxima.emplace_back(value);
      bin_heights.emplace_back(static_cast<HistogramCountType>(
          std::ceil(estimate_cardinality(PredicateCondition::Equals, variant_value).cardinality)));
      bin_distinct_counts.emplace_back(1);
    } break;

    case PredicateCondition::NotEquals: {
      const auto value_bin_id = _bin_for_value(value);
      if (value_bin_id == INVALID_BIN_ID) return clone();

      // Do not create empty bin.
      const auto new_bin_count = bin_distinct_count(value_bin_id) == 1u ? bin_count() - 1 : bin_count();

      bin_minima.resize(new_bin_count);
      bin_maxima.resize(new_bin_count);
      bin_heights.resize(new_bin_count);
      bin_distinct_counts.resize(new_bin_count);

      auto old_bin_id = BinID{0};
      auto new_bin_id = BinID{0};
      for (; old_bin_id < bin_count(); ++old_bin_id) {
        // TODO(anyone) we currently do not manipulate the bin bounds if `variant_value` equals such a bound. We would
        //              expect the accuracy improvement to be minimal, if we did. Also, this would be hard to do for
        //              strings.
        bin_minima[new_bin_id] = bin_minimum(old_bin_id);
        bin_maxima[new_bin_id] = bin_maximum(old_bin_id);

        if (old_bin_id == value_bin_id) {
          const auto distinct_count = bin_distinct_count(old_bin_id);

          // Do not create empty bin.
          if (distinct_count == 1) {
            continue;
          }

          const auto value_count =
              estimate_cardinality(PredicateCondition::Equals, variant_value, variant_value2).cardinality;

          bin_heights[new_bin_id] = static_cast<HistogramCountType>(std::ceil(bin_height(old_bin_id) - value_count));
          bin_distinct_counts[new_bin_id] = distinct_count - 1;
        } else {
          bin_heights[new_bin_id] = bin_height(old_bin_id);
          bin_distinct_counts[new_bin_id] = bin_distinct_count(old_bin_id);
        }

        ++new_bin_id;
      }
    } break;

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals: {
      const auto bin_for_value = _bin_for_value(value);

      BinID sliced_bin_count;
      if (bin_for_value == INVALID_BIN_ID) {
        // If the value does not belong to a bin, we need to differentiate between values greater than the maximum
        // of the histogram and all other values. If the value is greater than the maximum, return a copy of itself.
        // Otherwise, we include all bins before to the bin of that value.
        const auto next_bin_for_value = _next_bin_for_value(value);

        if (next_bin_for_value == INVALID_BIN_ID) {
          return clone();
        } else {
          sliced_bin_count = next_bin_for_value;
        }
      } else if (predicate_type == PredicateCondition::LessThan && value == bin_minimum(bin_for_value)) {
        // If the predicate is LessThan and the value is the lower edge of a bin, we do not need to include that bin.
        sliced_bin_count = bin_for_value;
      } else {
        sliced_bin_count = bin_for_value + 1;
      }

      DebugAssert(sliced_bin_count > 0, "This should have been caught by _does_not_contain().");

      bin_minima.resize(sliced_bin_count);
      bin_maxima.resize(sliced_bin_count);
      bin_heights.resize(sliced_bin_count);
      bin_distinct_counts.resize(sliced_bin_count);

      // If value is not in a gap, calculate the share of the last bin to slice, and write it to back of the vectors.
      // Otherwise take the whole bin later.
      auto last_sliced_bin_id = sliced_bin_count;
      if (value < bin_maximum(last_sliced_bin_id - 1)) {
        last_sliced_bin_id--;
        bin_minima.back() = bin_minimum(last_sliced_bin_id);
        // TODO(anyone): this could be previous_value(value) for LessThan, but this is not available for strings
        // and we do not expect it to make a big difference.
        bin_maxima.back() = value;

        const auto less_than_bound = predicate_type == PredicateCondition::LessThan ? value : _get_next_value(value);
        const auto sliced_bin_share = _share_of_bin_less_than_value(last_sliced_bin_id, less_than_bound);
        bin_heights.back() =
            static_cast<HistogramCountType>(std::ceil(bin_height(last_sliced_bin_id) * sliced_bin_share));
        bin_distinct_counts.back() =
            static_cast<HistogramCountType>(std::ceil(bin_distinct_count(last_sliced_bin_id) * sliced_bin_share));
      }

      for (auto bin_id = BinID{0}; bin_id < last_sliced_bin_id; ++bin_id) {
        bin_minima[bin_id] = bin_minimum(bin_id);
        bin_maxima[bin_id] = bin_maximum(bin_id);
        bin_heights[bin_id] = bin_height(bin_id);
        bin_distinct_counts[bin_id] = bin_distinct_count(bin_id);
      }
    } break;

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      const auto bin_for_value = _bin_for_value(value);

      BinID sliced_bin_count;
      if (bin_for_value == INVALID_BIN_ID) {
        // If the value does not belong to a bin, we need to differentiate between values greater than the maximum
        // of the histogram and all other values. If the value is greater than the maximum, we have no matches.
        // Otherwise, we include all bins before the bin of that value.
        const auto next_bin_for_value = _next_bin_for_value(value);

        if (next_bin_for_value == INVALID_BIN_ID) {
          sliced_bin_count = 0;
        } else if (next_bin_for_value == 0) {
          return clone();
        } else {
          sliced_bin_count = bin_count() - next_bin_for_value;
        }
      } else if (predicate_type == PredicateCondition::GreaterThan && value == bin_maximum(bin_for_value)) {
        // If the predicate is GreaterThan and the value is the upper edge of a bin, we do not need to include that bin.
        sliced_bin_count = bin_count() - bin_for_value - 1;
      } else {
        sliced_bin_count = bin_count() - bin_for_value;
      }

      DebugAssert(sliced_bin_count > 0, "This should have been caught by _does_not_contain().");

      const auto first_sliced_bin_id = BinID{bin_count() - sliced_bin_count};

      bin_minima.resize(sliced_bin_count);
      bin_maxima.resize(sliced_bin_count);
      bin_heights.resize(sliced_bin_count);
      bin_distinct_counts.resize(sliced_bin_count);

      bin_maxima.front() = bin_maximum(first_sliced_bin_id);

      // If value is not in a gap, calculate the share of the bin to slice. Otherwise take the whole bin.
      if (value > bin_minimum(first_sliced_bin_id)) {
        bin_minima.front() = predicate_type == PredicateCondition::GreaterThan ? _get_next_value(value) : value;

        // For GreaterThan, `_get_previous_value(value)` would be more correct, but we don't have that for strings
        const auto sliced_bin_share = 1.0f - _share_of_bin_less_than_value(first_sliced_bin_id, value);

        bin_heights.front() =
            static_cast<HistogramCountType>(std::ceil(bin_height(first_sliced_bin_id) * sliced_bin_share));
        bin_distinct_counts.front() =
            static_cast<HistogramCountType>(std::ceil(bin_distinct_count(first_sliced_bin_id) * sliced_bin_share));

      } else {
        bin_minima.front() = bin_minimum(first_sliced_bin_id);
        bin_heights.front() = bin_height(first_sliced_bin_id);
        bin_distinct_counts.front() = bin_distinct_count(first_sliced_bin_id);
      }

      const auto first_complete_bin_id = BinID{bin_count() - sliced_bin_count + 1};
      for (auto bin_id = first_complete_bin_id; bin_id < bin_count(); ++bin_id) {
        const auto sliced_bin_id = bin_id - first_complete_bin_id + 1;
        bin_minima[sliced_bin_id] = bin_minimum(bin_id);
        bin_maxima[sliced_bin_id] = bin_maximum(bin_id);
        bin_heights[sliced_bin_id] = bin_height(bin_id);
        bin_distinct_counts[sliced_bin_id] = bin_distinct_count(bin_id);
      }

    } break;

    case PredicateCondition::Between:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return slice_with_predicate(PredicateCondition::GreaterThanEquals, variant_value)
          ->slice_with_predicate(PredicateCondition::LessThanEquals, *variant_value2);

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return clone();

    case PredicateCondition::In:
    case PredicateCondition::NotIn:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("PredicateCondition not supported by Histograms");
  }

  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::scale_with_selectivity(
    const Selectivity selectivity) const {
  auto bin_minima = std::vector<T>{};
  auto bin_maxima = std::vector<T>{};
  auto bin_heights = std::vector<HistogramCountType>();
  auto bin_distinct_counts = std::vector<HistogramCountType>();

  bin_minima.reserve(bin_count());
  bin_maxima.reserve(bin_count());
  bin_heights.reserve(bin_count());
  bin_distinct_counts.reserve(bin_count());

  // Scale the number of values in the bin with the given selectivity.
  // Round up the numbers such that we tend to over- rather than underestimate.
  // Also, we avoid 0 as a height.
  for (auto bin_id = BinID{0}; bin_id < bin_count(); bin_id++) {
    const auto height = std::ceil(bin_height(bin_id) * selectivity);
    const auto distinct_count =
        std::ceil(scale_distinct_count(selectivity, bin_height(bin_id), bin_distinct_count(bin_id)));

    if (height == 0 || distinct_count == 0) continue;

    bin_minima.emplace_back(bin_minimum(bin_id));
    bin_maxima.emplace_back(bin_maximum(bin_id));
    bin_heights.emplace_back(static_cast<HistogramCountType>(height));
    bin_distinct_counts.emplace_back(static_cast<HistogramCountType>(distinct_count));
  }

  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> AbstractHistogram<T>::split_at_bin_edges(
    const std::vector<std::pair<T, T>>& additional_bin_edges) const {
  /**
   * Create vector with pairs for each split.
   * For a lower bin edge e, the new histogram will have to be split at previous_value(e) and e.
   * For an upper bin edge e, the new histogram will have to be split at e and next_value(e).
   *
   * We can safely ignore duplicate splits, so we use a set first and then copy the splits to a vector.
   * Also, we will create splits in gaps for now, but we will not create any actual bins for them later on.
   */
  const auto current_bin_count = bin_count();

  std::unordered_set<std::pair<T, T>, boost::hash<std::pair<T, T>>> split_set;

  for (auto bin_id = BinID{0}; bin_id < current_bin_count; bin_id++) {
    const auto bin_min = bin_minimum(bin_id);
    const auto bin_max = bin_maximum(bin_id);
    if constexpr (std::is_arithmetic_v<T>) {
      split_set.insert(std::make_pair(previous_value(bin_min), bin_min));
      split_set.insert(std::make_pair(bin_max, next_value(bin_max)));
    } else {
      // TODO(tim): turn into compile-time error
      Fail("Not supported for strings.");
    }
  }

  for (const auto& edge_pair : additional_bin_edges) {
    if constexpr (std::is_arithmetic_v<T>) {
      split_set.insert(std::make_pair(previous_value(edge_pair.first), edge_pair.first));
      split_set.insert(std::make_pair(edge_pair.second, next_value(edge_pair.second)));
    } else {
      // TODO(tim): turn into compile-time error
      Fail("Not supported for strings.");
    }
  }

  /**
   * Split pairs into single values.
   * These are the new bin edges, and we have to sort them.
   */
  std::vector<T> all_edges(split_set.size() * 2);
  size_t edge_idx = 0;
  for (const auto& split_pair : split_set) {
    all_edges[edge_idx] = split_pair.first;
    all_edges[edge_idx + 1] = split_pair.second;
    edge_idx += 2;
  }

  std::sort(all_edges.begin(), all_edges.end());

  /**
   * Remove the first and last value.
   * These are the values introduced in the first step for the first and last split.
   * The lower edge of bin 0 basically added the upper edge of bin -1,
   * and the upper edge of the last bin basically added the lower edge of the bin after the last bin.
   * Both values are obviously not needed.
   */
  all_edges.erase(all_edges.begin());
  all_edges.pop_back();

  std::vector<T> bin_minima;
  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_heights;
  std::vector<HistogramCountType> bin_distinct_counts;

  // We do not resize the vectors because we might not need all the slots because bins can be empty.
  const auto new_bin_count = all_edges.size() / 2;
  bin_minima.reserve(new_bin_count);
  bin_maxima.reserve(new_bin_count);
  bin_heights.reserve(new_bin_count);
  bin_distinct_counts.reserve(new_bin_count);

  /**
   * Create new bins.
   * Bin edges are defined by consecutive values in pairs of two.
   */
  for (auto bin_id = BinID{0}; bin_id < new_bin_count; bin_id++) {
    const auto bin_min = all_edges[bin_id * 2];
    const auto bin_max = all_edges[bin_id * 2 + 1];

    const auto estimate = estimate_cardinality(PredicateCondition::Between, bin_min, bin_max);
    if (estimate.type == EstimateType::MatchesNone) {
      continue;
    }

    auto height = estimate.cardinality;
    auto distinct_count = estimate_distinct_count(PredicateCondition::Between, bin_min, bin_max);

    // HACK to account for the fact that estimate_distinct_count() might return a slightly higher value
    //      than estimate_cardinality() due to float arithmetics.
    if (distinct_count > height && distinct_count - height < 0.001f) {
      distinct_count = height;
    }

    height = std::ceil(height);
    distinct_count = std::ceil(distinct_count);

    // Not creating empty bins
    if (height == 0) continue;

    bin_minima.emplace_back(bin_min);
    bin_maxima.emplace_back(bin_max);
    bin_heights.emplace_back(static_cast<HistogramCountType>(height));
    bin_distinct_counts.emplace_back(static_cast<HistogramCountType>(distinct_count));
  }

  if (bin_maxima.empty()) {
    return std::make_shared<SingleBinHistogram<T>>(T{}, T{}, 0, 0);
  } else {
    return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                                 std::move(bin_distinct_counts));
  }
}

template <typename T>
std::vector<std::pair<T, T>> AbstractHistogram<T>::bin_edges() const {
  std::vector<std::pair<T, T>> bin_edges(bin_count());

  for (auto bin_id = BinID{0}; bin_id < bin_edges.size(); bin_id++) {
    bin_edges[bin_id] = std::make_pair(bin_minimum(bin_id), bin_maximum(bin_id));
  }

  return bin_edges;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::_reduce_to_single_bin_histogram_impl() const {
  if constexpr (std::is_same_v<T, std::string>) {
    return std::make_shared<SingleBinHistogram<T>>(minimum(), maximum(), total_count(), total_distinct_count(),
                                                   _supported_characters, _string_prefix_length);
  } else {
    return std::make_shared<SingleBinHistogram<T>>(minimum(), maximum(), total_count(), total_distinct_count());
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
