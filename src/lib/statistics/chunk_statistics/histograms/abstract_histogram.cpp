#include "abstract_histogram.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/algorithm/string/replace.hpp"

#include "expression/evaluation/like_matcher.hpp"
#include "histogram_utils.hpp"
#include "storage/create_iterable_from_segment.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

template <typename T>
AbstractHistogram<T>::AbstractHistogram() : _supported_characters(""), _string_prefix_length(0ul) {}

template <>
AbstractHistogram<std::string>::AbstractHistogram() {
  const auto pair = get_default_or_check_prefix_settings();
  _supported_characters = pair.first;
  _string_prefix_length = pair.second;
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::string& supported_characters,
                                                  const uint64_t string_prefix_length)
    : _supported_characters(supported_characters), _string_prefix_length(string_prefix_length) {
  DebugAssert(check_prefix_settings(_supported_characters, _string_prefix_length), "Invalid prefix settings.");
}

template <typename T>
std::string AbstractHistogram<T>::description() const {
  std::stringstream stream;
  stream << histogram_type_to_string.at(histogram_type()) << std::endl;
  stream << "  distinct    " << total_count_distinct() << std::endl;
  stream << "  min         " << min() << std::endl;
  stream << "  max         " << max() << std::endl;
  // TODO(tim): consider non-null ratio in histograms
  // stream << "  non-null " << non_null_value_ratio() << std::endl;
  stream << "  bins        " << num_bins() << std::endl;

  stream << "  edges / counts " << std::endl;
  for (BinID bin = 0u; bin < num_bins(); bin++) {
    stream << "              [" << _bin_min(bin) << ", " << _bin_max(bin) << "]: ";
    stream << _bin_count(bin) << std::endl;
  }

  return stream.str();
}

template <typename T>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_sort_value_counts(
    const std::unordered_map<T, uint64_t>& value_counts) {
  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());

  std::sort(result.begin(), result.end(),
            [](const std::pair<T, uint64_t>& lhs, const std::pair<T, uint64_t>& rhs) { return lhs.first < rhs.first; });

  return result;
}

template <typename T>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_calculate_value_counts(
    const std::shared_ptr<const BaseSegment>& segment) {
  std::unordered_map<T, uint64_t> value_counts;

  resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
    auto iterable = create_iterable_from_segment<T>(typed_segment);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        value_counts[value.value()]++;
      }
    });
  });

  return AbstractHistogram<T>::_sort_value_counts(value_counts);
}

template <typename T>
T AbstractHistogram<T>::min() const {
  return _bin_min(0u);
}

template <typename T>
T AbstractHistogram<T>::max() const {
  return _bin_max(num_bins() - 1u);
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
T AbstractHistogram<T>::_bin_width(const BinID index) const {
  DebugAssert(index < num_bins(), "Index is not a valid bin.");
  return get_next_value(_bin_max(index) - _bin_min(index));
}

template <>
std::string AbstractHistogram<std::string>::_bin_width(const BinID /*index*/) const {
  Fail("Not supported for string histograms. Use _string_bin_width instead.");
}

template <>
uint64_t AbstractHistogram<std::string>::_string_bin_width(const BinID index) const {
  DebugAssert(index < num_bins(), "Index is not a valid bin.");

  const auto num_min = this->_convert_string_to_number_representation(_bin_min(index));
  const auto num_max = this->_convert_string_to_number_representation(_bin_max(index));
  return num_max - num_min + 1u;
}

template <typename T>
T AbstractHistogram<T>::get_next_value(const T value) const {
  return next_value(value);
}

template <>
std::string AbstractHistogram<std::string>::get_next_value(const std::string value) const {
  return next_value(value, _supported_characters);
}

template <typename T>
float AbstractHistogram<T>::_bin_share(const BinID bin_id, const T value) const {
  return static_cast<float>(value - _bin_min(bin_id)) / _bin_width(bin_id);
}

template <>
float AbstractHistogram<std::string>::_bin_share(const BinID bin_id, const std::string value) const {
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
  const auto bin_min = _bin_min(bin_id);
  const auto bin_max = _bin_max(bin_id);
  const auto common_prefix_len = common_prefix_length(bin_min, bin_max);

  DebugAssert(value.substr(0, common_prefix_len) == bin_min.substr(0, common_prefix_len),
              "Value does not belong to bin");

  const auto value_repr = _convert_string_to_number_representation(value.substr(common_prefix_len));
  const auto min_repr = _convert_string_to_number_representation(bin_min.substr(common_prefix_len));
  const auto max_repr = _convert_string_to_number_representation(bin_max.substr(common_prefix_len));
  return static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);
}

template <typename T>
bool AbstractHistogram<T>::_can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                      const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast<T>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto bin_id = _bin_for_value(value);
      // It is possible for EqualWidthHistograms to have empty bins.
      return bin_id == INVALID_BIN_ID || _bin_count(bin_id) == 0ul;
    }
    case PredicateCondition::NotEquals:
      return min() == value && max() == value;
    case PredicateCondition::LessThan:
      return value <= min();
    case PredicateCondition::LessThanEquals:
      return value < min();
    case PredicateCondition::GreaterThanEquals:
      return value > max();
    case PredicateCondition::GreaterThan:
      return value >= max();
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");

      if (can_prune(PredicateCondition::GreaterThanEquals, value)) {
        return true;
      }

      const auto value2 = type_cast<T>(*variant_value2);
      if (can_prune(PredicateCondition::LessThanEquals, value2) || value2 < value) {
        return true;
      }

      const auto value_bin = _bin_for_value(value);
      const auto value2_bin = _bin_for_value(value2);

      // In an EqualNumElementsHistogram, if both values fall into the same gap, we can prune the predicate.
      // We need to have at least two bins to rule out pruning if value < min and value2 > max.
      if (value_bin == INVALID_BIN_ID && value2_bin == INVALID_BIN_ID && num_bins() > 1ul &&
          _upper_bound_for_value(value) == _upper_bound_for_value(value2)) {
        return true;
      }

      // In an EqualWidthHistogram, if both values fall into a bin that has no elements,
      // and there are either no bins in between or none of them have any elements, we can also prune the predicate.
      if (value_bin != INVALID_BIN_ID && value2_bin != INVALID_BIN_ID && _bin_count(value_bin) == 0 &&
          _bin_count(value2_bin) == 0) {
        for (auto current_bin = value_bin + 1; current_bin < value2_bin; current_bin++) {
          if (_bin_count(current_bin) > 0ul) {
            return false;
          }
        }
        return true;
      }

      return false;
    }
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      Fail("Predicate NOT LIKE is not supported for non-string columns.");
    default:
      // Do not prune predicates we cannot (yet) handle.
      return false;
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                     const std::optional<AllTypeVariant>& variant_value2) const {
  return _can_prune(predicate_type, variant_value, variant_value2);
}

template <>
bool AbstractHistogram<std::string>::can_prune(const PredicateCondition predicate_type,
                                               const AllTypeVariant& variant_value,
                                               const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast<std::string>(variant_value);

  // Only allow supported characters in search value.
  // If predicate is (NOT) LIKE additionally allow wildcards.
  const auto allowed_characters =
      _supported_characters +
      (predicate_type == PredicateCondition::Like || predicate_type == PredicateCondition::NotLike ? "_%" : "");
  Assert(value.find_first_not_of(allowed_characters) == std::string::npos, "Unsupported characters.");

  switch (predicate_type) {
    case PredicateCondition::Like: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return can_prune(PredicateCondition::Equals, value);
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
        const auto upper_bound = next_value(search_prefix, _supported_characters, search_prefix.length());
        return can_prune(PredicateCondition::GreaterThanEquals, search_prefix) ||
               can_prune(PredicateCondition::LessThan, upper_bound) ||
               (_bin_for_value(search_prefix) == INVALID_BIN_ID && _bin_for_value(upper_bound) == INVALID_BIN_ID &&
                _upper_bound_for_value(search_prefix) == _upper_bound_for_value(upper_bound));
      }

      return false;
    }
    case PredicateCondition::NotLike: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return can_prune(PredicateCondition::NotEquals, variant_value);
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
        if (search_prefix == min().substr(0, search_prefix.length()) &&
            search_prefix == max().substr(0, search_prefix.length())) {
          return true;
        }
      }

      return false;
    }
    default:
      return _can_prune(predicate_type, variant_value, variant_value2);
  }
}

template <typename T>
float AbstractHistogram<T>::_estimate_cardinality(const PredicateCondition predicate_type,
                                                  const AllTypeVariant& variant_value,
                                                  const std::optional<AllTypeVariant>& variant_value2) const {
  if (can_prune(predicate_type, variant_value, variant_value2)) {
    return 0.f;
  }

  const auto value = type_cast<T>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto index = _bin_for_value(value);
      const auto bin_count_distinct = _bin_count_distinct(index);

      // This should never be false because can_prune should have been true further up if this was the case.
      DebugAssert(bin_count_distinct > 0u, "0 distinct values in bin.");

      return static_cast<float>(_bin_count(index)) / static_cast<float>(bin_count_distinct);
    }
    case PredicateCondition::NotEquals:
      return total_count() - _estimate_cardinality(PredicateCondition::Equals, variant_value);
    case PredicateCondition::LessThan: {
      if (value > max()) {
        return total_count();
      }

      // This should never be false because can_prune should have been true further up if this was the case.
      DebugAssert(value >= min(), "Value smaller than min of histogram.");

      auto index = _bin_for_value(value);
      auto cardinality = 0.f;

      if (index == INVALID_BIN_ID) {
        // The value is within the range of the histogram, but does not belong to a bin.
        // Therefore, we need to sum up the counts of all bins with a max < value.
        index = _upper_bound_for_value(value);
      } else {
        cardinality += _bin_share(index, value) * _bin_count(index);
      }

      // Sum up all bins before the bin (or gap) containing the value.
      for (BinID bin = 0u; bin < index; bin++) {
        cardinality += _bin_count(bin);
      }

      /**
       * The cardinality is capped at total_count().
       * It is possible for a value that is smaller than or equal to the max of the EqualHeightHistogram
       * to yield a calculated cardinality higher than total_count.
       * This is due to the way EqualHeightHistograms store the count for a bin,
       * which is in a single value (count_per_bin) for all bins rather than a vector (one value for each bin).
       * Consequently, this value is the desired count for all bins.
       * In practice, _bin_count(n) >= _count_per_bin for n < num_bins() - 1,
       * because bins are filled up until the count is at least _count_per_bin.
       * The last bin typically has a count lower than _count_per_bin.
       * Therefore, if we calculate the share of the last bin based on _count_per_bin
       * we might end up with an estimate higher than total_count(), which is then capped.
       */
      return std::min(cardinality, static_cast<float>(total_count()));
    }
    case PredicateCondition::LessThanEquals:
      return estimate_cardinality(PredicateCondition::LessThan, get_next_value(value));
    case PredicateCondition::GreaterThanEquals:
      return total_count() - estimate_cardinality(PredicateCondition::LessThan, variant_value);
    case PredicateCondition::GreaterThan:
      return total_count() - estimate_cardinality(PredicateCondition::LessThanEquals, variant_value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast<T>(*variant_value2);

      if (value2 < value) {
        return 0.f;
      }

      return estimate_cardinality(PredicateCondition::LessThanEquals, *variant_value2) -
             estimate_cardinality(PredicateCondition::LessThan, variant_value);
    }
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      Fail("Predicate NOT LIKE is not supported for non-string columns.");
    default:
      // TODO(anyone): implement more meaningful things here
      return total_count();
  }
}

// Specialization for numbers.
template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const PredicateCondition predicate_type,
                                                 const AllTypeVariant& variant_value,
                                                 const std::optional<AllTypeVariant>& variant_value2) const {
  return _estimate_cardinality(predicate_type, variant_value, variant_value2);
}

// Specialization for strings.
template <>
float AbstractHistogram<std::string>::estimate_cardinality(const PredicateCondition predicate_type,
                                                           const AllTypeVariant& variant_value,
                                                           const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast<std::string>(variant_value);

  // Only allow supported characters in search value.
  // If predicate is (NOT) LIKE additionally allow wildcards.
  const auto allowed_characters =
      _supported_characters +
      (predicate_type == PredicateCondition::Like || predicate_type == PredicateCondition::NotLike ? "_%" : "");
  Assert(value.find_first_not_of(allowed_characters) == std::string::npos, "Unsupported characters.");

  if (can_prune(predicate_type, variant_value, variant_value2)) {
    return 0.f;
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
        return total_count();
      }

      const auto any_chars_count = std::count(value.cbegin(), value.cend(), '%');
      DebugAssert(any_chars_count > 0u,
                  "contains_wildcard() should not return true if there is neither a '%' nor a '_' in the string.");

      // Match everything.
      if (value == "%") {
        return total_count();
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
        const auto additional_characters = value.length() - search_prefix.length() - any_chars_count;
        return (estimate_cardinality(PredicateCondition::LessThan,
                                     next_value(search_prefix, _supported_characters, search_prefix.length())) -
                estimate_cardinality(PredicateCondition::LessThan, search_prefix)) /
               ipow(_supported_characters.length(), additional_characters);
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
      return static_cast<float>(total_count()) / ipow(_supported_characters.length(), fixed_characters);
    }
    case PredicateCondition::NotLike: {
      if (!LikeMatcher::contains_wildcard(value)) {
        return estimate_cardinality(PredicateCondition::NotEquals, variant_value);
      }

      // We don't deal with this for now because it is not worth the effort.
      // TODO(anyone): think about good way to handle SingleChar wildcard in patterns.
      const auto single_char_count = std::count(value.cbegin(), value.cend(), '_');
      if (single_char_count > 0u) {
        return total_count();
      }

      return total_count() - estimate_cardinality(PredicateCondition::Like, variant_value);
    }
    default:
      return _estimate_cardinality(predicate_type, variant_value, variant_value2);
  }
}

template <typename T>
float AbstractHistogram<T>::estimate_selectivity(const PredicateCondition predicate_type,
                                                 const AllTypeVariant& variant_value,
                                                 const std::optional<AllTypeVariant>& variant_value2) const {
  return estimate_cardinality(predicate_type, variant_value, variant_value2) / total_count();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
