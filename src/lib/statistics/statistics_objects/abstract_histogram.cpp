#include "abstract_histogram.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/functional/hash.hpp"
#include "boost/lexical_cast.hpp"

#include "expression/evaluation/like_matcher.hpp"
#include "generic_histogram.hpp"
#include "generic_histogram_builder.hpp"
#include "lossy_cast.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

template <typename T>
AbstractHistogram<T>::AbstractHistogram(const HistogramDomain<T>& domain)
    : AbstractStatisticsObject(data_type_from_type<T>()), _domain(domain) {}

template <typename T>
const HistogramDomain<T>& AbstractHistogram<T>::domain() const {
  return _domain;
}

template <typename T>
std::string AbstractHistogram<T>::description() const {
  std::stringstream stream;

  stream << name();
  stream << " value count: " << total_count() << ";";
  stream << " distinct count: " << total_distinct_count() << ";";
  stream << " bin count: " << bin_count() << ";";

  stream << "  Bins" << std::endl;
  for (BinID bin_id = 0u; bin_id < bin_count(); ++bin_id) {
    if constexpr (std::is_same_v<T, pmr_string>) {
      stream << "  ['" << bin_minimum(bin_id) << "' (" << _domain.string_to_number(bin_minimum(bin_id)) << ") -> '";
      stream << bin_maximum(bin_id) << "' (" << _domain.string_to_number(bin_maximum(bin_id)) << ")]: ";
    } else {
      stream << "  [" << bin_minimum(bin_id) << " -> " << bin_maximum(bin_id) << "]: ";
    }
    stream << "Height: " << bin_height(bin_id) << "; DistinctCount: " << bin_distinct_count(bin_id) << std::endl;
  }

  return stream.str();
}

template <typename T>
HistogramBin<T> AbstractHistogram<T>::bin(const BinID index) const {
  return {bin_minimum(index), bin_maximum(index), bin_height(index), bin_distinct_count(index)};
}

template <typename T>
bool AbstractHistogram<T>::bin_contains(const BinID index, const T& value) const {
  return bin_minimum(index) <= value && value <= bin_maximum(index);
}

template <typename T>
typename AbstractHistogram<T>::HistogramWidthType AbstractHistogram<T>::bin_width(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // The width of an float bin [5.0, 7.0] is 2, as one would expect, but the width of an integer bin [5, 7] is 3.
  // An integer (e.g. 3) is thought to have a width of 1, whereas the floating point number 3.0 is thought to have no
  // width.
  // This makes our math more correct in multiple places, e.g., for a bin [0, 3] with a width of 4, the ratio of
  // values smaller than 2 is (2 - 0) / (width=4) -> 0.5: 0 and 1 are smaller, 2 and 3 are larger.
  // For a float bin [0.0, 3.0] it is obvious that only two-thirds of the bin are smaller than 2:
  // 2 - 0 / (width=3) -> 0.6666

  if constexpr (std::is_same_v<T, pmr_string>) {
    const auto repr_min = _domain.string_to_number(bin_minimum(index));
    const auto repr_max = _domain.string_to_number(bin_maximum(index));
    return repr_max - repr_min + 1u;
  } else if constexpr (std::is_floating_point_v<T>) {
    return bin_maximum(index) - bin_minimum(index);
  } else {
    // The width of a integral-histogram bin [4,5] is 5 - 4 + 1 = 2
    return bin_maximum(index) - bin_minimum(index) + 1;
  }
}

template <typename T>
float AbstractHistogram<T>::bin_ratio_less_than(const BinID bin_id, const T& value) const {
  if (value <= bin_minimum(bin_id)) {
    return 0.0f;
  }

  if (value > bin_maximum(bin_id)) {
    return 1.0f;
  }

  if constexpr (!std::is_same_v<T, pmr_string>) {
    return (static_cast<float>(value) - static_cast<float>(bin_minimum(bin_id))) /
           static_cast<float>(bin_width(bin_id));
  } else {
    /*
    * We need to convert strings to their numerical representation to calculate a share.
    * This conversion is done based on prefixes because strings of arbitrary length cannot be converted to a numerical
    * representation that satisfies the following requirements:
    *  1. For two strings s1 and s2: s1 < s2 -> repr(s1) < repr(s2)
    *  2. For two strings s1 and s2: dist(s1, s2) == repr(s2) - repr(s1)
    *  repr(s) is the numerical representation for a string s, and dist(s1, s2) returns the number of strings between
    *  s1 and s2 in the domain of strings with at most length `_domain.prefix_length` and the set of supported
    * characters in the `_domain`.
    *
    * Thus, we calculate the range based only on a domain of strings with a maximum length of `_domain.prefix_length`
    * characters. To improve estimations where strings exceed that length, but share a common prefix, we strip that
    * common prefix and take the substring starting after that prefix.
    *
    * Example:
    *  - bin: ["intelligence", "intellij"]
    *  - _domain.supported_characters: [a-z]
    *  - _domain.prefix_length: 4
    *  - value: intelligent
    *
    *  Traditionally, if we did not strip the common prefix, we would calculate the range based on the
    *  substring of length `_domain.prefix_length`, which is "inte" for both lower and upper edge of the bin.
    *  We could not make a reasonable assumption how large the share is.
    *  Instead, we strip the common prefix ("intelli") and calculate the share based on the numerical representation
    *  of the substring after the common prefix.
    *  That is, what is the share of values smaller than "gent" in the range ["gence", "j"]?
    */

    const auto bin_min = bin_minimum(bin_id);
    const auto bin_max = bin_maximum(bin_id);

    // Determine the common_prefix_lengths of bin_min and bin_max. E.g. bin_max=abcde and bin_max=abcz have a
    // common_prefix_length=3
    auto common_prefix_length = size_t{0};
    const auto max_common_prefix_length =
        std::min(bin_min.length() - _domain.prefix_length, bin_max.length() - _domain.prefix_length);
    for (; common_prefix_length < max_common_prefix_length; ++common_prefix_length) {
      if (bin_min[common_prefix_length] != bin_max[common_prefix_length]) {
        break;
      }
    }

    const auto in_domain_value = _domain.string_to_domain(value.substr(common_prefix_length));
    const auto value_repr = _domain.string_to_number(in_domain_value);
    const auto min_repr = _domain.string_to_number(bin_min.substr(common_prefix_length));
    const auto max_repr = _domain.string_to_number(bin_max.substr(common_prefix_length));
    const auto bin_ratio = static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);

    return bin_ratio;
  }
}

template <typename T>
float AbstractHistogram<T>::bin_ratio_less_than_equals(const BinID bin_id, const T& value) const {
  if (value < bin_minimum(bin_id)) {
    return 0.0f;
  }

  if (value >= bin_maximum(bin_id)) {
    return 1.0f;
  }

  if constexpr (!std::is_same_v<T, pmr_string>) {
    return bin_ratio_less_than(bin_id, _domain.next_value_clamped(value));
  } else {
    // See bin_ratio_less_than for comment.
    const auto bin_min = bin_minimum(bin_id);
    const auto bin_max = bin_maximum(bin_id);

    auto common_prefix_length = size_t{0};
    const auto max_common_prefix_length =
        std::min(bin_min.length() - _domain.prefix_length, bin_max.length() - _domain.prefix_length);
    for (; common_prefix_length < max_common_prefix_length; ++common_prefix_length) {
      if (bin_min[common_prefix_length] != bin_max[common_prefix_length]) {
        break;
      }
    }

    const auto in_domain_value = _domain.string_to_domain(value.substr(common_prefix_length));
    const auto value_repr = _domain.string_to_number(in_domain_value) + 1;
    const auto min_repr = _domain.string_to_number(bin_min.substr(common_prefix_length));
    const auto max_repr = _domain.string_to_number(bin_max.substr(common_prefix_length));
    const auto bin_ratio = static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);

    return bin_ratio;
  }
}

template <typename T>
float AbstractHistogram<T>::bin_ratio_between(const BinID bin_id, const T& value, const T& value2) const {
  // All values that are less than or equal to the upper boundary minus all values that are smaller than the lower
  // boundary
  return bin_ratio_less_than_equals(bin_id, value2) - bin_ratio_less_than(bin_id, value);
}

template <typename T>
bool AbstractHistogram<T>::does_not_contain(const PredicateCondition predicate_condition,
                                            const AllTypeVariant& variant_value,
                                            const std::optional<AllTypeVariant>& variant_value2) const {
  if (bin_count() == 0) {
    return true;
  }

  const auto value = lossy_variant_cast<T>(variant_value);
  if (!value) {
    return false;
  }

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      const auto bin_id = _bin_for_value(*value);
      // It is possible for EqualWidthHistograms to have empty bins.
      return bin_id == INVALID_BIN_ID || bin_height(bin_id) == 0ul;
    }
    case PredicateCondition::NotEquals:
      return bin_minimum(BinID{0}) == value && bin_maximum(bin_count() - 1) == value;
    case PredicateCondition::LessThan:
      return value <= bin_minimum(BinID{0});
    case PredicateCondition::LessThanEquals:
      return value < bin_minimum(BinID{0});
    case PredicateCondition::GreaterThanEquals:
      return value > bin_maximum(bin_count() - 1);
    case PredicateCondition::GreaterThan:
      return value >= bin_maximum(bin_count() - 1);

    // For simplicity we do not distinguish between the different Between* types.
    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");

      if (does_not_contain(PredicateCondition::GreaterThanEquals, *value)) {
        return true;
      }

      const auto value2 = lossy_variant_cast<T>(*variant_value2);
      if (!value2) {
        return false;
      }
      if (does_not_contain(PredicateCondition::LessThanEquals, *value2) || value2 < value) {
        return true;
      }

      const auto value_bin = _bin_for_value(*value);
      const auto value2_bin = _bin_for_value(*value2);

      // In an EqualDistinctCountHistogram, if both values fall into the same gap, we can prune the predicate.
      // We need to have at least two bins to rule out pruning if value < min and value2 > max.
      if (value_bin == INVALID_BIN_ID && value2_bin == INVALID_BIN_ID && bin_count() > 1ul &&
          _next_bin_for_value(*value) == _next_bin_for_value(*value2)) {
        return true;
      }

      // In an EqualWidthHistogram, if both values fall into a bin that has no elements,
      // and there are either no bins in between or none of them have any elements, we can also prune the predicate.
      if (value_bin != INVALID_BIN_ID && value2_bin != INVALID_BIN_ID && bin_height(value_bin) == 0 &&
          bin_height(value2_bin) == 0) {
        for (auto current_bin = value_bin + 1; current_bin < value2_bin; ++current_bin) {
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
      if constexpr (std::is_same_v<pmr_string, T>) {
        // Generally, (NOT) LIKE is to hard to perform estimations on. Some special patterns could be estimated,
        // e.g. `LIKE 'a'` is the same as `= 'a'` or `LIKE 'a%'` is the same as `>= a AND < b`, but we leave it to other
        // components (e.g., the optimizer) to perform these transformations.
        // Thus, (NOT) LIKE is not estimated and we cannot determine whether does_not_contain() could be true.
        return false;
      } else {
        Fail("Predicate NOT LIKE is not supported for non-string columns.");
      }

    default:
      // Do not prune predicates we cannot (yet) handle.
      return false;
  }
}

template <typename T>
std::pair<Cardinality, DistinctCount> AbstractHistogram<T>::estimate_cardinality_and_distinct_count(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  auto value = lossy_variant_cast<T>(variant_value);
  if (!value) {
    return {static_cast<Cardinality>(total_count()), static_cast<float>(total_distinct_count())};
  }

  // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
  if constexpr (std::is_same_v<T, pmr_string>) {
    value = _domain.string_to_domain(*value);
  }

  if (does_not_contain(predicate_condition, variant_value, variant_value2)) {
    return {Cardinality{0}, 0.0f};
  }

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      const auto bin_id = _bin_for_value(*value);
      const auto bin_distinct_count = this->bin_distinct_count(bin_id);

      if (bin_distinct_count == 0) {
        return {Cardinality{0.0f}, 0.0f};
      } else {
        const auto cardinality = Cardinality{bin_height(bin_id) / bin_distinct_count};
        return {cardinality, std::min(bin_distinct_count, HistogramCountType{1.0f})};
      }
    }

    case PredicateCondition::NotEquals:
      return _invert_estimate(estimate_cardinality_and_distinct_count(PredicateCondition::Equals, variant_value));

    case PredicateCondition::LessThan: {
      if (*value > bin_maximum(bin_count() - 1)) {
        return {static_cast<Cardinality>(total_count()), static_cast<float>(total_distinct_count())};
      }

      // This should never be false because does_not_contain should have been true further up if this was the case.
      DebugAssert(*value >= bin_minimum(BinID{0}), "Value smaller than min of histogram.");

      auto cardinality = Cardinality{0};
      auto distinct_count = 0.f;
      auto bin_id = _bin_for_value(*value);

      if (bin_id == INVALID_BIN_ID) {
        // The value is within the range of the histogram, but does not belong to a bin.
        // Therefore, we need to sum up the counts of all bins with a max < value.
        bin_id = _next_bin_for_value(*value);
      } else if (value == bin_minimum(bin_id) || bin_height(bin_id) == 0u) {
        // If the value is exactly the lower bin edge or the bin is empty,
        // we do not have to add anything of that bin and know the cardinality exactly.
      } else {
        const auto share = bin_ratio_less_than(bin_id, *value);
        cardinality += share * bin_height(bin_id);
        distinct_count += share * bin_distinct_count(bin_id);
      }

      DebugAssert(bin_id != INVALID_BIN_ID, "Should have been caught by does_not_contain().");

      // Sum up all bins before the bin (or gap) containing the value.
      for (BinID bin = 0u; bin < bin_id; ++bin) {
        cardinality += bin_height(bin);
        distinct_count += bin_distinct_count(bin);
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
      return {std::min(cardinality, static_cast<Cardinality>(total_count())), distinct_count};
    }
    case PredicateCondition::LessThanEquals:
      return estimate_cardinality_and_distinct_count(PredicateCondition::LessThan, _domain.next_value_clamped(*value));

    case PredicateCondition::GreaterThanEquals:
      return _invert_estimate(estimate_cardinality_and_distinct_count(PredicateCondition::LessThan, variant_value));

    case PredicateCondition::GreaterThan:
      return _invert_estimate(
          estimate_cardinality_and_distinct_count(PredicateCondition::LessThanEquals, variant_value));

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = lossy_variant_cast<T>(*variant_value2);
      if (!value2) {
        return {total_count(), total_distinct_count()};
      }

      if (*value2 < *value) {
        return {Cardinality{0}, 0.0f};
      }

      // Adjust value (lower_bound) and value2 (lower_bin_id) so that both values are contained within a bin
      auto lower_bound = *value;
      auto lower_bin_id = _bin_for_value(*value);
      if (lower_bin_id == INVALID_BIN_ID) {
        lower_bin_id = _next_bin_for_value(*value);
        lower_bound = bin_minimum(lower_bin_id);
      }

      auto upper_bound = *value2;
      auto upper_bin_id = _bin_for_value(*value2);
      if (upper_bin_id == INVALID_BIN_ID) {
        upper_bin_id = _next_bin_for_value(*value2);
        if (upper_bin_id == INVALID_BIN_ID) {
          upper_bin_id = bin_count() - 1;
        } else {
          upper_bin_id = upper_bin_id - 1;
        }
        upper_bound = bin_maximum(upper_bin_id);
      }

      // Accumulate the cardinality/distinct count of all bins from the lower bound to the upper bound
      auto cardinality = HistogramCountType{0};
      auto distinct_count = HistogramCountType{0};
      for (auto bin_id = lower_bin_id; bin_id <= upper_bin_id; ++bin_id) {
        cardinality += bin_height(bin_id);
        distinct_count += bin_distinct_count(bin_id);
      }

      // Subtract the cardinality/distinct below the lower bound and above the upper bound
      const auto bin_ratio_less_than_lower_bound = bin_ratio_less_than(lower_bin_id, lower_bound);
      cardinality -= bin_height(lower_bin_id) * bin_ratio_less_than_lower_bound;
      distinct_count -= bin_distinct_count(lower_bin_id) * bin_ratio_less_than_lower_bound;

      const auto bin_ratio_greater_than_upper_bound = 1.0f - bin_ratio_less_than_equals(upper_bin_id, upper_bound);
      cardinality -= bin_height(upper_bin_id) * bin_ratio_greater_than_upper_bound;
      distinct_count -= bin_distinct_count(upper_bin_id) * bin_ratio_greater_than_upper_bound;

      return {cardinality, distinct_count};
    }

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      if constexpr (std::is_same_v<pmr_string, T>) {
        // Generally, (NOT) LIKE is too hard to perform estimations on. Some special patterns could be estimated,
        // e.g. `LIKE 'a'` is the same as `= 'a'` or `LIKE 'a%'` is the same as `>= a AND < b`, but we leave it to other
        // components (e.g., the optimizer) to perform these transformations.
        // Thus, (NOT) LIKE is not estimated and we return a selectivity of 1.
        return {total_count(), total_distinct_count()};
      } else {
        Fail("Predicate NOT LIKE is not supported for non-string columns.");
      }

    default:
      Fail("Predicate not supported");
  }
}

template <typename T>
std::pair<Cardinality, DistinctCount> AbstractHistogram<T>::_invert_estimate(
    const std::pair<Cardinality, DistinctCount>& estimate) const {
  DebugAssert(total_count() >= estimate.first, "Estimate cannot be higher than total_count()");
  DebugAssert(total_distinct_count() >= estimate.second, "Estimate cannot be higher than total_count()");

  return {Cardinality{total_count() - estimate.first}, total_distinct_count() - estimate.second};
}

// Specialization for numbers.
template <typename T>
Cardinality AbstractHistogram<T>::estimate_cardinality(const PredicateCondition predicate_condition,
                                                       const AllTypeVariant& variant_value,
                                                       const std::optional<AllTypeVariant>& variant_value2) const {
  const auto estimate = estimate_cardinality_and_distinct_count(predicate_condition, variant_value, variant_value2);
  return estimate.first;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_condition, variant_value, variant_value2)) {
    return nullptr;
  }

  const auto value = lossy_variant_cast<T>(variant_value);
  DebugAssert(value, "sliced() cannot be called with NULL");

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      GenericHistogramBuilder<T> builder{1, _domain};
      builder.add_bin(*value, *value,
                      static_cast<HistogramCountType>(estimate_cardinality(PredicateCondition::Equals, variant_value)),
                      1);
      return builder.build();
    }

    case PredicateCondition::NotEquals: {
      const auto value_bin_id = _bin_for_value(*value);
      if (value_bin_id == INVALID_BIN_ID) return clone();

      auto minimum = bin_minimum(value_bin_id);
      auto maximum = bin_maximum(value_bin_id);
      const auto distinct_count = bin_distinct_count(value_bin_id);

      // Do not create empty bin, if `value` is the only value in the bin
      const auto new_bin_count = minimum == maximum ? bin_count() - 1 : bin_count();

      GenericHistogramBuilder<T> builder{new_bin_count, _domain};

      builder.add_copied_bins(*this, BinID{0}, value_bin_id);

      // Do not create empty bin, if `value` is the only value in the bin
      if (minimum != maximum) {
        // A bin [50, 60] sliced with `!= 60` becomes [50, 59]
        // TODO(anybody) Implement bin bounds trimming for strings
        // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
        if constexpr (!std::is_same_v<pmr_string, T>) {
          if (minimum == *value) {
            minimum = _domain.next_value_clamped(*value);
          }

          if (maximum == *value) {
            maximum = _domain.previous_value_clamped(*value);
          }
        }

        const auto estimate = estimate_cardinality_and_distinct_count(PredicateCondition::Equals, variant_value);
        const auto new_height = bin_height(value_bin_id) - estimate.first;
        const auto new_distinct_count = distinct_count - estimate.second;

        builder.add_bin(minimum, maximum, new_height, new_distinct_count);
      }

      builder.add_copied_bins(*this, value_bin_id + 1, bin_count());

      return builder.build();
    }

    case PredicateCondition::LessThanEquals:
      return sliced(PredicateCondition::LessThan, _domain.next_value_clamped(*value));

    case PredicateCondition::LessThan: {
      auto last_included_bin_id = _bin_for_value(*value);

      if (last_included_bin_id == INVALID_BIN_ID) {
        const auto next_bin_id_after_value = _next_bin_for_value(*value);

        if (next_bin_id_after_value == INVALID_BIN_ID) {
          last_included_bin_id = bin_count() - 1;
        } else {
          last_included_bin_id = next_bin_id_after_value - 1;
        }
      } else if (*value == bin_minimum(last_included_bin_id)) {
        --last_included_bin_id;
      }

      auto last_bin_maximum = T{};
      // previous_value_clamped(value) is not available for strings, but we do not expect it to make a big difference.
      // TODO(anybody) Correctly implement bin bounds trimming for strings
      // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
      if constexpr (!std::is_same_v<T, pmr_string>) {
        last_bin_maximum = std::min(bin_maximum(last_included_bin_id), _domain.previous_value_clamped(*value));
      } else {
        last_bin_maximum = std::min(bin_maximum(last_included_bin_id), *value);
      }

      GenericHistogramBuilder<T> builder{last_included_bin_id + 1, _domain};
      builder.add_copied_bins(*this, BinID{0}, last_included_bin_id);
      builder.add_sliced_bin(*this, last_included_bin_id, bin_minimum(last_included_bin_id), last_bin_maximum);

      return builder.build();
    }

    case PredicateCondition::GreaterThan:
      return sliced(PredicateCondition::GreaterThanEquals, _domain.next_value_clamped(*value));

    case PredicateCondition::GreaterThanEquals: {
      auto first_new_bin_id = _bin_for_value(*value);

      if (first_new_bin_id == INVALID_BIN_ID) {
        first_new_bin_id = _next_bin_for_value(*value);
      }

      DebugAssert(first_new_bin_id < bin_count(), "This should have been caught by does_not_contain().");

      GenericHistogramBuilder<T> builder{bin_count() - first_new_bin_id, _domain};

      builder.add_sliced_bin(*this, first_new_bin_id, std::max(*value, bin_minimum(first_new_bin_id)),
                             bin_maximum(first_new_bin_id));
      builder.add_copied_bins(*this, first_new_bin_id + 1, bin_count());

      return builder.build();
    }

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive:
      Assert(variant_value2, "BETWEEN needs a second value.");
      return sliced(PredicateCondition::GreaterThanEquals, variant_value)
          ->sliced(PredicateCondition::LessThanEquals, *variant_value2);

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      // TODO(anybody) Slicing for (NOT) LIKE not supported, yet
      return clone();

    case PredicateCondition::In:
    case PredicateCondition::NotIn:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("PredicateCondition not supported by Histograms");
  }

  Fail("Invalid enum value");
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::pruned(
    const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = lossy_variant_cast<T>(variant_value);
  DebugAssert(value, "pruned() cannot be called with NULL");

  // For each bin, bin_prunable_height holds the number of values that do not fulfill the predicate. If we had some
  // information about the sort order of the table, we might start pruning a GreaterThan predicate with the lowest
  // value instead of uniformly pruning across all affected chunks as a future optimization.
  std::vector<HistogramCountType> bin_prunable_height(bin_count());

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      // Bins that do not contain the value are fully prunable. In the bin that contains the value, there are
      // `height / distinct_count` instances of the value that satisfies the predicate. All other values are prunable.
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        if (bin_contains(bin_id, *value)) {
          bin_prunable_height[bin_id] = bin_height(bin_id) - (bin_height(bin_id) / bin_distinct_count(bin_id));
        } else {
          bin_prunable_height[bin_id] = bin_height(bin_id);
        }
      }
    } break;

    case PredicateCondition::NotEquals: {
      // Bins that do not contain the value are NOT prunable. In the bin that contains the value, there are
      // `height / distinct_count` instances of the value that DO NOT satisfy the predicate. All other values are
      // prunable.
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        if (bin_contains(bin_id, *value)) {
          bin_prunable_height[bin_id] = bin_height(bin_id) / bin_distinct_count(bin_id);
        } else {
          bin_prunable_height[bin_id] = 0.0f;
        }
      }
    } break;

    case PredicateCondition::LessThanEquals: {
      // For bins that are completely less than/equal to the value, bin_ratio_less_than_equals is 100% and no values
      // are prunable. After that, the inverse bin_ratio_less_than_equals is prunable.
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        bin_prunable_height[bin_id] = bin_height(bin_id) * (1.0f - bin_ratio_less_than_equals(bin_id, *value));
      }
    } break;

    case PredicateCondition::LessThan: {
      // Analogous to LessThanEquals
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        bin_prunable_height[bin_id] = bin_height(bin_id) * (1.0f - bin_ratio_less_than(bin_id, *value));
      }
    } break;

    case PredicateCondition::GreaterThan: {
      // Analogous to LessThanEquals
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        bin_prunable_height[bin_id] = bin_height(bin_id) * bin_ratio_less_than_equals(bin_id, *value);
      }
    } break;

    case PredicateCondition::GreaterThanEquals: {
      // Analogous to LessThanEquals
      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        bin_prunable_height[bin_id] = bin_height(bin_id) * bin_ratio_less_than(bin_id, *value);
      }
    } break;

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive: {
      // Treating all between conditions the same for now. Analogous to LessThanEquals.
      DebugAssert(variant_value2, "Expected second value for between");
      const auto value2 = lossy_variant_cast<T>(*variant_value2);
      DebugAssert(value2, "pruned() cannot be called with NULL");

      for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
        bin_prunable_height[bin_id] = bin_height(bin_id) * (1.0f - bin_ratio_between(bin_id, *value, *value2));
      }
    } break;

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      // TODO(anybody) Pruning for (NOT) LIKE not supported, yet
      return clone();

    case PredicateCondition::In:
    case PredicateCondition::NotIn:
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      Fail("PredicateCondition not supported by Histograms");
  }

  auto total_prunable_values = HistogramCountType{0};
  for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
    total_prunable_values += bin_prunable_height[bin_id];
  }

  // Without prunable values, the histogram is out-of-date. Return an unmodified histogram to avoid DIV/0 below.
  if (total_prunable_values == HistogramCountType{0}) {
    return clone();
  }

  // For an up-to-date histogram, the pruning ratio should never exceed 100%. If, however, the histogram is outdated
  // we must make sure that no bin becomes more than empty and that we do not touch the unpruned part of the bin.
  const auto pruning_ratio = std::min(static_cast<Selectivity>(num_values_pruned) / total_prunable_values, 1.0f);

  GenericHistogramBuilder<T> builder(bin_count(), _domain);
  for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
    const auto pruned_height = bin_prunable_height[bin_id] * pruning_ratio;
    const auto new_bin_height = bin_height(bin_id) - pruned_height;
    if (new_bin_height <= std::numeric_limits<float>::epsilon()) continue;

    builder.add_bin(bin_minimum(bin_id), bin_maximum(bin_id), new_bin_height, bin_distinct_count(bin_id));
  }

  return builder.build();
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> AbstractHistogram<T>::scaled(const Selectivity selectivity) const {
  GenericHistogramBuilder<T> builder(bin_count(), _domain);

  // Scale the number of values in the bin with the given selectivity.
  for (auto bin_id = BinID{0}; bin_id < bin_count(); ++bin_id) {
    builder.add_bin(bin_minimum(bin_id), bin_maximum(bin_id), bin_height(bin_id) * selectivity,
                    _scale_distinct_count(bin_height(bin_id), bin_distinct_count(bin_id), selectivity));
  }

  return builder.build();
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> AbstractHistogram<T>::split_at_bin_bounds(
    const std::vector<std::pair<T, T>>& additional_bin_edges) const {
  // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
  if constexpr (std::is_same_v<T, pmr_string>) {
    Fail("Cannot split_at_bin_bounds() on string histogram");
  }

  const auto input_bin_count = bin_count();

  /**
   * Collect "candidate" splits from the histogram itself and the `additional_bin_edges`.
   *
   * A split is a pair, where pair.first is the upper bound of a (potential) bin in the
   * result histogram and pair.second is the lower bound of the (potential) subsequent bin.
   *
   * E.g. if the histogram has the bins {[0, 10], [15, 20]} and additional_bin_edges is {[-4, 5], [16, 18]})
   * then candidate_split_set becomes {[-5, -4], [-1, 0], [5, 6], [10, 11], [14, 15], [15, 16], [20, 21], [18, 19]}
   */
  auto candidate_split_set = std::unordered_set<std::pair<T, T>, boost::hash<std::pair<T, T>>>{};

  for (auto bin_id = BinID{0}; bin_id < input_bin_count; ++bin_id) {
    const auto bin_min = bin_minimum(bin_id);
    const auto bin_max = bin_maximum(bin_id);
    // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
    if constexpr (std::is_arithmetic_v<T>) {
      candidate_split_set.insert(std::make_pair(_domain.previous_value_clamped(bin_min), bin_min));
      candidate_split_set.insert(std::make_pair(bin_max, _domain.next_value_clamped(bin_max)));
    }
  }

  for (const auto& edge_pair : additional_bin_edges) {
    // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
    if constexpr (std::is_arithmetic_v<T>) {
      candidate_split_set.insert(std::make_pair(_domain.previous_value_clamped(edge_pair.first), edge_pair.first));
      candidate_split_set.insert(std::make_pair(edge_pair.second, _domain.next_value_clamped(edge_pair.second)));
    }
  }

  if (candidate_split_set.empty()) {
    GenericHistogramBuilder<T> builder{0, _domain};
    return builder.build();
  }

  /**
   * Sequentialize `candidate_split_set` and sort it.
   *
   * E.g. {[-1, 0], [10, 11], [14, 15], [20, 21], [-5, -4], [5, 6], [15, 16], [18, 19]} becomes
   * [-5, -4, -1, 0, 5, 6, 10, 11, 14, 15, 15, 16, 18, 19, 20, 21]
   */
  auto candidate_edges = std::vector<T>(candidate_split_set.size() * 2);
  auto edge_idx = size_t{0};
  for (const auto& split_pair : candidate_split_set) {
    candidate_edges[edge_idx] = split_pair.first;
    candidate_edges[edge_idx + 1] = split_pair.second;
    edge_idx += 2;
  }

  std::sort(candidate_edges.begin(), candidate_edges.end());

  /**
   * Remove the first and last value.
   * These are the values introduced in the first step for the first and last split.
   * The lower edge of bin 0 basically added the upper edge of bin -1,
   * and the upper edge of the last bin basically added the lower edge of the bin after the last bin.
   * Both values are obviously not needed.
   *
   * E.g., [-5, -4, -1, 0, 5, 6, 10, 11, 14, 15, 15, 16, 18, 19, 20, 21] becomes
   * [-4, -1, 0, 5, 6, 10, 11, 14, 15, 15, 16, 18, 19, 20]
   */
  candidate_edges.erase(candidate_edges.begin());
  candidate_edges.pop_back();

  /**
   * Create new bins.
   * Bin edges are defined by two consecutive values in candidate_edges in pairs of two.
   *
   * E.g. [-4, -1, 0, 5, 6, 10, 11, 14, 15, 15, 16, 18, 19, 20] becomes
   * {[-4, -1], [0, 5], [6, 10], [11, 14], [15, 15], [16, 18], [19, 20]} but since some of these bins contain not data
   * in the original histogram, the resulting histogram is {[0, 5], [6, 10], [15, 15], [16, 18], [19, 20]}
   */
  const auto result_bin_count = candidate_edges.size() / 2;
  GenericHistogramBuilder<T> builder{result_bin_count, _domain};

  for (auto bin_id = BinID{0}; bin_id < result_bin_count; ++bin_id) {
    const auto bin_min = candidate_edges[bin_id * 2];
    const auto bin_max = candidate_edges[bin_id * 2 + 1];

    const auto estimate =
        estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin_min, bin_max);
    // Skip empty bins
    if (estimate.first == Cardinality{0}) {
      continue;
    }

    builder.add_bin(bin_min, bin_max, static_cast<HistogramCountType>(estimate.first),
                    static_cast<HistogramCountType>(estimate.second));
  }

  return builder.build();
}

template <typename T>
std::vector<std::pair<T, T>> AbstractHistogram<T>::bin_bounds() const {
  std::vector<std::pair<T, T>> bin_edges(bin_count());

  for (auto bin_id = BinID{0}; bin_id < bin_edges.size(); ++bin_id) {
    bin_edges[bin_id] = std::make_pair(bin_minimum(bin_id), bin_maximum(bin_id));
  }

  return bin_edges;
}

template <typename T>
void AbstractHistogram<T>::_assert_bin_validity() {
  for (BinID bin_id{0}; bin_id < bin_count(); ++bin_id) {
    Assert(bin_minimum(bin_id) <= bin_maximum(bin_id), "Bin minimum must be <= bin maximum.");

    if (bin_id < bin_count() - 1) {
      Assert(bin_maximum(bin_id) < bin_minimum(bin_id + 1), "Bins must be sorted and cannot overlap.");
    }

    // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
    if constexpr (std::is_same_v<T, pmr_string>) {
      Assert(_domain.contains(bin_minimum(bin_id)), "Invalid string bin minimum");
      Assert(_domain.contains(bin_maximum(bin_id)), "Invalid string bin maximum");
    }
  }
}

template <typename T>
Cardinality AbstractHistogram<T>::_scale_distinct_count(Cardinality value_count, Cardinality distinct_count,
                                                        Selectivity selectivity) {
  return std::min(distinct_count, Cardinality{value_count * selectivity});
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
