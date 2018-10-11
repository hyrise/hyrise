#include "range_filter.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "statistics/abstract_statistics_object.hpp"
#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
RangeFilter<T>::RangeFilter(std::vector<std::pair<T, T>> ranges) : _ranges(std::move(ranges)) {}

template <typename T>
bool RangeFilter<T>::does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                      const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast<T>(variant_value);
  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= _max since then no value from the table can be greater than value
  switch (predicate_type) {
    case PredicateCondition::GreaterThan: {
      auto& max = _ranges.back().second;
      return value >= max;
    }
    case PredicateCondition::GreaterThanEquals: {
      auto& max = _ranges.back().second;
      return value > max;
    }
    case PredicateCondition::LessThan: {
      auto& min = _ranges.front().first;
      return value <= min;
    }
    case PredicateCondition::LessThanEquals: {
      auto& min = _ranges.front().first;
      return value < min;
    }
    case PredicateCondition::Equals: {
      for (const auto& bounds : _ranges) {
        const auto& [min, max] = bounds;

        if (value >= min && value <= max) {
          return false;
        }
      }
      return true;
    }
    case PredicateCondition::NotEquals: {
      return _ranges.size() == 1 && _ranges.front().first == value && _ranges.front().second == value;
    }
    case PredicateCondition::Between: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = type_cast<T>(*variant_value2);

      if (value > value2) {
        return true;
      }

      if (does_not_contain(PredicateCondition::GreaterThanEquals, variant_value) ||
          does_not_contain(PredicateCondition::LessThanEquals, *variant_value2)) {
        return true;
      }

      const auto lower_bound_value_it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value,
                                                         [](const auto& a, const auto& b) { return a.second < b; });

      // If value belongs to a non-gap, we do not know whether the value is in the data or not.
      if (value >= (*lower_bound_value_it).first) {
        return false;
      }

      const auto lower_bound_value2_it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value2,
                                                          [](const auto& a, const auto& b) { return a.second < b; });

      // If value2 belongs to a non-gap, we do not know whether the value is in the data or not.
      if (value2 >= (*lower_bound_value2_it).first) {
        return false;
      }

      // If both values fall into the same gap, the data does not contain any of the values between value and value2.
      return lower_bound_value_it == lower_bound_value2_it;
    }
    default:
      return false;
  }
}

template <typename T>
std::pair<float, bool> RangeFilter<T>::estimate_cardinality(const PredicateCondition predicate_type,
                                                            const AllTypeVariant& variant_value,
                                                            const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {0.f, true};
  } else {
    return {1.f, false};
  }
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::slice_with_predicate(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_type, variant_value, variant_value2)) {
    Fail("NYI - return empty statistics object");
  }

  std::vector<std::pair<T, T>> ranges;
  const auto value = type_cast<T>(variant_value);

  // If value is on range edge, we do not take the opportunity to slightly improve the new object.
  // The impact should be small.
  switch (predicate_type) {
    case PredicateCondition::Equals:
      return std::make_shared<MinMaxFilter<T>>(value, value);
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals: {
      auto end_it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value,
                                     [](const auto& a, const auto& b) { return a.second < b; });

      // Copy all the ranges before the value.
      auto it = _ranges.cbegin();
      for (; it != end_it; it++) {
        ranges.emplace_back(*it);
      }

      DebugAssert(it != _ranges.cend(), "does_not_contain() should have caught that.");

      // If value is not in a gap, limit the last range's upper bound to value.
      if (value >= it->first) {
          ranges.emplace_back(std::pair<T, T>{it->first, value});
      }
    } break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value,
                                 [](const auto& a, const auto& b) { return a.second < b; });

      DebugAssert(it != _ranges.cend(), "does_not_contain() should have caught that.");

      // If value is in a gap, use the next range, otherwise limit the next range's upper bound to value.
      if (value <= it->first) {
        ranges.emplace_back(*it);
      } else {
        ranges.emplace_back(std::pair<T, T>{value, it->second});
      }
      it++;

      // Copy all following ranges.
      for (; it != _ranges.cend(); it++) {
        ranges.emplace_back(*it);
      }
    } break;
    case PredicateCondition::Between: {
      DebugAssert(variant_value2, "BETWEEN needs a second value.");
      const auto value2 = type_cast<T>(*variant_value2);
      return slice_with_predicate(PredicateCondition::GreaterThanEquals, value)
          ->slice_with_predicate(PredicateCondition::LessThanEquals, value2);
    }
    default:
      ranges = _ranges;
  }

  return std::make_shared<RangeFilter<T>>(ranges);
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::scale_with_selectivity(const float selectivity) const {
  return std::make_shared<RangeFilter<T>>(_ranges);
}

template <typename T>
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary,
                                                             uint32_t max_ranges_count) {
  static_assert(std::is_arithmetic_v<T>, "Range filters are only allowed on arithmetic types.");
  DebugAssert(!dictionary.empty(), "The dictionary should not be empty.");

  if (dictionary.size() == 1) {
    std::vector<std::pair<T, T>> ranges;
    ranges.emplace_back(dictionary.front(), dictionary.front());
    return std::make_unique<RangeFilter<T>>(std::move(ranges));
  }

  // calculate distances by taking the difference between two neighbouring elements
  // vector stores <distance to next element, dictionary index>
  std::vector<std::pair<T, size_t>> distances;
  distances.reserve(dictionary.size());
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.first > pair2.first; });

  if ((max_ranges_count - 1) < distances.size()) {
    distances.erase(distances.cbegin() + (max_ranges_count - 1), distances.cend());
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.second < pair2.second; });
  // we want a range until the last element in the dictionary
  distances.emplace_back(T{}, dictionary.size() - 1);

  // derive intervals from distances where items exist
  //
  // start   end  next_startpoint
  // v       v    v
  // 1 2 3 4 5    10 11     15 16
  //         ^
  //         distance 5, index 4
  //
  // next_startpoint is the start of the next range

  std::vector<std::pair<T, T>> ranges;
  size_t next_startpoint = 0u;
  for (const auto& distance_index_pair : distances) {
    const auto index = distance_index_pair.second;
    ranges.emplace_back(dictionary[next_startpoint], dictionary[index]);
    next_startpoint = index + 1;
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}

template class RangeFilter<int32_t>;
template class RangeFilter<int64_t>;
template class RangeFilter<float>;
template class RangeFilter<double>;

}  // namespace opossum
