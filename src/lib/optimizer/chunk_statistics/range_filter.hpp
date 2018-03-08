#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include "optimizer/chunk_statistics/abstract_filter.hpp"

namespace opossum {

//! default number of maximum range count
static constexpr uint32_t MAX_RANGES_COUNT = 10;

/**
 * Filter that stores a certain number of value ranges. Each range represents a spread
 * of values that is contained within the bounds.
 * Example: [1, 2, 4, 7] might be represented as [1, 7]
 * These ranges can be used to check whether a certain value exists in the column.
 * Once the between operator uses two parameters, the ranges can be used for that as well.
*/
template <typename T>
class RangeFilter : public AbstractFilter {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");

  explicit RangeFilter(std::vector<std::pair<T, T>> ranges) : _ranges(std::move(ranges)) {}
  ~RangeFilter() override = default;

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary,
                                                      uint32_t max_ranges_count = MAX_RANGES_COUNT);

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    const auto t_value = boost::get<T>(value);
    // Operators work as follows: value_from_table <operator> t_value
    // e.g. OpGreaterThan: value_from_table > t_value
    // thus we can exclude chunk if t_value >= _max since then no value from the table can be greater than t_value
    switch (predicate_type) {
      case PredicateCondition::GreaterThan: {
        auto& max = _ranges.back().second;
        return t_value >= max;
      }
      case PredicateCondition::GreaterThanEquals: {
        auto& max = _ranges.back().second;
        return t_value > max;
      }
      case PredicateCondition::LessThan: {
        auto& min = _ranges.front().first;
        return t_value <= min;
      }
      case PredicateCondition::LessThanEquals: {
        auto& min = _ranges.front().first;
        return t_value < min;
      }
      case PredicateCondition::Equals: {
        bool prunable = true;
        for (const auto& bounds : _ranges) {
          const auto & [ min, max ] = bounds;
          // prunable becomes false if t_value is within any of the bounds
          prunable &= !(t_value >= min && t_value <= max);
        }
        return prunable;
      }
      default:
        return false;
    }
  }

 protected:
  std::vector<std::pair<T, T>> _ranges;
};

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
    const auto index = std::get<1>(distance_index_pair);
    ranges.emplace_back(dictionary[next_startpoint], dictionary[index]);
    next_startpoint = index + 1;
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}
}  // namespace opossum
