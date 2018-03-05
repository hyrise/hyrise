#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include "optimizer/chunk_statistics/abstract_filter.hpp"

namespace opossum {

// select how many ranges we want in the filter
// make this customizable?
static constexpr uint32_t MAX_RANGES_COUNT = 10;

/**
 * Filter that stores MAX_RANGES_COUNT value ranges. Each range represents a gap in the data
 * i.e. an interval where the column has no values.
 * These ranges can be used to check whether a certain value exists in the column.
 * Once the between operator uses two parameters, the ranges can be used for that aswell.
*/
template <typename T>
class RangeFilter : public AbstractFilter {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");

  explicit RangeFilter(std::vector<std::pair<T, T>> ranges) : _ranges(std::move(ranges)) {}
  ~RangeFilter() override = default;

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary);

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    const auto t_value = boost::get<T>(value);
    switch (predicate_type) {
      case PredicateCondition::Equals: {
        bool prunable = false;
        for (const auto& min_max_pair : _ranges) {
          const auto & [ min, max ] = min_max_pair;
          prunable |= min < t_value && t_value < max;
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
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary) {
  // calculate distances by taking the difference between two neighbouring elements
  std::vector<std::pair<T, size_t>> distances;
  distances.reserve(dictionary.size());
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.first > pair2.first; });

  if ((MAX_RANGES_COUNT - 1) < distances.size()) {
    distances.erase(distances.cbegin() + (MAX_RANGES_COUNT - 1), distances.cend());
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.second < pair2.second; });

  // derive intervals where items don't exist from distances
  //
  //         index  index + 1
  //         v      v
  // 1 2 3 4 5      10 11     15 16
  //         ^
  //       distance 5, index 4

  std::vector<std::pair<T, T>> ranges;
  for (const auto& distance_index_pair : distances) {
    // `index + 1` is ok because we check `dict_it + 1 != dictionary.cend()` above
    auto index = std::get<1>(distance_index_pair);
    ranges.push_back(std::make_pair(dictionary[index], dictionary[index + 1]));
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}
}  // namespace opossum
