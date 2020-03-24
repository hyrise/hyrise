#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "abstract_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

static constexpr uint32_t DEFAULT_MAX_RANGES_COUNT = 10;

/**
 * Filter that stores a certain number of value ranges. Each range represents a spread of values that is contained
 * within the bounds. Ranges are chosen so that the cumulative size of all gaps is maximal.
 * Example: [2, 3, 5, 8, 9, 11] with max_ranges_count=3 will be represented as [2, 3] [5, 5] [8, 11].
 * RangeFilters are typically created for a single chunk and can be used to check whether a certain value exists in the
 * segment.
*/
template <typename T>
class RangeFilter : public AbstractStatisticsObject {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");  // #1536

  explicit RangeFilter(std::vector<std::pair<T, T>> init_ranges);

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary,
                                                      uint32_t max_ranges_count = DEFAULT_MAX_RANGES_COUNT);

  Cardinality estimate_cardinality(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                   const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  bool does_not_contain(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  const std::vector<std::pair<T, T>> ranges;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const RangeFilter<T>& filter) {
  stream << "{";

  for (const auto& range : filter.ranges) {
    stream << range.first << "->" << range.second << " ";
  }

  stream << "}";
  return stream;
}

}  // namespace opossum
