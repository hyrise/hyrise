#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "abstract_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

//! default number of maximum range count
static constexpr uint32_t MAX_RANGES_COUNT = 10;

/**
 * Filter that stores a certain number of value ranges. Each range represents a spread
 * of values that is contained within the bounds.
 * Example: [1, 2, 4, 7] might be represented as [1, 7]
 * These ranges can be used to check whether a certain value exists in the segment.
 * Once the between operator uses two parameters, the ranges can be used for that as well.
*/
template <typename T>
class RangeFilter : public AbstractStatisticsObject {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");

  explicit RangeFilter(std::vector<std::pair<T, T>> init_ranges);
  ~RangeFilter() override = default;

  // cppcoreguidelines-special-member-functions
  RangeFilter(const RangeFilter& other) = default;
  RangeFilter(RangeFilter&& other) = default;
  RangeFilter& operator=(const RangeFilter& other) = default;
  RangeFilter& operator=(RangeFilter&& other) = default;

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary,
                                                      uint32_t max_ranges_count = MAX_RANGES_COUNT);

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
