#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include "abstract_filter.hpp"

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
class RangeFilter : public AbstractFilter {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");

  explicit RangeFilter(std::vector<std::pair<T, T>> ranges) : _ranges(std::move(ranges)) {}
  ~RangeFilter() override = default;

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary,
                                                      uint32_t max_ranges_count = MAX_RANGES_COUNT);

  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

 protected:
  std::vector<std::pair<T, T>> _ranges;
};

}  // namespace opossum
