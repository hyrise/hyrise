#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "abstract_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

static constexpr uint32_t DEFAULT_MAX_RANGES_COUNT = 10;

/**
 * Filters are data structures that are primarily used for probabilistic membership queries. In Hyrise, they are
 * typically created on a single segment. They can then be used to check whether a certain value exists in the segment.
 * While histograms also support does_not_contain, their main purpose is not to answer membership queries, but to
 * provide statistics estimations.
 *
 * The RangeFilter stores a certain number of value ranges. Each range represents a spread of values that is contained
 * within the bounds. Ranges are chosen so that the cumulative size of all gaps is maximal.
 * Example: [2, 3, 5, 8, 9, 11] with max_ranges_count=3 will be represented as [2, 3] [5, 5] [8, 11].
 * RangeFilters are typically created for a single segment and can be used to check whether a certain value exists in
 * the segment.
 *
 * RangeFilters could be expressed as a type of histogram. This is not done for two reasons: First, we like to keep
 * filters and histograms separate, as they serve different purposes. Having histograms both on a per-segment basis
 * (for membership queries) and on a per-column basis (for cardinality estimations) could lead to confusion. Second,
 * building such a histogram would be more expensive than build a RangeFilter, as we would have to look at each value
 * instead of only looking at the distinct values (which is significantly cheaper for dictionary encoding).
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
