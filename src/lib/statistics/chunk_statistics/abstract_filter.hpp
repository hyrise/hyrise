#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Base class for all filters. A filter is part of SegmentStatistics and holds
 * some kind of statistical information about the contents of a segment.
 * This information can be used to optimize queries.
*/
class AbstractFilter : public std::enable_shared_from_this<AbstractFilter> {
 public:
  virtual ~AbstractFilter() = default;

  /**
   * checks whether the filter is able to determine that the given value
   * and predicate condition will not yield any positive results with the values
   * represented by the filter data.
   *
   * In other words: returns true if a scan operation with value and predicate_type
   * on the segment that this filter was created on would yield zero result rows.
  */
  virtual bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;
};

}  // namespace opossum
