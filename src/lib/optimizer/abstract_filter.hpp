#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractFilter : public std::enable_shared_from_this<AbstractFilter> {
 public:
  virtual ~AbstractFilter() = default;

  /**
   * checks whether the filter is able to determine that the given value 
   * and predicate condition will not yield any positive results with the values
   * represented by the filter data.
   * 
   * In other words: A scan operation with value and predicate_type on the column/chunk
   * that this filter was created on would yield zero result rows.
  */
  virtual bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const = 0;
};

}  // namespace opossum
