#pragma once

#include "abstract_filter.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a segment's minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractFilter {
 public:
  explicit MinMaxFilter(T min, T max);

  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

 protected:
  const T _min;
  const T _max;
};

}  // namespace opossum
