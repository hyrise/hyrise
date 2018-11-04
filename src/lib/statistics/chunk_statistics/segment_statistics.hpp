#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "abstract_filter.hpp"

namespace opossum {

class BaseSegment;

/**
 * Container class that holds a set of filters with statistical information about a
 * certain segment. Is part of ChunkStatistics.
 */
class SegmentStatistics final {
 public:
  static std::shared_ptr<SegmentStatistics> build_statistics(DataType data_type,
                                                             const std::shared_ptr<const BaseSegment>& segment);

  void add_filter(std::shared_ptr<AbstractFilter> filter);

  /**
   * calls can_prune on each filter in this object
  */
  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

 protected:
  std::vector<std::shared_ptr<AbstractFilter>> _filters;
};
}  // namespace opossum
