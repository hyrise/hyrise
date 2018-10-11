#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "segment_statistics.hpp"

namespace opossum {

/**
 * Container class that holds objects with statistical information about a chunk.
 */
class ChunkStatistics final : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  explicit ChunkStatistics(std::vector<std::shared_ptr<SegmentStatistics>> statistics) : _statistics(statistics) {}

  const std::vector<std::shared_ptr<SegmentStatistics>>& statistics() const { return _statistics; }

  /**
   * calls can_prune on the SegmentStatistics corresponding to column_id
   */
  bool can_prune(const ColumnID column_id, const PredicateCondition predicate_condition,
                 const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

 protected:
  std::vector<std::shared_ptr<SegmentStatistics>> _statistics;
};
}  // namespace opossum
