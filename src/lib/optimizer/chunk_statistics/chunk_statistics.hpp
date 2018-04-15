#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "optimizer/chunk_statistics/chunk_column_statistics.hpp"

namespace opossum {

/**
 * Container class that holds objects with statistical information about a chunk.
 */
class ChunkStatistics final : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  explicit ChunkStatistics(std::vector<std::shared_ptr<ChunkColumnStatistics>> statistics) : _statistics(statistics) {}

  const std::vector<std::shared_ptr<ChunkColumnStatistics>>& statistics() const { return _statistics; }

  /**
   * calls can_prune on the ChunkColumnStatistics corresponding to column_id
   */
  bool can_prune(const ColumnID column_id, const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<std::shared_ptr<ChunkColumnStatistics>> _statistics;
};
}  // namespace opossum
