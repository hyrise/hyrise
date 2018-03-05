#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "optimizer/chunk_column_statistics.hpp"

namespace opossum {

class ChunkStatistics : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  explicit ChunkStatistics(std::vector<std::shared_ptr<ChunkColumnStatistics>> statistics) : _statistics(statistics) {}

  const std::vector<std::shared_ptr<ChunkColumnStatistics>>& statistics() const { return _statistics; }

  bool can_prune(const ColumnID column_id, const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<std::shared_ptr<ChunkColumnStatistics>> _statistics;
};
}  // namespace opossum
