#include "chunk_statistics.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const ColumnID column_id, const PredicateCondition predicate_condition,
                                const AllTypeVariant& variant_value,
                                const std::optional<AllTypeVariant>& variant_value2) const {
  DebugAssert(column_id < _statistics.size(), "The passed column ID should fit in the bounds of the statistics.");
  DebugAssert(_statistics[column_id], "The statistics should not contain any empty shared_ptrs.");
  return _statistics[column_id]->can_prune(predicate_condition, variant_value, variant_value2);
}

}  // namespace opossum
