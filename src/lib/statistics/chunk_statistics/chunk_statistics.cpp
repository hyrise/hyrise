#include "chunk_statistics.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const CxlumnID cxlumn_id, const AllTypeVariant& value,
                                const PredicateCondition predicate_condition) const {
  DebugAssert(cxlumn_id < _statistics.size(), "The passed cxlumn ID should fit in the bounds of the statistics.");
  DebugAssert(_statistics[cxlumn_id], "The statistics should not contain any empty shared_ptrs.");
  return _statistics[cxlumn_id]->can_prune(value, predicate_condition);
}

}  // namespace opossum
