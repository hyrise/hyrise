#include "abstract_range_index.hpp"

#include <memory>
#include <vector>

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

namespace opossum {

AbstractRangeIndex::AbstractRangeIndex(const SegmentIndexType type) : AbstractIndex<ChunkOffset>(type) {}

AbstractRangeIndex::Iterator AbstractRangeIndex::lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      (_get_indexed_segments().size() >= values.size()),
      "AbstractRangeIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _lower_bound(values);
}

AbstractRangeIndex::Iterator AbstractRangeIndex::upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      (_get_indexed_segments().size() >= values.size()),
      "AbstractRangeIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _upper_bound(values);
}

}  // namespace opossum
